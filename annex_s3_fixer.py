#!/usr/bin/env python3
"""CLI tool to find and fix git-annex keys missing S3 URL metadata (.log.rmet).

OpenNeuro datasets stored as git-annex repos sometimes have keys registered as
present on S3 but missing the .log.rmet file that stores the versionId URL
metadata. Without .log.rmet, git-annex can't construct download URLs for
versioned S3 buckets, making files undownloadable.
"""

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import boto3
import click
from botocore import UNSIGNED
from botocore.config import Config

lgr = logging.getLogger(__name__)


# -- Parsing helpers --

def parse_remote_log(content: str) -> dict[str, dict[str, str]]:
    """Parse git-annex remote.log into {uuid: {key: value, ...}} dict.

    Each line looks like:
      <uuid> key1=val1 key2=val2 ... timestamp=<ts>
    """
    remotes = {}
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        uuid = parts[0]
        # Parse key=value pairs (values may not contain spaces in remote.log)
        attrs = {}
        for m in re.finditer(r'(\w+)=(\S+)', parts[1]):
            attrs[m.group(1)] = m.group(2)
        remotes[uuid] = attrs
    return remotes


def find_s3_remote(remotes: dict, name: str = "s3-PUBLIC") -> tuple[str, dict] | None:
    """Find the S3 remote with the given name. Returns (uuid, attrs) or None."""
    for uuid, attrs in remotes.items():
        if attrs.get("name") == name:
            return uuid, attrs
    return None


def parse_log_file(content: str) -> list[dict]:
    """Parse a git-annex .log file.

    Lines like: <timestamp>s <1|0> <uuid>
    Returns list of {timestamp: str, present: bool, uuid: str}
    """
    entries = []
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'(\S+s)\s+([01])\s+(\S+)', line)
        if m:
            entries.append({
                "timestamp": m.group(1),
                "present": m.group(2) == "1",
                "uuid": m.group(3),
            })
    return entries


def parse_rmet_file(content: str) -> list[dict]:
    r"""Parse a git-annex .log.rmet file.

    Lines like: <timestamp>s <uuid>:V +<versionId>#<s3path>
    May also have other formats; we focus on the :V variant.
    """
    entries = []
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'(\S+s)\s+(\S+?):V\s+\+(\S+?)#(\S+)', line)
        if m:
            entries.append({
                "timestamp": m.group(1),
                "uuid": m.group(2),
                "version_id": m.group(3),
                "s3_path": m.group(4),
            })
    return entries


def extract_size_from_key(key: str) -> int | None:
    """Extract file size from a git-annex key name.

    Keys look like: MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz
    The -s<SIZE>- field encodes the size.
    """
    m = re.search(r'-s(\d+)-', key)
    if m:
        return int(m.group(1))
    return None


def extract_checksum_from_key(key: str) -> tuple[str, str] | None:
    """Extract hash type and checksum from a git-annex key name.

    Key formats:
      MD5E-s186016172--b905bd144175d465b60ba9a14d229d9e.nii.gz
        -> ("md5", "b905bd144175d465b60ba9a14d229d9e")
      SHA1--cd3b8d583a83d14c97228f0b46e65b7c766e3fad
        -> ("sha1", "cd3b8d583a83d14c97228f0b46e65b7c766e3fad")
      SHA256E-s12345--abc123def456.txt
        -> ("sha256", "abc123def456")

    The hash type is the prefix (MD5, MD5E, SHA1, SHA256, SHA256E, etc.).
    'E' suffix means extension is preserved in the key name but the checksum
    is computed on the content without the extension.
    The checksum follows '--'.
    For 'E' variants, the checksum ends where the first '.' of the extension starts.
    """
    # Split on '--' to get the checksum part
    parts = key.split("--", 1)
    if len(parts) != 2:
        return None

    prefix = parts[0]  # e.g. "MD5E-s186016172" or "SHA1"
    checksum_part = parts[1]  # e.g. "b905bd14...nii.gz" or "cd3b8d..."

    # Determine hash type from prefix
    m = re.match(r'(MD5|SHA1|SHA256|SHA512|SHA224|SHA384|SHA3_256|SHA3_512|SKEIN256|SKEIN512)(E)?', prefix)
    if not m:
        return None

    hash_type = m.group(1).lower().replace("_", "-")
    has_extension = m.group(2) == "E"

    if has_extension:
        # Checksum ends at first '.' (extension boundary)
        dot_pos = checksum_part.find(".")
        if dot_pos > 0:
            checksum = checksum_part[:dot_pos]
        else:
            checksum = checksum_part
    else:
        checksum = checksum_part

    return (hash_type, checksum)


def extract_key_from_annex_path(annex_path: str) -> str:
    """Extract the key name from a git-annex branch path.

    Path like: 23a/e1e/MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz.log
    Returns: MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz
    """
    basename = os.path.basename(annex_path)
    # Strip .log, .log.rmet, .log.web suffixes
    for suffix in (".log.rmet", ".log.web", ".log.cnk", ".log"):
        if basename.endswith(suffix):
            return basename[: -len(suffix)]
    return basename


def key_stem(annex_path: str) -> str:
    """Get the stem (path without .log/.log.rmet/.log.web suffix).

    Used for grouping related files.
    """
    for suffix in (".log.rmet", ".log.web", ".log.cnk", ".log"):
        if annex_path.endswith(suffix):
            return annex_path[: -len(suffix)]
    return annex_path


def increment_timestamp(ts: str) -> str:
    """Increment a git-annex timestamp by 1 second.

    Input like: 1534885659.567118843s
    Output: 1534885660.567118843s
    """
    if ts.endswith("s"):
        ts_num = ts[:-1]
    else:
        ts_num = ts
    parts = ts_num.split(".")
    seconds = int(parts[0]) + 1
    if len(parts) > 1:
        return f"{seconds}.{parts[1]}s"
    return f"{seconds}s"


def format_rmet_line(timestamp: str, uuid: str, version_id: str, s3_path: str) -> str:
    """Format a .log.rmet line."""
    return f"{timestamp} {uuid}:V +{version_id}#{s3_path}\n"


# -- Git helpers --

def git_run(args: list[str], cwd: str | None = None) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git"] + args,
        capture_output=True, text=True, cwd=cwd,
        check=True,
    )
    return result.stdout


def git_show(ref: str, cwd: str | None = None) -> str:
    """Run git show <ref> and return content."""
    return git_run(["show", ref], cwd=cwd)


def git_ls_tree_annex(cwd: str | None = None) -> list[dict]:
    """Parse output of git ls-tree -r git-annex.

    Returns list of {mode, type, hash, path}.
    """
    output = git_run(["ls-tree", "-r", "git-annex"], cwd=cwd)
    entries = []
    for line in output.strip().splitlines():
        if not line:
            continue
        # Format: <mode> <type> <hash>\t<path>
        meta, path = line.split("\t", 1)
        parts = meta.split()
        entries.append({
            "mode": parts[0],
            "type": parts[1],
            "hash": parts[2],
            "path": path,
        })
    return entries


def group_annex_keys(entries: list[dict]) -> dict[str, dict[str, str]]:
    """Group git-annex branch entries by key stem.

    Returns {stem: {suffix: path, ...}} where suffix is 'log', 'rmet', 'web', etc.
    Top-level files (remote.log, uuid.log, etc.) are excluded.
    """
    groups: dict[str, dict[str, str]] = defaultdict(dict)
    for entry in entries:
        path = entry["path"]
        # Skip top-level files (no directory component with key hash structure)
        if "/" not in path:
            continue
        stem = key_stem(path)
        if path.endswith(".log.rmet"):
            groups[stem]["rmet"] = path
        elif path.endswith(".log.web"):
            groups[stem]["web"] = path
        elif path.endswith(".log.cnk"):
            groups[stem]["cnk"] = path
        elif path.endswith(".log"):
            groups[stem]["log"] = path
        else:
            groups[stem]["other"] = path
    return dict(groups)


# -- S3 helpers --

def get_s3_client():
    """Create an anonymous S3 client."""
    return boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED),
        region_name="us-east-1",
    )


def list_s3_versions(
    s3_client, bucket: str, key: str
) -> list[dict]:
    """List all versions of an S3 object.

    Returns list of {VersionId, Size, LastModified, IsLatest, ...}.
    """
    versions = []
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for v in page.get("Versions", []):
            if v["Key"] == key:  # exact match, not just prefix
                versions.append(v)
    return versions


def match_version_by_size(
    versions: list[dict], target_size: int
) -> list[dict]:
    """Filter S3 versions matching the target size."""
    return [v for v in versions if v["Size"] == target_size]


def match_version_by_checksum(
    s3_client,
    bucket: str,
    s3_key: str,
    candidates: list[dict],
    hash_type: str,
    expected_checksum: str,
) -> dict | None:
    """Disambiguate multiple S3 version candidates by downloading and checksumming.

    Only supports md5 and sha1/sha256/sha512 hash types.
    Downloads each candidate version and computes the checksum to find a match.
    Returns the matching version dict, or None.
    """
    hash_name_map = {
        "md5": "md5",
        "sha1": "sha1",
        "sha256": "sha256",
        "sha512": "sha512",
        "sha224": "sha224",
        "sha384": "sha384",
    }
    hash_name = hash_name_map.get(hash_type)
    if hash_name is None:
        lgr.warning("Unsupported hash type %s for checksum verification", hash_type)
        return None

    for candidate in candidates:
        version_id = candidate["VersionId"]
        lgr.info(
            "Downloading version %s of %s for checksum verification...",
            version_id, s3_key,
        )
        try:
            response = s3_client.get_object(
                Bucket=bucket, Key=s3_key, VersionId=version_id,
            )
            h = hashlib.new(hash_name)
            for chunk in response["Body"].iter_chunks(1024 * 1024):
                h.update(chunk)
            computed = h.hexdigest()
            if computed == expected_checksum:
                lgr.info("Checksum match for version %s", version_id)
                return candidate
            else:
                lgr.debug(
                    "Version %s checksum %s != expected %s",
                    version_id, computed, expected_checksum,
                )
        except Exception as e:
            lgr.warning("Failed to download version %s: %s", version_id, e)
    return None


# -- CLI --

@click.group()
@click.option(
    "--repo", "-C", default=".", type=click.Path(exists=True),
    help="Path to git-annex repository (default: current directory)",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def cli(ctx, repo, verbose):
    """Tool to find and fix git-annex keys missing S3 URL metadata."""
    ctx.ensure_object(dict)
    ctx.obj["repo"] = os.path.abspath(repo)
    ctx.obj["verbose"] = verbose
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


@cli.command("find-keys-without-urls")
@click.option(
    "--require", type=click.Choice(["rmet", "web", "any"]), default="rmet",
    help="Which URL metadata to require (default: rmet)",
)
@click.option(
    "--remote", default="s3-PUBLIC",
    help="Only report keys present on this remote (default: s3-PUBLIC)",
)
@click.option(
    "--all-keys", is_flag=True,
    help="Report all keys missing URLs, not just those on the specified remote",
)
@click.pass_context
def find_keys_without_urls(ctx, require, remote, all_keys):
    """Find git-annex keys that have .log but lack URL metadata."""
    repo = ctx.obj["repo"]
    verbose = ctx.obj["verbose"]

    # Get remote UUID if filtering by remote
    remote_uuid = None
    if not all_keys:
        remote_log = git_show("git-annex:remote.log", cwd=repo)
        remotes = parse_remote_log(remote_log)
        result = find_s3_remote(remotes, remote)
        if result is None:
            click.echo(f"ERROR: Remote '{remote}' not found in remote.log", err=True)
            sys.exit(1)
        remote_uuid, remote_attrs = result
        if verbose:
            click.echo(f"Remote '{remote}': uuid={remote_uuid}")

    # Parse git-annex branch
    entries = git_ls_tree_annex(cwd=repo)
    groups = group_annex_keys(entries)

    missing_count = 0
    total_keys = 0
    for stem, suffixes in sorted(groups.items()):
        if "log" not in suffixes:
            continue
        total_keys += 1

        has_url = False
        if require == "rmet":
            has_url = "rmet" in suffixes
        elif require == "web":
            has_url = "web" in suffixes
        elif require == "any":
            has_url = "rmet" in suffixes or "web" in suffixes

        if has_url:
            continue

        # Check if key is on the specified remote
        if remote_uuid and not all_keys:
            log_content = git_show(f"git-annex:{suffixes['log']}", cwd=repo)
            log_entries = parse_log_file(log_content)
            on_remote = any(
                e["uuid"] == remote_uuid and e["present"]
                for e in log_entries
            )
            if not on_remote:
                continue

        key = extract_key_from_annex_path(suffixes["log"])
        missing_count += 1
        if verbose:
            click.echo(f"{key}\t(log: {suffixes['log']})")
        else:
            click.echo(key)

    click.echo(
        f"\n{missing_count} keys missing URL metadata out of {total_keys} total keys",
        err=True,
    )


@cli.command("find-files-without-urls")
@click.option(
    "--remote", default="s3-PUBLIC",
    help="Check for URLs from this remote (default: s3-PUBLIC)",
)
@click.pass_context
def find_files_without_urls(ctx, remote):
    """Find files in current tree where S3 remote has no URL."""
    repo = ctx.obj["repo"]
    verbose = ctx.obj["verbose"]

    result = subprocess.run(
        ["git", "annex", "whereis", "--json"],
        capture_output=True, text=True, cwd=repo,
    )
    if result.returncode != 0:
        click.echo(f"ERROR: git annex whereis failed: {result.stderr}", err=True)
        sys.exit(1)

    missing_count = 0
    total_on_remote = 0
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not data.get("success"):
            continue

        for w in data.get("whereis", []) + data.get("untrusted", []):
            desc = w.get("description", "")
            if remote in desc:
                total_on_remote += 1
                urls = w.get("urls", [])
                if not urls:
                    missing_count += 1
                    filepath = data.get("file", "?")
                    key = data.get("key", "?")
                    if verbose:
                        click.echo(f"{filepath}\t{key}")
                    else:
                        click.echo(filepath)
                break

    click.echo(
        f"\n{missing_count} files without URLs out of {total_on_remote} on '{remote}'",
        err=True,
    )


@cli.command("fix-missing-s3-urls")
@click.option(
    "--remote", default="s3-PUBLIC",
    help="S3 remote name (default: s3-PUBLIC)",
)
@click.option(
    "--dry-run/--apply", default=True,
    help="Dry run (default) or apply changes to git-annex branch",
)
@click.option(
    "--limit", type=int, default=0,
    help="Limit number of keys to fix (0=all)",
)
@click.pass_context
def fix_missing_s3_urls(ctx, remote, dry_run, limit):
    """Fix keys missing .log.rmet by querying S3 for version IDs."""
    repo = ctx.obj["repo"]
    verbose = ctx.obj["verbose"]

    # Parse remote.log
    remote_log = git_show("git-annex:remote.log", cwd=repo)
    remotes = parse_remote_log(remote_log)
    s3_info = find_s3_remote(remotes, remote)
    if s3_info is None:
        click.echo(f"ERROR: Remote '{remote}' not found", err=True)
        sys.exit(1)
    remote_uuid, remote_attrs = s3_info
    bucket = remote_attrs.get("bucket", "")
    fileprefix = remote_attrs.get("fileprefix", "")

    click.echo(f"Remote: {remote} (uuid={remote_uuid})", err=True)
    click.echo(f"Bucket: {bucket}, prefix: {fileprefix}", err=True)

    # Find keys missing .log.rmet that are on this remote
    entries = git_ls_tree_annex(cwd=repo)
    groups = group_annex_keys(entries)

    missing_keys = []  # list of (key, log_path, log_content)
    for stem, suffixes in sorted(groups.items()):
        if "log" not in suffixes or "rmet" in suffixes:
            continue
        log_content = git_show(f"git-annex:{suffixes['log']}", cwd=repo)
        log_entries = parse_log_file(log_content)
        on_remote = [
            e for e in log_entries
            if e["uuid"] == remote_uuid and e["present"]
        ]
        if not on_remote:
            continue
        key = extract_key_from_annex_path(suffixes["log"])
        missing_keys.append((key, suffixes["log"], log_entries))

    click.echo(f"Found {len(missing_keys)} keys needing fix", err=True)

    if not missing_keys:
        return

    # Build key -> filepath mapping from current tree
    key_to_filepath = _build_key_filepath_map(repo)

    # Query S3 and generate fixes
    s3_client = get_s3_client()
    fixes = []  # list of (annex_rmet_path, rmet_content)
    fixed = 0
    errors = 0
    skipped_no_path = 0
    skipped_no_size = 0

    for key, log_path, log_entries in missing_keys:
        if limit and fixed >= limit:
            break

        filepath = key_to_filepath.get(key)
        if filepath is None:
            lgr.warning("Key %s not in current tree, skipping (historical lookup not yet implemented)", key)
            skipped_no_path += 1
            continue

        size = extract_size_from_key(key)
        if size is None:
            lgr.warning("Cannot extract size from key %s, no size in key name", key)
            skipped_no_size += 1
            continue

        s3_key = fileprefix + filepath
        lgr.debug("Querying S3: bucket=%s key=%s", bucket, s3_key)

        try:
            versions = list_s3_versions(s3_client, bucket, s3_key)
        except Exception as e:
            lgr.warning("S3 query failed for %s: %s", s3_key, e)
            errors += 1
            continue

        matches = match_version_by_size(versions, size)

        if len(matches) == 0:
            lgr.warning(
                "No S3 version matches size %d for %s (%d versions found)",
                size, filepath, len(versions),
            )
            errors += 1
            continue
        elif len(matches) > 1:
            lgr.warning(
                "Multiple S3 versions match size %d for %s: %s — attempting checksum disambiguation",
                size, filepath,
                [v["VersionId"] for v in matches],
            )
            checksum_info = extract_checksum_from_key(key)
            if checksum_info is None:
                lgr.warning("Cannot extract checksum from key %s, cannot disambiguate", key)
                errors += 1
                continue
            hash_type, expected_checksum = checksum_info
            winner = match_version_by_checksum(
                s3_client, bucket, s3_key, matches, hash_type, expected_checksum,
            )
            if winner is None:
                lgr.warning("No version matched checksum for %s", filepath)
                errors += 1
                continue
            matches = [winner]

        version_id = matches[0]["VersionId"]

        # Find timestamp from .log for this remote, add 1 second
        remote_entry = next(
            (e for e in log_entries if e["uuid"] == remote_uuid and e["present"]),
            None,
        )
        if remote_entry is None:
            lgr.warning("No log entry for remote %s in %s", remote_uuid, log_path)
            errors += 1
            continue

        rmet_timestamp = increment_timestamp(remote_entry["timestamp"])
        rmet_content = format_rmet_line(
            rmet_timestamp, remote_uuid, version_id, s3_key,
        )

        # The .rmet path is the .log path + ".rmet" stripped back:
        # Actually: the .log path is like aaa/bbb/KEY.log
        # The .rmet path is aaa/bbb/KEY.log.rmet
        rmet_path = log_path + ".rmet"

        if dry_run:
            click.echo(f"WOULD FIX: {filepath}")
            click.echo(f"  key: {key}")
            click.echo(f"  S3 versionId: {version_id}")
            click.echo(f"  rmet path: {rmet_path}")
            click.echo(f"  rmet content: {rmet_content.strip()}")
        else:
            fixes.append((rmet_path, rmet_content))

        fixed += 1

    if not dry_run and fixes:
        _apply_fixes_to_annex_branch(repo, fixes)
        click.echo(f"Applied {len(fixes)} fixes to git-annex branch", err=True)

    click.echo(f"\nSummary:", err=True)
    click.echo(f"  {len(missing_keys)} keys on '{remote}' missing .log.rmet", err=True)
    click.echo(f"  {fixed} {'would be fixed' if dry_run else 'fixed'}", err=True)
    if skipped_no_path:
        click.echo(f"  {skipped_no_path} skipped (not in current tree)", err=True)
    if skipped_no_size:
        click.echo(f"  {skipped_no_size} skipped (no size in key name)", err=True)
    if errors:
        click.echo(f"  {errors} errors (S3 query failures / no matching version)", err=True)
    remaining = len(missing_keys) - fixed - skipped_no_path - skipped_no_size - errors
    if limit and remaining > 0:
        click.echo(f"  {remaining} not attempted (--limit {limit})", err=True)


def _build_key_filepath_map(repo: str) -> dict[str, str]:
    """Build a mapping from git-annex key -> file path in current tree.

    Uses git annex whereis --json for accuracy.
    """
    key_to_filepath = {}
    result = subprocess.run(
        ["git", "annex", "whereis", "--json"],
        capture_output=True, text=True, cwd=repo,
    )
    if result.returncode != 0:
        lgr.warning("git annex whereis failed, trying symlink approach")
        return _build_key_filepath_map_from_symlinks(repo)

    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = data.get("key")
        filepath = data.get("file")
        if key and filepath:
            key_to_filepath[key] = filepath
    return key_to_filepath


def _build_key_filepath_map_from_symlinks(repo: str) -> dict[str, str]:
    """Build key->filepath map by reading symlinks in the working tree."""
    key_to_filepath = {}
    result = subprocess.run(
        ["git", "annex", "find", "--include=*"],
        capture_output=True, text=True, cwd=repo,
    )
    for filepath in result.stdout.strip().splitlines():
        filepath = filepath.strip()
        if not filepath:
            continue
        full_path = os.path.join(repo, filepath)
        if os.path.islink(full_path):
            target = os.readlink(full_path)
            key = os.path.basename(target)
            key_to_filepath[key] = filepath
    return key_to_filepath


def _apply_fixes_to_annex_branch(repo: str, fixes: list[tuple[str, str]]) -> None:
    """Apply .log.rmet fixes to the git-annex branch.

    Uses git plumbing commands (hash-object, read-tree, update-index,
    write-tree, commit-tree) to commit directly to the git-annex branch
    without needing a worktree or checkout.
    """
    env = os.environ.copy()

    # Use a temporary index file so we don't disturb the working tree
    with tempfile.NamedTemporaryFile(
        prefix="annex-fix-index-", dir=repo, delete=False,
    ) as tmp:
        tmp_index = tmp.name

    try:
        env["GIT_INDEX_FILE"] = tmp_index

        # Read the current git-annex tree into the temp index
        subprocess.run(
            ["git", "read-tree", "git-annex"],
            check=True, capture_output=True, cwd=repo, env=env,
        )

        # For each fix, hash the content and update the index
        for rmet_path, rmet_content in fixes:
            # Hash the content into the object store
            result = subprocess.run(
                ["git", "hash-object", "-w", "--stdin"],
                input=rmet_content, capture_output=True, text=True,
                check=True, cwd=repo,
            )
            blob_hash = result.stdout.strip()

            # Update the index entry
            subprocess.run(
                ["git", "update-index", "--add",
                 "--cacheinfo", f"100644,{blob_hash},{rmet_path}"],
                check=True, capture_output=True, cwd=repo, env=env,
            )

        # Write the tree
        result = subprocess.run(
            ["git", "write-tree"],
            capture_output=True, text=True, check=True, cwd=repo, env=env,
        )
        tree_hash = result.stdout.strip()

        # Get the current git-annex commit hash as parent
        parent = git_run(["rev-parse", "git-annex"], cwd=repo).strip()

        # Create a commit
        result = subprocess.run(
            ["git", "commit-tree", tree_hash, "-p", parent,
             "-m", "fix: add missing .log.rmet for S3 versioned keys"],
            capture_output=True, text=True, check=True, cwd=repo,
        )
        commit_hash = result.stdout.strip()

        # Update the git-annex branch ref
        git_run(["update-ref", "refs/heads/git-annex", commit_hash], cwd=repo)
    finally:
        if os.path.exists(tmp_index):
            os.unlink(tmp_index)


if __name__ == "__main__":
    cli()
