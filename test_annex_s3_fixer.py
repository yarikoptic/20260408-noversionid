"""Tests for annex_s3_fixer.py"""

import json
import os
import subprocess
import textwrap

import pytest

from annex_s3_fixer import (
    extract_checksum_from_key,
    extract_key_from_annex_path,
    extract_size_from_key,
    find_s3_remote,
    format_rmet_line,
    group_annex_keys,
    increment_timestamp,
    key_stem,
    parse_export_log,
    parse_log_file,
    parse_remote_log,
    parse_rmet_file,
)


# -- Unit tests for parsing --


class TestParseRemoteLog:
    SAMPLE = textwrap.dedent("""\
        66a7004d-a15e-4764-90cd-54bbd179f74a autoenable=true bucket=openneuro-private datacenter=US encryption=none exporttree=yes fileprefix=ds001473/ host=s3.amazonaws.com name=s3-PRIVATE partsize=1GiB port=80 public=no publicurl=no storageclass=STANDARD type=S3 versioning=yes timestamp=1541104695.225259256s
        e28d70a7-9314-4542-a4ce-7d95b862070f autoenable=true bucket=openneuro.org datacenter=US encryption=none exporttree=yes fileprefix=ds000113/ host=s3.amazonaws.com name=s3-PUBLIC partsize=1GiB port=80 public=yes publicurl=https://s3.amazonaws.com/openneuro.org storageclass=STANDARD type=S3 versioning=yes timestamp=1597694369.712881988s
    """)

    def test_parse_two_remotes(self):
        remotes = parse_remote_log(self.SAMPLE)
        assert len(remotes) == 2
        assert "66a7004d-a15e-4764-90cd-54bbd179f74a" in remotes
        assert "e28d70a7-9314-4542-a4ce-7d95b862070f" in remotes

    def test_remote_attributes(self):
        remotes = parse_remote_log(self.SAMPLE)
        public = remotes["e28d70a7-9314-4542-a4ce-7d95b862070f"]
        assert public["name"] == "s3-PUBLIC"
        assert public["bucket"] == "openneuro.org"
        assert public["fileprefix"] == "ds000113/"
        assert public["versioning"] == "yes"
        assert public["publicurl"] == "https://s3.amazonaws.com/openneuro.org"

    def test_find_s3_remote(self):
        remotes = parse_remote_log(self.SAMPLE)
        result = find_s3_remote(remotes, "s3-PUBLIC")
        assert result is not None
        uuid, attrs = result
        assert uuid == "e28d70a7-9314-4542-a4ce-7d95b862070f"
        assert attrs["bucket"] == "openneuro.org"

    def test_find_s3_remote_missing(self):
        remotes = parse_remote_log(self.SAMPLE)
        assert find_s3_remote(remotes, "nonexistent") is None

    def test_empty_input(self):
        assert parse_remote_log("") == {}
        assert parse_remote_log("  \n  \n") == {}


class TestParseLogFile:
    SAMPLE = textwrap.dedent("""\
        1534885659.567118843s 1 66a7004d-a15e-4764-90cd-54bbd179f74a
        1534864953.82370134s 1 b8b60a40-f339-4ddc-b08a-2a6f645bd3ef
        1536597721.51113742s 1 e28d70a7-9314-4542-a4ce-7d95b862070f
    """)

    def test_parse_entries(self):
        entries = parse_log_file(self.SAMPLE)
        assert len(entries) == 3
        assert entries[0]["timestamp"] == "1534885659.567118843s"
        assert entries[0]["present"] is True
        assert entries[0]["uuid"] == "66a7004d-a15e-4764-90cd-54bbd179f74a"

    def test_absent_entry(self):
        entries = parse_log_file("1234567890.0s 0 some-uuid")
        assert len(entries) == 1
        assert entries[0]["present"] is False

    def test_empty(self):
        assert parse_log_file("") == []


class TestParseRmetFile:
    SAMPLE = "1590111061.001560314s e28d70a7-9314-4542-a4ce-7d95b862070f:V +TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr#ds000113/derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat\n"

    def test_parse(self):
        entries = parse_rmet_file(self.SAMPLE)
        assert len(entries) == 1
        e = entries[0]
        assert e["timestamp"] == "1590111061.001560314s"
        assert e["uuid"] == "e28d70a7-9314-4542-a4ce-7d95b862070f"
        assert e["version_id"] == "TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr"
        assert "ds000113/" in e["s3_path"]

    def test_empty(self):
        assert parse_rmet_file("") == []


class TestExtractSizeFromKey:
    def test_md5e(self):
        assert extract_size_from_key(
            "MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz"
        ) == 161017800

    def test_small_file(self):
        assert extract_size_from_key(
            "MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat"
        ) == 405

    def test_sha1(self):
        # SHA1 keys don't have size
        assert extract_size_from_key(
            "SHA1--cd3b8d583a83d14c97228f0b46e65b7c766e3fad"
        ) is None

    def test_sha256e_with_size(self):
        assert extract_size_from_key(
            "SHA256E-s12345--abc123.txt"
        ) == 12345


class TestExtractChecksumFromKey:
    def test_md5e(self):
        result = extract_checksum_from_key(
            "MD5E-s186016172--b905bd144175d465b60ba9a14d229d9e.nii.gz"
        )
        assert result == ("md5", "b905bd144175d465b60ba9a14d229d9e")

    def test_md5e_small(self):
        result = extract_checksum_from_key(
            "MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat"
        )
        assert result == ("md5", "eb58560408b3bca63a5673cd470972fa")

    def test_sha1(self):
        result = extract_checksum_from_key(
            "SHA1--cd3b8d583a83d14c97228f0b46e65b7c766e3fad"
        )
        assert result == ("sha1", "cd3b8d583a83d14c97228f0b46e65b7c766e3fad")

    def test_sha256e(self):
        result = extract_checksum_from_key(
            "SHA256E-s12345--abc123def456789.txt"
        )
        assert result == ("sha256", "abc123def456789")

    def test_no_checksum(self):
        # Key without '--' separator
        assert extract_checksum_from_key("WORM-12345") is None

    def test_unknown_hash_type(self):
        # Unknown prefix
        assert extract_checksum_from_key("UNKNOWN--abcdef") is None


class TestExtractKeyFromAnnexPath:
    def test_log(self):
        assert extract_key_from_annex_path(
            "23a/e1e/MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz.log"
        ) == "MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz"

    def test_rmet(self):
        assert extract_key_from_annex_path(
            "f90/250/MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat.log.rmet"
        ) == "MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat"

    def test_web(self):
        assert extract_key_from_annex_path(
            "abc/def/KEY.txt.log.web"
        ) == "KEY.txt"


class TestKeyStem:
    def test_log(self):
        assert key_stem("abc/def/KEY.nii.gz.log") == "abc/def/KEY.nii.gz"

    def test_rmet(self):
        assert key_stem("abc/def/KEY.nii.gz.log.rmet") == "abc/def/KEY.nii.gz"

    def test_web(self):
        assert key_stem("abc/def/KEY.nii.gz.log.web") == "abc/def/KEY.nii.gz"


class TestIncrementTimestamp:
    def test_basic(self):
        assert increment_timestamp("1534885659.567118843s") == "1534885660.567118843s"

    def test_integer(self):
        assert increment_timestamp("100s") == "101s"


class TestFormatRmetLine:
    def test_format(self):
        line = format_rmet_line(
            "1234567890.0s",
            "e28d70a7-9314-4542-a4ce-7d95b862070f",
            "TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr",
            "ds000113/some/file.nii.gz",
        )
        assert line == (
            "1234567890.0s e28d70a7-9314-4542-a4ce-7d95b862070f"
            ":V +TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr"
            "#ds000113/some/file.nii.gz\n"
        )


class TestParseExportLog:
    SAMPLE = textwrap.dedent("""\
        1534885655.499159794s b8b60a40-f339-4ddc-b08a-2a6f645bd3ef:66a7004d-a15e-4764-90cd-54bbd179f74a c85a1ab87ddbb6422e8e878ed5bae5bcf396692a
        1592541184.985931609s b8b60a40-f339-4ddc-b08a-2a6f645bd3ef:e28d70a7-9314-4542-a4ce-7d95b862070f c85a1ab87ddbb6422e8e878ed5bae5bcf396692a 71f1469f4b43994892b0cec09d926439c5e0b91a
    """)

    def test_find_trees_for_s3_public(self):
        trees = parse_export_log(
            self.SAMPLE, "e28d70a7-9314-4542-a4ce-7d95b862070f"
        )
        # Most recent first
        assert trees == [
            "71f1469f4b43994892b0cec09d926439c5e0b91a",
            "c85a1ab87ddbb6422e8e878ed5bae5bcf396692a",
        ]

    def test_find_trees_for_s3_private(self):
        trees = parse_export_log(
            self.SAMPLE, "66a7004d-a15e-4764-90cd-54bbd179f74a"
        )
        assert trees == ["c85a1ab87ddbb6422e8e878ed5bae5bcf396692a"]

    def test_nonexistent_remote(self):
        assert parse_export_log(self.SAMPLE, "nonexistent") == []

    def test_empty(self):
        assert parse_export_log("", "anything") == []


class TestGroupAnnexKeys:
    SAMPLE_ENTRIES = [
        {"mode": "100644", "type": "blob", "hash": "aaa", "path": "ffc/a4f/MD5E-s1132--4e5d3bbf07d929f5b26ba726d4aeed6e.txt.log"},
        {"mode": "100644", "type": "blob", "hash": "bbb", "path": "ffc/a4f/MD5E-s1132--4e5d3bbf07d929f5b26ba726d4aeed6e.txt.log.rmet"},
        {"mode": "100644", "type": "blob", "hash": "ccc", "path": "ffd/830/MD5E-s22890065--8dd4913caf83e31605e3caef37a6e399.nii.gz.log"},
        {"mode": "100644", "type": "blob", "hash": "ddd", "path": "remote.log"},
        {"mode": "100644", "type": "blob", "hash": "eee", "path": "uuid.log"},
    ]

    def test_groups(self):
        groups = group_annex_keys(self.SAMPLE_ENTRIES)
        # Should have 2 key groups (remote.log and uuid.log excluded)
        assert len(groups) == 2

    def test_key_with_rmet(self):
        groups = group_annex_keys(self.SAMPLE_ENTRIES)
        stem = "ffc/a4f/MD5E-s1132--4e5d3bbf07d929f5b26ba726d4aeed6e.txt"
        assert "log" in groups[stem]
        assert "rmet" in groups[stem]

    def test_key_without_rmet(self):
        groups = group_annex_keys(self.SAMPLE_ENTRIES)
        stem = "ffd/830/MD5E-s22890065--8dd4913caf83e31605e3caef37a6e399.nii.gz"
        assert "log" in groups[stem]
        assert "rmet" not in groups[stem]

    def test_top_level_excluded(self):
        groups = group_annex_keys(self.SAMPLE_ENTRIES)
        assert not any("remote.log" in k for k in groups)
        assert not any("uuid.log" in k for k in groups)


# -- Integration tests (clone ds000113 fresh from GitHub) --


DS000113_URL = "https://github.com/OpenNeuroDatasets/ds000113.git"


@pytest.fixture(scope="module")
def ds000113_clone(tmp_path_factory):
    """Clone ds000113 fresh from GitHub for integration testing.

    Module-scoped so it's shared across all integration tests (one clone).
    Uses full clone (not --depth=1) so git-annex branch is properly fetched,
    then `git annex init` merges it via git-annex's own machinery.
    """
    dest = str(tmp_path_factory.mktemp("ds000113"))
    subprocess.run(
        ["git", "clone", DS000113_URL, dest],
        check=True, capture_output=True, text=True,
    )
    # git annex init properly fetches and merges origin/git-annex
    subprocess.run(
        ["git", "annex", "init", "test-integration"],
        check=True, capture_output=True, text=True, cwd=dest,
    )
    return dest


@pytest.mark.ai_generated
@pytest.mark.integration
class TestIntegrationDs000113:
    """Integration tests against a fresh ds000113 clone from GitHub."""

    @pytest.fixture
    def repo(self, ds000113_clone):
        return ds000113_clone

    def test_remote_log_has_s3_public(self, repo):
        content = subprocess.run(
            ["git", "show", "git-annex:remote.log"],
            capture_output=True, text=True, cwd=repo, check=True,
        ).stdout
        remotes = parse_remote_log(content)
        result = find_s3_remote(remotes, "s3-PUBLIC")
        assert result is not None
        uuid, attrs = result
        assert attrs["bucket"] == "openneuro.org"
        assert "ds000113" in attrs.get("fileprefix", "")

    def test_find_keys_without_rmet(self, repo):
        """Fresh clone should have keys on s3-PUBLIC missing .log.rmet."""
        from click.testing import CliRunner
        from annex_s3_fixer import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["-C", repo, "find-keys-without-urls"])
        assert result.exit_code == 0
        lines = [l for l in result.output.strip().splitlines() if l and not l.startswith("\n")]
        assert len(lines) > 0, "Expected to find keys without URLs"
        assert any(
            "MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14" in l
            for l in lines
        ), f"Expected known bad key in output, got: {lines[:5]}"

    def test_find_files_without_urls(self, repo):
        """Fresh clone should have files on s3-PUBLIC without URLs."""
        from click.testing import CliRunner
        from annex_s3_fixer import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["-C", repo, "find-files-without-urls"])
        assert result.exit_code == 0
        lines = [l for l in result.output.strip().splitlines() if l]
        assert any(
            "sub-01_ses-auditoryperception_task-auditoryperception_run-01_bold" in l
            for l in lines
        ), f"Expected known bad file in output, got: {lines[:5]}"

    def test_fix_dry_run(self, repo):
        """Dry run should propose fixes."""
        from click.testing import CliRunner
        from annex_s3_fixer import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["-C", repo, "fix-missing-s3-urls", "--dry-run", "--limit", "2"],
        )
        assert result.exit_code == 0
        assert "WOULD FIX" in result.output

    def test_all_s3_remotes_dry_run(self, repo):
        """--all-s3-remotes should detect both s3-PUBLIC and s3-PRIVATE."""
        from click.testing import CliRunner
        from annex_s3_fixer import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["-C", repo, "fix-missing-s3-urls",
                  "--all-s3-remotes", "--dry-run", "--limit", "2"],
        )
        assert result.exit_code == 0
        # Should mention s3-PRIVATE being skipped (public=no)
        assert "Skipping" in result.output and "s3-PRIVATE" in result.output
        # Should still fix s3-PUBLIC keys
        assert "WOULD FIX" in result.output


@pytest.mark.ai_generated
@pytest.mark.integration
class TestEndToEndFix:
    """End-to-end test: apply fix then verify git annex get works."""

    # Use a tiny file that appears early in the fix order (sorted by key hash path)
    TEST_FILE = "sub-01/ses-movie/func/sub-01_ses-movie_task-movie_run-8_defacemask.nii.gz"
    TEST_KEY = "MD5E-s1733--997ecb86e29576f604716c420d05f803.nii.gz"
    TEST_SIZE = 1733

    @pytest.fixture
    def disposable_clone(self, tmp_path):
        """Clone ds000113 fresh from GitHub — guaranteed broken state."""
        dest = str(tmp_path / "ds000113")
        subprocess.run(
            ["git", "clone", DS000113_URL, dest],
            check=True, capture_output=True, text=True,
        )
        subprocess.run(
            ["git", "annex", "init", "test-disposable"],
            check=True, capture_output=True, text=True, cwd=dest,
        )
        return dest

    def test_broken_before_fix(self, disposable_clone):
        """Before fix, git annex get warns about missing S3 version ID."""
        result = subprocess.run(
            ["git", "annex", "get", "--from", "s3-PUBLIC", self.TEST_FILE],
            capture_output=True, text=True, cwd=disposable_clone,
            timeout=60,
        )
        # git-annex may still succeed via exporttree scan, but should warn
        combined = result.stdout + result.stderr
        assert "no S3 version ID is recorded" in combined, (
            f"Expected versionId warning, got stdout={result.stdout!r} stderr={result.stderr!r}"
        )

    def test_fix_and_get(self, disposable_clone):
        """Apply the fix, then git annex get should work without warnings."""
        from click.testing import CliRunner
        from annex_s3_fixer import cli

        runner = CliRunner()

        # Apply the fix (not dry run)
        result = runner.invoke(
            cli,
            ["-C", disposable_clone, "fix-missing-s3-urls", "--apply", "--limit", "5"],
        )
        assert result.exit_code == 0, f"Fix failed: {result.output}"
        # CliRunner mixes stdout+stderr into result.output
        assert "fixed" in result.output.lower()

        # Now git annex get should work cleanly — no versionId warning
        get_after = subprocess.run(
            ["git", "annex", "get", "--from", "s3-PUBLIC", self.TEST_FILE],
            capture_output=True, text=True, cwd=disposable_clone,
            timeout=60,
        )
        assert get_after.returncode == 0, (
            f"git annex get failed after fix: {get_after.stderr}"
        )
        combined = get_after.stdout + get_after.stderr
        assert "no S3 version ID is recorded" not in combined, (
            f"Still got versionId warning after fix: {combined}"
        )

        # Verify the file content is present and correct size
        filepath = os.path.join(disposable_clone, self.TEST_FILE)
        assert os.path.exists(filepath)
        size = os.path.getsize(filepath)
        assert size == self.TEST_SIZE, f"Expected {self.TEST_SIZE} bytes, got {size}"
