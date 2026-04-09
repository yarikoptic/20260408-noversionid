# OpenNeuro S3 URL Metadata Fix Report

## Problem

OpenNeuro datasets stored as git-annex repos had keys registered as present on
S3 remotes but missing `.log.rmet` files containing the S3 `versionId`. Without
`.log.rmet`, git-annex cannot construct download URLs for versioned S3 buckets
(error: "Remote is configured to use versioning, but no S3 version ID is
recorded for this key").

Reference: https://github.com/OpenNeuroOrg/openneuro/issues/3875

## Tool

`annex_s3_fixer.py` — Python CLI (click + boto3) with three commands:

- **`find-keys-without-urls`** — scans git-annex branch for keys missing `.log.rmet`
- **`find-files-without-urls`** — checks current tree files via `git annex whereis --json`
- **`fix-missing-s3-urls`** — queries S3 anonymously for object versions, matches
  by file size (and checksum for disambiguation), generates `.log.rmet` entries,
  patches the `git-annex` branch using git plumbing commands

Key features:
- `--all-s3-remotes` auto-detects all versioned S3 remotes; skips private ones
  (`public=no`) with a warning
- Keys not in current tree are resolved via export tree from `export.log`
- SHA1 keys (no size in key name) sized via `git cat-file -s`
- Appends to existing `.log.rmet` files (multi-remote safe)
- Uses git plumbing (read-tree/update-index/write-tree/commit-tree) to commit
  directly to the git-annex branch without disturbing the working tree

## Setup for running the fixer

Clone the dataset with `annex.private=true` to avoid leaking temporary clone
info into the git-annex branch (see
https://git-annex.branchable.com/tips/cloning_a_repository_privately/):

```bash
git clone https://github.com/OpenNeuroDatasets/$ds.git
cd $ds
git config annex.private true
git annex init "fix-session"
python annex_s3_fixer.py fix-missing-s3-urls --all-s3-remotes --apply
```

## Results

All datasets cloned fresh from GitHub with `annex.private=true`.

| Dataset  | Total keys | Missing .log.rmet | Fixed | Errors | Notes |
|----------|-----------|-------------------|-------|--------|-------|
| ds000113 |     5,654 |             1,951 | 1,951 |      0 | 559 keys via export tree; s3-PRIVATE skipped (public=no) |
| ds001499 |    18,523 |            10,789 |10,789 |      0 | 2,703 keys via export tree; s3-PRIVATE skipped (public=no) |
| ds001506 |     1,435 |             1,201 | 1,201 |      0 | 612 keys via export tree; s3-PRIVATE skipped (public=no) |
| ds006623 |    36,438 |                 0 |     — |      — | s3-PUBLIC has `public=no`; 0 missing keys anyway |

## Verification

After applying fixes, all originally reported broken files download with correct
`versionId` from `s3-PUBLIC`, confirmed via `--debug`:

- **ds000113**: `sub-01_ses-auditoryperception_...bold.nii.gz` — `versionId=...` OK, no warnings
- **ds001499**: `sub-CSI1_ses-01_task-5000scenes_...bold.nii.gz` — `versionId=IQPHsybDPAX9WjVQwwJqt3iOLmKQZrUr` OK
- **ds001506**: `sub-01_ses-imagery01_task-imagery_...bold.nii.gz` — `versionId=HXwcUkJ3i_cQdyV6zz9AVw_w8FXphYUM` OK

Post-fix scan confirms 0 keys missing URL metadata on all three fixed datasets.
`origin/git-annex` remote tracking branch preserved. Clone UUID not leaked to
git-annex branch (private clone).

### s3-PRIVATE warnings

s3-PRIVATE also has `versioning=yes` but `public=no`, so it cannot be queried
anonymously. git-annex tries s3-PRIVATE before s3-PUBLIC, producing "no S3
version ID" warnings before falling back to s3-PUBLIC (which now works). To
eliminate these warnings, run the fixer with AWS credentials for the
`openneuro-private` bucket:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  python annex_s3_fixer.py fix-missing-s3-urls --remote s3-PRIVATE --apply
```

## ds006623 note

ds006623 was not affected by the missing `.log.rmet` issue. Its `s3-PUBLIC`
remote has `public=no` and all keys already have URLs. The original download
failure for `sub-02/func/sub-02_task-imagery_run-1_bold.nii.gz` was because
that file was only on the `OpenNeuro` remote (a different, non-S3 special
remote) with zero URLs — a separate issue.

## How to push fixes upstream

The fixes are committed on the local `git-annex` branch. The clone's own UUID
is not present on the branch (private clone), so pushing is clean:

```bash
cd ds000113 && git push origin git-annex
cd ../ds001499 && git push origin git-annex
cd ../ds001506 && git push origin git-annex
```
