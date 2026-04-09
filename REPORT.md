# OpenNeuro S3 URL Metadata Fix Report

## Problem

OpenNeuro datasets stored as git-annex repos had keys registered as present on
the `s3-PUBLIC` S3 remote but missing `.log.rmet` files containing the S3
`versionId`. Without `.log.rmet`, git-annex cannot construct download URLs for
versioned S3 buckets, making files undownloadable (error: "Remote is configured
to use versioning, but no S3 version ID is recorded for this key").

Reference: https://github.com/OpenNeuroOrg/openneuro/issues/3875

## Tool

`annex_s3_fixer.py` — queries S3 anonymously for object versions, matches by
file size (and checksum for disambiguation), generates `.log.rmet` entries, and
patches the `git-annex` branch using git plumbing commands.

For keys not in the current working tree (removed files, export-only keys), the
tool finds file paths via the export tree recorded in `export.log`.

## Results

| Dataset  | Total keys | Missing .log.rmet | Fixed | Errors | Notes |
|----------|-----------|-------------------|-------|--------|-------|
| ds000113 |     5,654 |             1,951 | 1,951 |      0 | 559 keys resolved via export tree |
| ds001499 |    18,523 |            10,789 |10,789 |      0 | 2,703 keys resolved via export tree |
| ds001506 |     1,435 |             1,201 | 1,201 |      0 | 612 keys resolved via export tree |
| ds006623 |    36,438 |                 0 |     — |      — | s3-PUBLIC has all URLs intact; the issue was files only on `OpenNeuro` remote (not S3) |

## Verification

After applying fixes, all originally reported broken files download successfully
from `s3-PUBLIC` without the "no S3 version ID" warning:

- **ds000113**: `sub-01/ses-auditoryperception/func/sub-01_ses-auditoryperception_task-auditoryperception_run-01_bold.nii.gz` — OK
- **ds001499**: `sub-CSI1/ses-01/func/sub-CSI1_ses-01_task-5000scenes_run-01_bold.nii.gz` — OK
- **ds001506**: `sub-01/ses-imagery01/func/sub-01_ses-imagery01_task-imagery_run-01_bold.nii.gz` — OK

Post-fix scan confirms 0 keys missing URL metadata on all three fixed datasets.

## ds006623 note

ds006623 was not affected by the missing `.log.rmet` issue on `s3-PUBLIC` (all
22,427 files on s3-PUBLIC have URLs). The original download failure for
`sub-02/func/sub-02_task-imagery_run-1_bold.nii.gz` was because that file was
only available on the `OpenNeuro` remote (a different, non-S3 special remote),
not on `s3-PUBLIC` at all. All 39,797 files on the `OpenNeuro` remote have zero
URLs — that is a separate issue unrelated to missing `.log.rmet`.

## How to push fixes upstream

The fixes are committed on the local `git-annex` branch of each dataset clone.
To apply upstream:

```bash
cd ds000113 && git push origin git-annex
cd ../ds001499 && git push origin git-annex
cd ../ds001506 && git push origin git-annex
```
