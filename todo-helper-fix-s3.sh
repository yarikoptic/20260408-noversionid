The issue https://github.com/OpenNeuroOrg/openneuro/issues/3875 provides details for the situation.  Citing here in full:

### Original post

    ### What went wrong?

    in the scope of working on https://github.com/OpenNeuroStudies/OpenNeuroStudies ran into the case where among those few I am "prototyping on" 4 (ds000113, ds001499, ds001506, ds006623) had issues as no URLs available for the load.

    here is a list with sample files


    ```
      │ ds000113 │ sub-01/ses-auditoryperception/func/sub-01_ses-auditoryperception_task-auditoryperception_run-01_bold.nii.gz │ s3-PUBLIC (3 copies) │ YES (HTTP 200) │
      ├──────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────┼──────────────────────┼────────────────┤
      │ ds001499 │ sub-CSI1/ses-01/func/sub-CSI1_ses-01_task-5000scenes_run-01_bold.nii.gz                                     │ s3-PUBLIC (3 copies) │ YES (HTTP 200) │
      ├──────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────┼──────────────────────┼────────────────┤
      │ ds001506 │ sub-01/ses-imagery01/func/sub-01_ses-imagery01_task-imagery_run-01_bold.nii.gz                              │ s3-PUBLIC (3 copies) │ YES (HTTP 200) │
      ├──────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────┼──────────────────────┼────────────────┤
      │ ds006623 │ sub-02/func/sub-02_task-imagery_run-1_bold.nii.gz                                                           │ OpenNeuro (1 copy)   │ YES (HTTP 200) │
    ```

    I (even without claude, uff) scripted 2 scripts to visualize the problem here -- shared all with duct logs across those 4 at https://github.com/yarikoptic/20260408-noversionid .

    The main point behind 3 of those first ones is that there is no URLs information logged for those keys at all. Files provide comparison to keys which do, e.g.

    https://github.com/yarikoptic/20260408-noversionid/blob/master/ds000113.log#L31

    ```
    whereis sub-01/ses-auditoryperception/func/sub-01_ses-auditoryperception_task-auditoryperception_run-01_bold.nii.gz (3 copies)
        66a7004d-a15e-4764-90cd-54bbd179f74a -- [s3-PRIVATE]
        b8b60a40-f339-4ddc-b08a-2a6f645bd3ef -- root@8dc3dbd70baf:/datalad/ds001473
        e28d70a7-9314-4542-a4ce-7d95b862070f -- [s3-PUBLIC]
    ok
    ---- Key before get
    -- 23a/e1e/MD5E-s161017800--e3d1a133a2b66ddc0bfbb6941fa7ef14.nii.gz.log:
    1534885659.567118843s 1 66a7004d-a15e-4764-90cd-54bbd179f74a
    1534864953.82370134s 1 b8b60a40-f339-4ddc-b08a-2a6f645bd3ef
    1536597721.51113742s 1 e28d70a7-9314-4542-a4ce-7d95b862070f
    ```

    and then key which is ok

    ```
    whereis derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat (2 copies)
        b8b60a40-f339-4ddc-b08a-2a6f645bd3ef -- root@8dc3dbd70baf:/datalad/ds001473
        e28d70a7-9314-4542-a4ce-7d95b862070f -- [s3-PUBLIC]

      s3-PUBLIC: https://s3.amazonaws.com/openneuro.org/ds000113/derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat?versionId=TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr
    ok
    ---- Key before get
    -- f90/250/MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat.log:
    1537582851.069763384s 1 b8b60a40-f339-4ddc-b08a-2a6f645bd3ef
    1590111061.142173652s 1 e28d70a7-9314-4542-a4ce-7d95b862070f
    -- f90/250/MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat.log.rmet:
    1590111061.001560314s e28d70a7-9314-4542-a4ce-7d95b862070f:V +TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr#ds000113/derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat
    ```

    ### needed:

    so, to fix, I see it needing code which
    - traverses the git-annex keys which have no .rmet but present in that s3-PUBLIC remote
    - for the path associated with them get versions on S3 with their versionIds and sizes and match the versionId
      - match by size:
         - If multiple observed -- fail, we could figure out later if needed (e.g. download all and match checksum or go by ETags etc)
         - if no match -- overall error for that dataset so we know that some keys didn't find corresponding path
       - if match (some datasets potentially would have none) -- given such information, produce `.log.rmet` file for that key in the `git-annex` branch.  timestamp could be taken from .log file for that remote + 1 second as
         -  there were no record there whatsoever
         - we now do get time AFTER we marked availability in that remote, so 'git-annex' AFAIK would merge fine.

    I think the same issue might have been observed in other issues:
    - https://github.com/OpenNeuroOrg/openneuro/issues/3090#issuecomment-4048432595
    - https://github.com/OpenNeuroOrg/openneuro/issues/2086 on the same 000113 (I think part of the problem was hidden by me using `datalad get`)
    - https://github.com/OpenNeuroOrg/openneuro/issues/2049 on 000248

    and I guess it might be just sipping through the fix via re-export done in

    - https://github.com/OpenNeuroOrg/openneuro/issues/1731

    Filed issue in git-annex to facilitate fixing on git-annex side:
    - https://git-annex.branchable.com/todo/fsck_against_versioned_S3_should_populate_log.rmet/

    ### Expected behavior

    have URL associated

    ### How to reproduce

    see above and in https://github.com/yarikoptic/20260408-noversionid


### Chris's reply

    I'm having trouble turning this into actionable issues.

    The unversioned S3 remotes are a lingering problem from older versions of OpenNeuro/git-annex. They've generally become obvious problems when attempting to create new releases that modify old data instead of simply adding files. We've generally fixed them as necessary, but not systematically.

    It would be good to be able to identify these for our dashboard. I suppose git annex fsck --fast --from=s3-PUBLIC could be used, though perhaps there's a more efficient way to detect them.

    Did any of the files fail to download at all?


### My reply

    Did any of the files fail to download at all?

    yes in https://github.com/yarikoptic/20260408-noversionid/blob/master/ds006623.log#L36

    get sub-02/func/sub-02_task-imagery_run-1_bold.nii.gz (not available)
      Maybe add some of these git remotes (git remote add ...):
        185fe801-93f0-42d1-b387-a3568a0b374a -- OpenNeuro

      (Note that these git remotes have annex-ignore set: origin)
    failed
    get: 1 failed
    Good file: derivatives/LOR_ROR_Timing.xlsx
    I'm having trouble turning this into actionable issues. ... We've generally fixed them as necessary, but not systematically.

    Create an analysis/fixup script which does it systematically (see needed I now added in the original description). In principle IMHO it should be just annex fsck functionality, but might also be prudent to do alternative quick implementation.

    As for overall detection, as ideally all annexed keys should have both .log.rmet, could be as "simple" as load entire tree listing of git-annex branch in and find which .log has neither .log.rmet nor .log.web -- if any -- some files do not have remote URLs and you highlight that dataset.


## So what is needed

Write a simple python helper with click interface defining commands

### find-keys-without-urls [PATH or current dir]

where it would run smth like 

    git ls-tree -r git-annex | sort -k 4

which outputs smth like

    100644 blob b2f251255dfd754af3e3eefab1ce9ae682241064    ffc/a4f/MD5E-s1132--4e5d3bbf07d929f5b26ba726d4aeed6e.txt.log
    100644 blob e1c3a86d4aaa17742a3dcd611ae1183ceec078ce    ffc/a4f/MD5E-s1132--4e5d3bbf07d929f5b26ba726d4aeed6e.txt.log.rmet
    100644 blob 6f669634304a29b733b2d444f524a9c42896576d    ffd/830/MD5E-s22890065--8dd4913caf83e31605e3caef37a6e399.nii.gz.log
    100644 blob d08fb86d1b784968e8ccb41dfae54cb89ce09bf2    ffd/830/MD5E-s22890065--8dd4913caf83e31605e3caef37a6e399.nii.gz.log.rmet
    100644 blob 6180a85a21e564e8f080dee9896575094de3dd39    ffe/159/SHA1--cd3b8d583a83d14c97228f0b46e65b7c766e3fad.log
    100644 blob 4d76c327f92675bde81f3c81a66b9ad2906677b4    ffe/159/SHA1--cd3b8d583a83d14c97228f0b46e65b7c766e3fad.log.rmet
    100644 blob 69279cd0bf2d92e345bd7a508ebd6479b3a086df    ffe/3df/MD5E-s11765177--6fe2d2ec55763e99593190ded86d6d35.nii.gz.log
    100644 blob 080ed84cff8bb011682ab59ae6b8458a64f3fe72    ffe/3df/MD5E-s11765177--6fe2d2ec55763e99593190ded86d6d35.nii.gz.log.rmet
    100644 blob 0234018607ea78ba2f14274e81d7826827891469    remote.log
    100644 blob 485fa9b3a31bbdbe46fb0322106db5e48a7c1f8d    uuid.log

 so for each of those key (not top level .log files) we should check if for .log there is either .log.rmet or .log.web.  
 Actually it could be option to look for either .rmet or .web or for both.  .rmet iirc would contain details about urls
 to be minted by special remotes (in remote.log) while .web could contain direct final URLs.  For openneuro in principle we should aim for .rmet

This way we could quickly check for ALL (not only in current tree) keys in a dataset.

### find-files-without-urls

Could just use  git annex whereis --json  which would output for current tree files where they are available from and also URLs per each remote (or universal web remote), e.g.

    $> git annex whereis --json | head -n 1 | jq .
    {
      "command": "whereis",
      "error-messages": [],
      "file": "derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat",
      "input": [
        "derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat"
      ],
      "key": "MD5E-s405--eb58560408b3bca63a5673cd470972fa.mat",
      "note": "3 copies\n\tadeaaa02-bcf8-41dd-b70c-475fc0c7fb3d -- yoh@smaug:/mnt/btrfs/datasets/datalad/tmp/20260408-noversionid/ds000113 [here]\n\tb8b60a40-f339-4ddc-b08a-2a6f645bd3ef -- root@8dc3dbd70baf:/datalad/ds001473\n\te28d70a7-9314-4542-a4ce-7d95b862070f -- [s3-PUBLIC]\n\ns3-PUBLIC: https://s3.amazonaws.com/openneuro.org/ds000113/derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat?versionId=TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr\n",
      "success": true,
      "untrusted": [],
      "whereis": [
        {
          "description": "yoh@smaug:/mnt/btrfs/datasets/datalad/tmp/20260408-noversionid/ds000113",
          "here": true,
          "urls": [],
          "uuid": "adeaaa02-bcf8-41dd-b70c-475fc0c7fb3d"
        },
        {
          "description": "root@8dc3dbd70baf:/datalad/ds001473",
          "here": false,
          "urls": [],
          "uuid": "b8b60a40-f339-4ddc-b08a-2a6f645bd3ef"
        },
        {
          "description": "[s3-PUBLIC]",
          "here": false,
          "urls": [
            "https://s3.amazonaws.com/openneuro.org/ds000113/derivatives/linear_anatomical_alignment/sub-01/ses-forrestgump/func/sub-01_ses-forrestgump_task-forrestgump_rec-XFMdico7Tad2grpbold7Tad_run-01_bold.mat?versionId=TxSZbdFLyz5ATEWs0m9vRFQIozJO7owr"
          ],
          "uuid": "e28d70a7-9314-4542-a4ce-7d95b862070f"
        }
      ]
    }


so you can see that s3-PUBLIC has url for this particular file.

### fix-missing-s3-urls

from remote.log which looks like 

   $> git show git-annex:remote.log
66a7004d-a15e-4764-90cd-54bbd179f74a autoenable=true bucket=openneuro-private datacenter=US encryption=none exporttree=yes fileprefix=ds001473/ host=s3.amazonaws.com name=s3-PRIVATE partsize=1GiB port=80 public=no publicurl=no storageclass=STANDARD type=S3 versioning=yes timestamp=1541104695.225259256s
e28d70a7-9314-4542-a4ce-7d95b862070f autoenable=true bucket=openneuro.org datacenter=US encryption=none exporttree=yes fileprefix=ds000113/ host=s3.amazonaws.com name=s3-PUBLIC partsize=1GiB port=80 public=yes publicurl=https://s3.amazonaws.com/openneuro.org storageclass=STANDARD type=S3 versioning=yes timestamp=1597694369.712881988s


can get prefix for publicurl, and also remote uuid (e28d70a7-9314-4542-a4ce-7d95b862070f) for remote s3-PUBLIC.

Then for each key which does not have s3 url, if we know file (e.g. we identified from current tree) -- we need to get key versions information from s3 (e.g. using boto3 API, anonymous requests), match using size, if multiple matching available -- could potentially go by downloading and checking for checksum and compare to target one which is in git-annex key already, thus deciding what would be that target key and thus versionId to correspond to the git-annex key.  And then create .log.rmet file for that key to patch git-annex branch with.

If operating on key which is not in current tree -- need to collect git log for the repo until encountering a file being removed which points to that key, and then approach using that path.



