#!/bin/zsh
# Step 15y step 6: `size mtime path` of every file of the 1.01.02 layer except the root status.tsv, manifest.json,
# progress.log, */stats.json, */params.json and the top-level files of wbc/. Run from the AS root: resume_list.sh LAYER
L=$1
find $L -type f ! -path "$L/status.tsv" ! -path "$L/manifest.json" ! -path "$L/progress.log" ! -name stats.json ! -name params.json ! \( -path "$L/wbc/*" ! -path "$L/wbc/*/*" \) | sort | xargs stat -f "%z %m %N"
