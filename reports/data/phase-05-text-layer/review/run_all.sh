#!/bin/sh
# Phase 05 review: re-run every acceptance check, sequentially. Run from the AS root.
SP=${1:?usage: run_all.sh SCRATCH_DIR}
NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
LOTA=/Users/Shared/claude_projects/data_scopes/language-of-the-archive
AS=/Users/marijnkoolen/Code/Huygens/archival-structures
D=$AS/reports/data/phase-05-text-layer
export PYTHONNOUSERSITE=1
cd $AS
t() { echo; echo "=== $1 ($(date +%T))"; }
# AS .venv on 2026-10-15 lacked pytest's py.py; the review put a copy of the shim on PYTHONPATH.
mkdir -p $SP/s15 $SP/c15
cp $NLP/output/as-phase-05-originals/src/voc_text_layer.py $SP/s15/nlp_voc_text_layer.py
echo "1120,1267,1274,1388,1529,1539,1547,1557,2448,2548,2555,2775,3142,3891,7923,8023,8121,8237,8276,8284,8697,8834" > $SP/c15/inv22.txt

t "protect snapshot";   .venv/bin/python -I $D/step-15c/protect.py snapshot $SP/protect.json
t "AS full suite";      .venv/bin/python -m pytest -q -p no:cacheprovider tests 2>&1 | tail -5
t "AS text-layer targeted"; .venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_packaging_core_imports.py 2>&1 | tail -3
t "step 14 check";      mkdir -p $SP/s14 && .venv/bin/python -I $D/step-14/check.py $SP/s14 2>&1 | tail -25
t "step 15 check ref ab"; /usr/bin/time -p .venv/bin/python -I $D/step-15/check.py $SP/s15 ref ab 2>&1 | tail -30
t "step 15 check scanid"; mkdir -p $SP/s15/sid && .venv/bin/python -I $D/step-15/check.py $SP/s15/sid scanid 2>&1 | tail -15
t "15c CLI 22";         /usr/bin/time -p .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $SP/c15/r22 --stage all --inventories $(cat $SP/c15/inv22.txt) --write-events --workers 8 > $SP/c15/run1.out 2>&1; echo exit $?; tail -3 $SP/c15/run1.out
t "15c check";          .venv/bin/python -I $D/step-15c/check.py $SP/c15 2>&1 | tail -60
t "15c rerun (resume)"; find $SP/c15/r22/voc_text_layer_v1/lines $SP/c15/r22/voc_text_layer_v1/regions -type f -exec stat -f '%N %m' {} \; | sort > $SP/c15/mt1.txt
                        /usr/bin/time -p .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $SP/c15/r22 --stage all --inventories $(cat $SP/c15/inv22.txt) --write-events --workers 8 2>&1 | grep -E "to do|nothing|real|end"
                        find $SP/c15/r22/voc_text_layer_v1/lines $SP/c15/r22/voc_text_layer_v1/regions -type f -exec stat -f '%N %m' {} \; | sort > $SP/c15/mt2.txt
                        cmp $SP/c15/mt1.txt $SP/c15/mt2.txt && echo "mtimes unchanged ($(wc -l < $SP/c15/mt1.txt) files)"
t "15c recipe";         sh $D/step-15c/recipe.sh $SP/c15/r3 2>&1 | tail -12; echo exit $?
t "15c help/estimate";  .venv/bin/as-build-text-layer --help | grep -oE '^\s+--[a-z-]+' | tr -d ' ' | sort | tr '\n' ' '; echo
                        .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $SP/c15/est --estimate --inventories $(cat $SP/c15/inv22.txt) --workers 8 2>&1 | tail -6
t "NLP verify";         cd $NLP && /usr/bin/time -p venv/bin/python scripts/verify_as_text_layer.py --out-dir $SP/nlpverify 2>&1 | tail -40; echo exit $?
t "NLP tests";          venv/bin/python -m pytest -q -p no:cacheprovider tests/test_voc_text_layer.py 2>&1 | tail -2
                        venv/bin/python -m pytest -q -p no:cacheprovider tests/test_evaluation.py tests/test_semi_markov.py 2>&1 | tail -2
t "LotA suite";         cd $LOTA && venv/bin/python -m pytest -q -p no:cacheprovider 2>&1 | tail -3
t "envcheck";           for py in $AS/.venv/bin/python $NLP/venv/bin/python $LOTA/venv/bin/python; do echo "-- $py"; $py -I $D/step-13/envcheck.py; done
cd $AS
t "protect verify";     .venv/bin/python -I $D/step-15c/protect.py verify $SP/protect.json; echo exit $?
t "done"
