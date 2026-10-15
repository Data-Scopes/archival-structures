#!/bin/sh
# Criterion 4: the v2.1 recipe, run locally on 3 inventories against NLP's <REF>.
# usage: recipe.sh OUT_DIR      (run from the repo root; PYTHONNOUSERSITE=1 in agent sessions)
set -e
O="$1"; NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
REF=$NLP/output/voc_text_layer/voc_text_layer_v1; INVS=1120,2555,8834
.venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir "$O" --stage extract --inventories $INVS --workers 8
mkdir -p "$O/voc_text_layer_v1/wbc"
cp $REF/wbc/wbc_per_inventory.tsv $REF/wbc/corpus_default.json "$O/voc_text_layer_v1/wbc/"
.venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir "$O" --stage join --inventories $INVS --workers 8
.venv/bin/python -m archival_structures.text_layer.compare "$O/voc_text_layer_v1" $REF --inventories $INVS --out "$O/compare_ref.tsv"
