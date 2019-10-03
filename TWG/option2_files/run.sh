#! /bin/bash

python test_plt_formats.py ${NUM_PERIODS:-100} ${EVENT_RATE:-10} ${NUM_RISKS:-10} ${NUM_SAMPLES:-10} ${OUTPUT_FILE:-/tmp/test} --output-type all