#!/bin/bash
#
# Wrapper around minlag/mermaid-cli:8.8.0 for easy
# conversion of Mermaid input mmd file into png/svg
#
# eg.
# render_mermaid.sh /path/to/file.mmd /full/path/output.{png,svg}
#
# See:
#  - https://mermaid-js.github.io/mermaid/#/
#  - https://github.com/mermaid-js/mermaid-cli
#  - https://github.com/mermaid-js/mermaid-cli/blob/master/docs/docker-permission-denied.md
#

INPUT_FILE=$1
OUTPUT_FILE=$2

INPUT_DIR=$(dirname $INPUT_FILE)
OUTPUT_DIR=$(dirname $INPUT_FILE)

docker run \
    -u $UID \
    --rm \
    --network none \
    --volume $INPUT_DIR:/input_dir:ro \
    --volume $OUTPUT_DIR:/output_dir \
    minlag/mermaid-cli:8.8.0 \
        --input /input_dir/$(basename $INPUT_FILE) \
        --output /output_dir/$(basename $OUTPUT_FILE)
