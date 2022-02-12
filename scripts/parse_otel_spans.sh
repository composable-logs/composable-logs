#!/bin/bash
#
# Wrapper around pynb_dag_runner pynb_log_parser cli
#
# eg.
# parse_otel_spans.sh <docker-image-name> /path/to/spans.json <output-flag> <output-path>
#

DOCKER_IMAGE=$1
INPUT_FILEPATH=$2
OUTPUT_FLAG=$3  # "output_directory", "output_filepath_mermaid_gantt"
OUTPUT_FILEPATH=$4

INPUT_DIR=$(dirname $INPUT_FILEPATH)
OUTPUT_DIR=$(dirname $OUTPUT_FILEPATH)

docker run \
    --rm \
    --network none \
    --volume $INPUT_DIR:/input_dir:ro \
    --volume $OUTPUT_DIR:/output_dir \
    --entrypoint pynb_log_parser \
    $DOCKER_IMAGE \
        --input_span_file /input_dir/$(basename $INPUT_FILEPATH) \
        --$OUTPUT_FLAG /output_dir/$(basename $OUTPUT_FILEPATH)
