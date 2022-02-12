#!/bin/bash
set -eu

echo "====================== process-otel-spans ====================="

OPENTELEMETRY_SPANS_JSON_FILEPATH=$1
PYTHON_DOCKER_IMAGE_NAME=$2
OUTPUT_BASEDIR=$3

echo "==============================================================="

echo "--- Converting: otel spans json -> directory structure with json:s and notebooks ..."
SCRIPT_DIRECTORY="$(dirname $(realpath $0))"

$SCRIPT_DIRECTORY/parse_otel_spans.sh \
    $PYTHON_DOCKER_IMAGE_NAME \
    $OPENTELEMETRY_SPANS_JSON_FILEPATH \
    output_directory \
    $OUTPUT_BASEDIR/pipeline-outputs

echo "==============================================================="
echo "--- Converting: otel spans json -> Mermaid input file for Gantt diagram ..."

$SCRIPT_DIRECTORY/parse_otel_spans.sh \
    $PYTHON_DOCKER_IMAGE_NAME \
    $OPENTELEMETRY_SPANS_JSON_FILEPATH \
    output_filepath_mermaid_gantt \
    $OUTPUT_BASEDIR/gantt.mmd

echo "==============================================================="
echo "--- Converting: Mermaid input file for Gantt diagram -> png"
$SCRIPT_DIRECTORY/render_mermaid.sh $OUTPUT_BASEDIR/gantt.mmd $OUTPUT_BASEDIR/gantt-diagram.png
rm $OUTPUT_BASEDIR/gantt.mmd

echo "==============================================================="
echo "--- Converting: Done"
echo "==============================================================="
