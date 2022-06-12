#!/bin/bash
set -eu

echo "====================== process-otel-spans ====================="

OPENTELEMETRY_SPANS_JSON_FILEPATH=$1
PYTHON_DOCKER_IMAGE_NAME=$2
OUTPUT_BASEDIR=$3

# Get path to this script, so we can call other scripts in the
# same directory.
SCRIPT_DIRECTORY="$(dirname $(realpath $0))"

echo "==============================================================="
echo "--- Converting: otel spans json -> directory structure with"
echo "--- json:s and notebooks ..."
echo "==============================================================="
$SCRIPT_DIRECTORY/parse_otel_spans.sh \
    $PYTHON_DOCKER_IMAGE_NAME \
    $OPENTELEMETRY_SPANS_JSON_FILEPATH \
    output_directory \
    $OUTPUT_BASEDIR/pipeline-outputs

echo "==============================================================="
echo "--- Converting: otel spans json -> Mermaid input files for DAG "
echo "--- and Gantt diagrams ..."
echo "==============================================================="
$SCRIPT_DIRECTORY/parse_otel_spans.sh \
    $PYTHON_DOCKER_IMAGE_NAME \
    $OPENTELEMETRY_SPANS_JSON_FILEPATH \
    output_filepath_mermaid_gantt \
    $OUTPUT_BASEDIR/gantt.mmd

# The below also outputs dag-nolinks.mmd without html links to github.
# That is used below to create png.
$SCRIPT_DIRECTORY/parse_otel_spans.sh \
    $PYTHON_DOCKER_IMAGE_NAME \
    $OPENTELEMETRY_SPANS_JSON_FILEPATH \
    output_filepath_mermaid_dag \
    $OUTPUT_BASEDIR/dag.mmd

echo "==============================================================="
echo "--- Converting: Mermaid input file for Gantt diagram -> png"
echo "==============================================================="
$SCRIPT_DIRECTORY/render_mermaid.sh $OUTPUT_BASEDIR/gantt.mmd $OUTPUT_BASEDIR/gantt-diagram.png
$SCRIPT_DIRECTORY/render_mermaid.sh $OUTPUT_BASEDIR/dag-nolinks.mmd $OUTPUT_BASEDIR/dag-diagram.png

echo "==============================================================="
echo "--- Converting: Done"
echo "==============================================================="
