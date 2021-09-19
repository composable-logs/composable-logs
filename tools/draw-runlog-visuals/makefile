SHELL := /bin/bash

draw-runlog-visuals:
	# After a pipeline has run it produces:
	#  - a directory of runlog files (runlog.json files under RUNLOGS_ROOT).
	#  - a task_dependency.json file listing the dependencies between tasks.
	#
	# This make target converts these inputs into two images (both writted to
	# PNG_OUTPUT_DIRECTORY):
	#
	# runtimes-gantt-chart.png
	#    This image show the relative runtimes of tasks as a Gantt chart. From
	#    the image one can eg. see which tasks have run in parallel.
	#
	# task-dependencies.png
	#    This image show which tasks are dependent on each other. Moreover,
	#    this png file contains run-parameters (eg retry/timeout settings)
	#    for each tasks.
	#

	# ensure the required parameters are given
	echo "RUNLOGS_ROOT=$${RUNLOGS_ROOT:?}"
	echo "PNG_OUTPUT_DIRECTORY=$${PNG_OUTPUT_DIRECTORY:?}"

	# Convert runlog.json files under RUNLOGS_ROOT into a Mermaid input file
	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    --volume $$(pwd):/app-src:ro \
	    pynb-dag-runner-base \
	        "python3 /app-src/render-runlogs.py \
	            --runlogs_root /runlogs_root \
	            --output_gantt_mmd /runlogs_root/runtimes-gantt-chart.mmd \
	            --output_dependency_mmd /runlogs_root/task-dependencies.mmd"

	# Next, render Mermaid input file to a png, see
	#  - https://github.com/mermaid-js/mermaid-cli
	#  - https://hub.docker.com/r/minlag/mermaid-cli
	#
	# Note/TODO: the below uses Mermaid 8.8.0. If we upgrade to 8.12.1, the
	# docker run-command fails with a permission error:
	#
	# [Error: EACCES: permission denied, open '/data/runtimes-gantt-chart.png']
	#
	docker pull minlag/mermaid-cli:8.8.0

	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    --volume $(PNG_OUTPUT_DIRECTORY):/png_output_dir \
	    minlag/mermaid-cli:8.8.0 \
	        --scale 2 \
	        --input /runlogs_root/runtimes-gantt-chart.mmd \
	        --output /png_output_dir/runtimes-gantt-chart.png

	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    --volume $(PNG_OUTPUT_DIRECTORY):/png_output_dir \
	    minlag/mermaid-cli:8.8.0 \
	        --scale 2 \
	        --input /runlogs_root/task-dependencies.mmd \
	        --output /png_output_dir/task-dependencies.png

	rm $(RUNLOGS_ROOT)/{runtimes-gantt-chart,task-dependencies}.mmd
