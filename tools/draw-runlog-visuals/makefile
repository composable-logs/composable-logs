SHELL := /bin/bash

draw-runlog-visuals:
	echo "RUNLOGS_ROOT=$${RUNLOGS_ROOT:?}"   # ensure parameter is given

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
	    minlag/mermaid-cli:8.8.0 \
	        --scale 2 \
	        --input /runlogs_root/runtimes-gantt-chart.mmd \
	        --output /runlogs_root/runtimes-gantt-chart.png

	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    minlag/mermaid-cli:8.8.0 \
	        --scale 2 \
	        --input /runlogs_root/task-dependencies.mmd \
	        --output /runlogs_root/task-dependencies.png

	rm $(RUNLOGS_ROOT)/{runtimes-gantt-chart,task-dependencies}.mmd
