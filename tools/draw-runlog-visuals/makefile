SHELL := /bin/bash

draw-runlog-visuals:
	echo "RUNLOGS_ROOT=$${RUNLOGS_ROOT:?}"   # ensure parameter is given

	# Convert runlog.json files under RUNLOGS_ROOT into a Mermaid input file
	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    --volume $$(pwd):/app-src:ro \
	    pynb-dag-runner-base \
	        "python3 /app-src/runtimes-gantt-chart.py \
	            --runlogs_root /runlogs_root \
	            --gantt_mmd_outputfile /runlogs_root/runtimes-gantt-chart.mmd"

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

	rm $(RUNLOGS_ROOT)/runtimes-gantt-chart.mmd
