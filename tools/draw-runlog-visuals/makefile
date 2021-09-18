SHELL := /bin/bash

draw-runlog-visuals:
	echo "RUNLOGS_ROOT=$${RUNLOGS_ROOT:?}"   # ensure parameter is given

	# convert runlog.json files under RUNLOGS_ROOT into Mermaid input file
	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    --volume $$(pwd):/app-src:ro \
	    pynb-dag-runner-base \
	        "python3 /app-src/runtimes-gantt-chart.py \
	            --runlogs_root /runlogs_root \
	            --output /runlogs_root/runtimes-gantt-chart.mmd"

	# render Mermaid input file to png, see
	#  - https://github.com/mermaid-js/mermaid-cli
	#  - https://hub.docker.com/r/minlag/mermaid-cli
	docker pull minlag/mermaid-cli:8.8.0

	# Note/TODO: if upgrading to 8.12.1, the below fails with
	# [Error: EACCES: permission denied, open '/data/runtimes-gantt-chart.png']
	docker run --rm \
	    --network none \
	    --volume $(RUNLOGS_ROOT):/runlogs_root \
	    minlag/mermaid-cli:8.8.0 \
	        --scale 2 \
	        --input /runlogs_root/runtimes-gantt-chart.mmd \
	        --output /runlogs_root/runtimes-gantt-chart.png

	rm $(RUNLOGS_ROOT)/runtimes-gantt-chart.mmd
