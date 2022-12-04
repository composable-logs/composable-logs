### Command line tools for manipulating OpenTelemetry span JSON:s

```bash
pip install -e .    # Install cli:s for local dev

# print out syntax
static_builder --help
pynb_log_parser --help
generate_static_data --help
```

#### Expand one OpenTelemetry JSON into a directory structure (eg for debugging)
```bash
# First: copy a pipeline trace JSON to opentelemetry-spans.json
pynb_log_parser \
    --input_span_file opentelemetry-spans.json \
    --output_filepath_mermaid_gantt $(pwd)/out/gantt.mmd \
    --output_filepath_mermaid_dag $(pwd)/out/dag.mmd
```

#### Generate data suitable for static website
```bash
# First: download selection of zip files to `./cache` directory
generate_static_data \
    --zip_cache_dir $(pwd)/cache \
    --output_www_root_directory $(pwd)/www-root
```
