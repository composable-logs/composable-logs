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

#### (deprecated) Generate data suitable for static website
```bash
# First: download selection of zip files to `./cache` directory
#
# Note:
# This command assumes that the json in each zip file has been expanded into a
# directory structure using pynb_log_parser
#
# This should no longer be used, and the command should be removed when no
# longer needed.
static_builder \
    --zip_cache_dir $(pwd)/cache \
    --output_dir $(pwd)/out/ \
    --output_static_data_json $(pwd)/out/static_website_data.json
```
