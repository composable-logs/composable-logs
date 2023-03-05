# Docker compose setup for Grafana setup

```
# start Grafana with Tempo backend
docker-compose up -d

# open Grafana UI at http://127.0.0.1:3000

# stop service
docker-compose down
```


### Copyright and license notice
The files (`docker-compose.yaml`, `grafana-datasources.yaml`, `otel-collector.yaml` and `tempo.yaml`) in this directory are based on the `otel-collector` example setup provided in https://github.com/grafana/tempo (commit `07208fa178587a6906ede184bde4c056228f3806`, 3/2023).

Direct links to the original files:
- [`example/docker-compose/otel-collector @07208fa1`](https://github.com/grafana/tempo/tree/07208fa178587a6906ede184bde4c056228f3806/example/docker-compose/otel-collector)
- [`example/docker-compose/shared @07208fa1`](https://github.com/grafana/tempo/tree/07208fa178587a6906ede184bde4c056228f3806/example/docker-compose/shared)

Please note that Grafana/Tempo is distributed under the terms of the AGPL-3.0 license. See (https://github.com/grafana/tempo/blob/07208fa178587a6906ede184bde4c056228f3806/LICENSE) for details.
This license is not compatible with the MIT license.
The modified files are here included under the assumption that this represents fair use.
