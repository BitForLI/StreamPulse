.PHONY: contract-test generator-test generator-smoke analytics-test recommendation-test recommendation-e2e detector-test detector-e2e clickhouse-migrate clickhouse-catchup-test flink-restart-test watermark-lateness-test demo compose-config upstream-up upstream-down topics clickhouse-benchmark

contract-test:
	python -m unittest discover -s tests/schema -v

generator-test:
	cd services/event-generator && go test ./...

generator-smoke:
	cd services/event-generator && go run ./cmd/generator -config ../../experiments/scenarios/smoke.yaml -output jsonl -output-file ../../.tmp/generator-smoke.jsonl -manifest ../../.tmp/generator-smoke-manifest.json
	python scripts/validate-generated-events.py .tmp/generator-smoke.jsonl

analytics-test:
	cd jobs/cdn-analytics && mvn test

recommendation-test:
	cd services/recommendation-api && go test ./... && go vet ./...

recommendation-e2e:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/recommendation-e2e.ps1

detector-test:
	python -m unittest discover -s tests/experiments -v

detector-e2e:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-detector-comparison.ps1

clickhouse-migrate:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/apply-clickhouse-migrations.ps1

clickhouse-catchup-test:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-clickhouse-catchup.ps1

flink-restart-test:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-flink-restart.ps1

watermark-lateness-test:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test-watermark-lateness.ps1

demo:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/demo.ps1

compose-config:
	docker compose -f compose.yaml config

upstream-up:
	docker compose -f compose.yaml up --build -d

upstream-down:
	docker compose -f compose.yaml down

topics:
	docker compose -f compose.yaml exec -T kafka sh < infra/kafka/create-topics.sh

clickhouse-benchmark:
	powershell -NoProfile -ExecutionPolicy Bypass -File scripts/benchmark-clickhouse.ps1
