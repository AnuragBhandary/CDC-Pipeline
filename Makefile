.PHONY: install up down demo sink load verify test lint typecheck check bench bench-smoke

install:          ## dependencies (needs uv)
	uv sync

up:               ## MySQL, Kafka (KRaft), Kafka Connect + Debezium, PostgreSQL
	docker compose up -d --wait

down:
	docker compose down

demo: up          ## reset, seed, start the connector (then run `make sink` and `make load`)
	uv run cdcpipe reset
	uv run cdcpipe seed
	uv run cdcpipe connector create

sink:             ## one sink consumer in the foreground (start more in other terminals)
	uv run cdcpipe sink --instance sink-$${N:-1}

load:             ## 100k row changes at 3,000/s with a column added halfway
	uv run cdcpipe load --events 100000 --rate 3000 --schema-change-at 50000

verify:           ## wait for catch-up, then compare every table (exit 1 on any difference)
	uv run cdcpipe verify

test: up
	uv run pytest --cov --cov-fail-under=85

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test

bench-smoke: up   ## ~1 minute: 50k events, schema change, one sink SIGKILLed
	uv run python bench/run_benchmark.py --events 50000 --rate 3000 --sinks 3 \
		--schema-change-at 20000 --kill-at 30000 --label smoke

bench: up         ## the resume run: 1M events at 4,000/s, schema change at 500k, SIGKILL at 700k
	uv run python bench/run_benchmark.py --events 1000000 --rate 4000 --writers 8 --sinks 3 \
		--schema-change-at 500000 --kill-at 700000 --label resume
