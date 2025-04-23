
rebuild_index:
	set -a; source ./.env; set +a; uv run insight-worker rebuild-index

delete_index:
	set -a; source ./.env; set +a; uv run insight-worker delete-index

ingest:
	set -a; source ./.env; set +a; QUEUE=ingest uv run watchmedo auto-restart -p "*.py" -R insight-worker process-messages

worker:
	set -a; source ./.env; set +a; QUEUE=default uv run watchmedo auto-restart -p "*.py" -R insight-worker process-messages

sqlacodegen:
	uv run sqlacodegen --generator=declarative --schemas=private postgresql://insight:insight@localhost:5432/insight --outfile insight_worker/models.py
