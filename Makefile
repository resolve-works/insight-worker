
rebuild_index:
	set -a; source ./.env; set +a; poetry run insight-worker rebuild-index

delete_index:
	set -a; source ./.env; set +a; poetry run insight-worker delete-index

ingest:
	set -a; source ./.env; set +a; QUEUE=ingest poetry run watchmedo auto-restart -p "*.py" -R insight-worker process-messages

worker:
	set -a; source ./.env; set +a; QUEUE=default poetry run watchmedo auto-restart -p "*.py" -R insight-worker process-messages

test:
	poetry run pytest

sqlacodegen:
	poetry run sqlacodegen --generator=declarative --schemas=private postgresql://insight:insight@localhost:5432/insight --outfile insight_worker/models.py
