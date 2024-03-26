
ingest:
	set -a; source ./.env; set +a; QUEUE=ingest poetry run insight-worker process-messages

worker:
	set -a; source ./.env; set +a; QUEUE=default poetry run insight-worker process-messages

test:
	poetry run pytest

sqlacodegen:
	poetry run sqlacodegen --generator=declarative --schemas=private postgresql://insight:insight@localhost:5432/insight --outfile insight_worker/models.py
