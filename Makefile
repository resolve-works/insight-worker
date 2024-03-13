
run:
	set -a; source ./.env; set +a; poetry run insight-worker process-messages

test:
	poetry run pytest

sqlacodegen:
	poetry run sqlacodegen --generator=declarative --schemas=private postgresql://insight:insight@localhost:5432/insight --outfile insight_worker/models.py
