serve:
	./venv/bin/datasette serve --immutable coronadata.db \
    --metadata=metadata.yaml \
    --host=0.0.0.0 \
    --cors \
    --setting hash_urls 1 \
    --setting sql_time_limit_ms 2000 \
    --setting allow_download off \
    --setting base_url /coronadatasette/ \
    --setting max_returned_rows 3000

update_db:
	./venv/bin/python scripts/db-update-states.py
	./venv/bin/python scripts/db-update-counties.py
	./venv/bin/python scripts/db-update-update-metadata.py

venv: scripts/requirements.txt
	[ -d ./venv ] || python3 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -Ur scripts/requirements.txt
	touch venv
