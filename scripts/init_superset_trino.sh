#!/usr/bin/env bash
# Đăng ký Trino connection trong Superset (chạy sau make up).
set -euo pipefail

echo "Registering Trino database in Superset..."

docker exec superset python - <<'PY'
import json
from superset import app, db
from superset.models.core import Database

uri = "trino://trino@trino:8080/hive"
name = "Trino Hive"

with app.app_context():
    existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if existing:
        existing.sqlalchemy_uri = uri
        db.session.commit()
        print(f"Updated existing database: {name}")
    else:
        database = Database(database_name=name, sqlalchemy_uri=uri, expose_in_sqllab=True)
        db.session.add(database)
        db.session.commit()
        print(f"Created database: {name}")
PY

echo "Done. Open Superset → SQL Lab → Trino Hive"
