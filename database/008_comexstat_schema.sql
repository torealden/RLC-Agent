requirements-comexstat.txt
CREATE TABLE IF NOT EXISTS bronze_comexstat_raw (
  id               BIGSERIAL PRIMARY KEY,
  extracted_at_utc TIMESTAMP NOT NULL,
  source           TEXT NOT NULL DEFAULT 'COMEXSTAT',
  endpoint         TEXT NOT NULL,
  params_json      TEXT NOT NULL,
  row_json         TEXT NOT NULL
);
requirements-comexstat.txt
