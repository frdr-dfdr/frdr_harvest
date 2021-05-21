alter table affiliations add column affiliation_ror;

CREATE TABLE IF NOT EXISTS "ror_affiliation_matches" (
	ror_affiliation_match_id INTEGER PRIMARY KEY NOT NULL,
	affiliation_string TEXT,
  ror_id TEXT,
  score NUMERIC,
	country TEXT,
  updated_timestamp INTEGER DEFAULT 0
);
