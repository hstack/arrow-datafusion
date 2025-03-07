CREATE OR REPLACE TABLE cross_industry_demo_data (
	_ACP_DATE DATE,
	timestamp TIMESTAMP_S, 
  endUserIDs STRUCT(
    aaid_id VARCHAR, 
    extra INT4
  ), 
  _experience STRUCT(
    eVar56 VARCHAR, 
    extra VARCHAR
  )
);

INSERT INTO cross_industry_demo_data VALUES
(
  '2025-01-03',
  '2025-01-03 02:00:30', 
  ROW(
    'd1', 
    1,
  ),
  ROW(
    'u1',
    'extra1',
  ),
);
INSERT INTO cross_industry_demo_data VALUES
(
  '2025-01-03',
  '2025-01-03 02:00:30', 
  ROW(
    'd1', 
    1,
  ),
  ROW(
    'u2',
    'extra1',
  ),
);
INSERT INTO cross_industry_demo_data VALUES
(
  '2025-01-03',
  '2025-01-03 02:00:30', 
  ROW(
    'd2', 
    1,
  ),
  ROW(
    'u1',
    'extra1',
  ),
);
INSERT INTO cross_industry_demo_data VALUES
(
  '2025-01-03',
  '2025-01-03 02:00:30', 
  ROW(
    'd2', 
    1,
  ),
  ROW(
    'u2',
    'extra1',
  ),
);
COPY cross_industry_demo_data TO 'output.parquet' (FORMAT PARQUET);
