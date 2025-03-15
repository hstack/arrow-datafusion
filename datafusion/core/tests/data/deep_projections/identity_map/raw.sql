CREATE OR REPLACE TABLE raw (
  timestamp TIMESTAMP_S,  -- 0
  web STRUCT(
    webPageDetails STRUCT(
      name VARCHAR, 
      pageViews STRUCT(value INT8)
    )
  ), 
  identityMap MAP(
    VARCHAR, 
    STRUCT(
      id VARCHAR, 
      prim BOOLEAN
    )[]
  )
);


INSERT INTO raw VALUES
(
  '2025-03-01 00:00:01',
  {
    webPageDetails: {
      name: 'page1', 
      pageViews: {
        value: 100
      }
    }
  }, 
  MAP {
    'ECID': [
      {
        id: 1, 
        prim: true
      }, 
      {
        id: 2, 
        prim: false
      }, 
    ]
  }
);

COPY raw TO 'raw.parquet' (FORMAT PARQUET);


