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

CREATE OR REPLACE TABLE double_map (
  web STRUCT(
    webPageDetails STRUCT(
      name VARCHAR, 
      pageViews STRUCT(value INT8)
    )
  ), 
  map1 MAP(
    VARCHAR, 
    STRUCT(
      submap1 MAP(
        VARCHAR, 
        STRUCT(
          field1 VARCHAR, 
          extra1 VARCHAR
        )[]
      ), 
      extra2 VARCHAR
    )[]
  )
);


INSERT INTO double_map VALUES
(
  {
    webPageDetails: {
      name: 'page1', 
      pageViews: {
        value: 100
      }
    }
  }, 
  MAP {
    'key1': [
      {
        submap1: MAP {
          'subkey1': [
            {
              field1: 'field11', 
              extra1: 'extra11'
            }, 
            {
              field1: 'field12', 
              extra1: 'extra12'
            }
          ]
        }, 
        extra2: 'extra21'
      }, 
      {
        submap1: MAP {
          'subkey2': [
            {
              field1: 'field21', 
              extra1: 'extra21'
            }, 
            {
              field1: 'field22', 
              extra1: 'extra22'
            }
          ]
        }, 
        extra2: 'extra22'
      }
    ]
  }
);

COPY double_map TO 'double_map.parquet' (FORMAT PARQUET);


