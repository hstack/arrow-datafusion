CREATE OR REPLACE TABLE midvalues (
  -- _acp_system_metadata STRUCT(
  --   acp_sourceBatchId VARCHAR, 
  --   commitBatchId VARCHAR, 
  --   ingestTime INT8, 
  --   isDeleted BOOL, 
  --   rowId VARCHAR, 
  --   rowVersion INT8, 
  --   trackingId VARCHAR
  -- ), 
  -- channel STRUCT(
  --   _id VARCHAR, 
  --   typeAtSource VARCHAR
  -- ), 
  timestamp TIMESTAMP_S, 
  _id VARCHAR, 
  productListItems STRUCT(
    SKU VARCHAR, 
    quantity INT4, 
    priceTotal FLOAT, 
    _experience STRUCT(
      event1 VARCHAR, 
      event2 VARCHAR
    )
  )[], 
  -- commerce STRUCT(
  --   order STRUCT(
  --     purchaseID VARCHAR, 
  --     currencyCode VARCHAR
  --   ), 
  --   purchases STRUCT(value FLOAT), 
  --   productViews STRUCT(value FLOAT)
  -- ), 
  -- dataSource STRUCT(
  --   _id VARCHAR
  -- ), 
  -- device STRUCT(
  --   typeIDService VARCHAR
  -- ), 
  -- search STRUCT(
  --   searchEngine VARCHAR,
  --   keywords VARCHAR
  -- ), 
  -- receivedTimestamp TIMESTAMP_S, 
  -- endUserIDs STRUCT(
  --   _experience STRUCT(
  --     aaid STRUCT(
  --       id VARCHAR,
  --       namespace VARCHAR
  --     ), 
  --     aaid STRUCT(
  --       id VARCHAR,
  --       namespace VARCHAR
  --     )
  --   )
  -- ), 
  -- identityMap MAP(
  --   VARCHAR, 
  --   STRUCT(
  --     id VARCHAR, 
  --     primary BOOLEAN
  --   )[]
  -- ), 
  web STRUCT(
    webPageDetails STRUCT(
      name VARCHAR, 
      pageViews STRUCT(value INT8)
    )
  ), 
  -- placeContext
  -- marketing
  -- userActivityRegion
  -- environment
  _experience STRUCT(
    analytics STRUCT(
      customDimensions STRUCT(
        eVars STRUCT(
          eVar40 VARCHAR, 
          eVar1 VARCHAR
        )
      ), 
      environment STRUCT(
        browserID INT4
      )
    )
    -- target
    -- decisioning
  ), 
  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR
);

INSERT INTO midvalues VALUES(
  '2025-01-01 13:00:00', 
  '1', 
  [
    {
      SKU : 'sku1', 
      quantity : 1, 
      priceTotal : 10.0, 
      _experience: {
        event1: 'event11', 
        event2: 'event12'
      }
    }, 
    {
      SKU : 'sku2', 
      quantity : 10, 
      priceTotal : 100.0, 
      _experience: {
        event1: 'event21', 
        event2: 'event22'
      }
    }
  ], 
  {
    webPageDetails: {
      name: 'page1', 
      pageViews: {value: 100}
    }
  }, 
  {
    analytics: {
      customDimensions: {
        eVars: {
          eVar40: 'entity1', 
          eVar1: 'xxx1'
        }
      }, 
      environment: {
        browserID: 1
      }
    }
  }, 
  '2025-01-01', 
  'batch1'
);

COPY midvalues TO 'midvalues.parquet' (FORMAT PARQUET);

