CREATE OR REPLACE TABLE table1 (
  _acp_system_metadata STRUCT(
    acp_sourceBatchId VARCHAR, 
    commitBatchId VARCHAR, 
    ingestTime INT8, 
    isDeleted BOOL, 
    rowId VARCHAR, 
    rowVersion INT8, 
    trackingId VARCHAR
  ), 
  orderData STRUCT(
    productList STRUCT(
      SKU VARCHAR, 
      quantity INT4, 
      priceTotal FLOAT
    )[], 
    date TIMESTAMP
  ), 
  list_of_struct_of_list_of_struct STRUCT(
    field1 VARCHAR, 
    field2 STRUCT(
      subfield1 VARCHAR, 
      subliststruct1 STRUCT(
        name VARCHAR, 
        price FLOAT
      )[]
    )[], 
    fieldmap MAP(
      VARCHAR, 
      STRUCT(
        prop1 VARCHAR, 
        val1 VARCHAR
      )[]
    )
  )[], 
  _id VARCHAR, 
  _ACP_BATCHID VARCHAR
);

INSERT INTO table1 VALUES(
  {
    acp_sourceBatchId : 'batch1', 
    commitBatchId : 'batch1', 
    ingestTime : 1, 
    isDeleted : false, 
    rowId : 'row1', 
    rowVersion : 1, 
    trackingId : 't1'
  }, 
  {
    productList : [
      {
        SKU : 'sku1', 
        quantity : 1, 
        priceTotal : 10.0
      }, 
      {
        SKU : 'sku2', 
        quantity : 10, 
        priceTotal : 100.0
      }
    ], 
    date: '2025-01-01 13:00:00'
  }, 
  [
    {
      field1 : 'v1', 
      field2 : [
        {
          subfield1 : 'v11', 
          subliststruct1 : [
            {name : 'n1', price : 100.0},   
            {name : 'n2', price : 100.0},   
          ]
        }, 
        {
          subfield1 : 'v12', 
          subliststruct1 : [
            {name : 'n1', price : 100.0},   
            {name : 'n2', price : 100.0},   
          ]
        }
      ], 
      field3: MAP {
        'key1': [
          { prop1: 'prop1', val1: 'val1'},   
          { prop1: 'prop2', val1: 'val2'}
        ], 
        'key2': [
          { prop1: 'prop1', val1: 'val1'},   
          { prop1: 'prop2', val1: 'val2'}
        ], 
      }
    }
  ], 
  'id1', 
  'batch1'
);

COPY table1 TO 'table.parquet' (FORMAT PARQUET);

