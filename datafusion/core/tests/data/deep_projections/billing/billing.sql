CREATE OR REPLACE TABLE billing (
  timestamp TIMESTAMP_S, 
  _acp_system_metadata STRUCT(
    acp_sourceBatchId VARCHAR, 
    commitBatchId VARCHAR, 
    ingestTime INT8, 
    isDeleted BOOL, 
    rowId VARCHAR, 
    rowVersion INT8, 
    trackingId VARCHAR
  ), 
  _aaanortheast STRUCT(
    TravelBookingEventDetails STRUCT(
      aaa_traveltype VARCHAR, 
      aaa_membernumber VARCHAR, 
      extra VARCHAR
    )
  ), 
  _id VARCHAR, 
  _eventType VARCHAR, 

  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR
);

INSERT INTO billing VALUES(
  '2025-02-01 13:00:00', 
  {
    acp_sourceBatchId: 'batch1', 
    isDeleted: false
  }, 
  {
    TravelBookingEventDetails: {
      aaa_traveltype: '1', 
      aaa_membernumber: 'm1', 
      extra: 'extra1'
    }
  }, 
  'id1', 
  'event1', 
  '2025-01-01', 
  'batch1'
);

COPY billing TO 'billing.parquet' (FORMAT PARQUET);


