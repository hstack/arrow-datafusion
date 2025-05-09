CREATE OR REPLACE TABLE gs_summary_metrics(
  timestamp TIMESTAMP,  -- 0
  _acp_system_metadata STRUCT( -- 1
    acp_sourceBatchId VARCHAR,  -- 2
    commitBatchId VARCHAR,  --3
    ingestTime INT8, -- 4
    isDeleted BOOL,  -- 5
    rowId VARCHAR, 
    rowVersion INT8, 
    trackingId VARCHAR
  ), 
  _wfadoberm STRUCT(
    genStudioInsights STRUCT(
      entityIDs STRUCT(
        account STRUCT(
          accountID VARCHAR, 
          accountGUID VARCHAR
        )
      ), 
      breakdownType VARCHAR
    ), 
    extra VARCHAR
  ), 
  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR
);

INSERT INTO gs_summary_metrics VALUES(
  '2025-01-01 13:00:00', 
  {
    acp_sourceBatchId: 'b1', 
    commitBatchId: 'b1', 
    ingestTime: 1, 
    isDeleted: false, 
    rowId: '1', 
    rowVersion: 1, 
    trackingId: 't1'
  }, 
  {
    genStudioInsights: {
      entityIDs: {
        account: {
          accountID: 'a1', 
          accountGUID: 'Meta_2974530739347344', 
        }
      }, 
      breakdownType: 'AccountCampaignAdGroupAd'
    }
  }, 
  '2025-01-01', 
  'batch1'
);
COPY gs_summary_metrics TO 'gs_summary_metrics.parquet' (FORMAT PARQUET);

