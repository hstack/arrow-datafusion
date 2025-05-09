CREATE OR REPLACE TABLE meta_asset_summary_metrics(
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
  _aresstagevalidationco STRUCT(
    genStudioInsights STRUCT(
      accountID VARCHAR, 
      adGroupID VARCHAR, 
      adID VARCHAR, 
      assetID VARCHAR, 
      campaignID VARCHAR, 
      metrics STRUCT(
        engagement STRUCT(
          addsToCart STRUCT(value INT8), 
          addsToWishList STRUCT(value INT8), 
          page STRUCT(
            engagement STRUCT(value INT8), 
            likes STRUCT(value INT8)
          ), 
          photoViews STRUCT(value INT8), 
          post STRUCT(
            comments STRUCT(value INT8), 
            engagement STRUCT(value INT8), 
            reactions STRUCT(value INT8), 
            saves STRUCT(value INT8), 
            shares STRUCT(value INT8)
          ) 
        ), 
        performance STRUCT(
          clicks STRUCT(value INT8), 
          conversionCount STRUCT(value INT8), 
          conversionValue STRUCT(value INT8), 
          impressions STRUCT(value INT8)
        ), 
        spend STRUCT(value INT8)
      )
    ), 
    network VARCHAR
  ), 
  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR
);
COPY meta_asset_summary_metrics TO 'meta_asset_summary_metrics.parquet' (FORMAT PARQUET);

CREATE OR REPLACE TABLE meta_asset_summary_metrics_by_age_and_gender(
  timestamp TIMESTAMP, 
  _acp_system_metadata STRUCT(
    acp_sourceBatchId VARCHAR, 
    commitBatchId VARCHAR, 
    ingestTime INT8, 
    isDeleted BOOL, 
    rowId VARCHAR, 
    rowVersion INT8, 
    trackingId VARCHAR
  ), 
  _aresstagevalidationco STRUCT(
    genStudioInsights STRUCT(
      accountID VARCHAR, 
      adGroupID VARCHAR, 
      adID VARCHAR, 
      age VARCHAR, 
      assetID VARCHAR, 
      campaignID VARCHAR, 
      gender VARCHAR, 
      metrics STRUCT(
        engagement STRUCT(
          addsToCart STRUCT(value INT8), 
          addsToWishList STRUCT(value INT8), 
          page STRUCT(
            engagement STRUCT(value INT8), 
            likes STRUCT(value INT8)
          ), 
          photoViews STRUCT(value INT8), 
          post STRUCT(
            comments STRUCT(value INT8), 
            engagement STRUCT(value INT8), 
            reactions STRUCT(value INT8), 
            saves STRUCT(value INT8), 
            shares STRUCT(value INT8)
          ) 
        ), 
        performance STRUCT(
          clicks STRUCT(value INT8), 
          conversionCount STRUCT(value INT8), 
          conversionValue STRUCT(value INT8), 
          impressions STRUCT(value INT8)
        ), 
        spend STRUCT(value INT8)
      )
    ), 
    network VARCHAR
  ), 
  _ACP_DATE DATE, 
  _ACP_BATCHID VARCHAR
);
COPY meta_asset_summary_metrics_by_age_and_gender TO 'meta_asset_summary_metrics_by_age_and_gender.parquet' (FORMAT PARQUET);

CREATE OR REPLACE TABLE meta_asset_featurization (
  _acp_system_metadata STRUCT(
    acp_sourceBatchId VARCHAR, 
    commitBatchId VARCHAR, 
    ingestTime INT8, 
    isDeleted BOOL, 
    rowId VARCHAR, 
    rowVersion INT8, 
    trackingId VARCHAR
  ), 
  _aresstagevalidationco STRUCT(
    contentAssets STRUCT(
      assetID VARCHAR, 
      assetPerceptionID VARCHAR, 
      assetThumbnailURL VARCHAR, 
      assetType VARCHAR, 
      version TIMESTAMP
    ), 
    contentFeaturization STRUCT(
      audioGenre VARCHAR, 
      audioGenreCategory VARCHAR, 
      audioMood VARCHAR, 
      audioTypes VARCHAR[], 
      categories VARCHAR[], 
      objects VARCHAR[], 
      orientation VARCHAR, 
      peopleCategories VARCHAR[], 
      version VARCHAR
    )
  ), 
  _id VARCHAR, 
  _ACP_BATCHID VARCHAR
);

COPY meta_asset_featurization TO 'meta_asset_featurization.parquet' (FORMAT PARQUET);

