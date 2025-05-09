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
        '2025-01-02 13:00:00', 
        {
            acp_sourceBatchId: 'b0', 
            commitBatchId: 'b0', 
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
        '2025-01-02', 
        'b0'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-03 13:00:00', 
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
        '2025-01-03', 
        'b1'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-04 13:00:00', 
        {
            acp_sourceBatchId: 'b2', 
            commitBatchId: 'b2', 
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
        '2025-01-04', 
        'b2'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-05 13:00:00', 
        {
            acp_sourceBatchId: 'b3', 
            commitBatchId: 'b3', 
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
        '2025-01-05', 
        'b3'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-06 13:00:00', 
        {
            acp_sourceBatchId: 'b4', 
            commitBatchId: 'b4', 
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
        '2025-01-06', 
        'b4'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-07 13:00:00', 
        {
            acp_sourceBatchId: 'b5', 
            commitBatchId: 'b5', 
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
        '2025-01-07', 
        'b5'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-08 13:00:00', 
        {
            acp_sourceBatchId: 'b6', 
            commitBatchId: 'b6', 
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
        '2025-01-08', 
        'b6'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-09 13:00:00', 
        {
            acp_sourceBatchId: 'b7', 
            commitBatchId: 'b7', 
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
        '2025-01-09', 
        'b7'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-10 13:00:00', 
        {
            acp_sourceBatchId: 'b8', 
            commitBatchId: 'b8', 
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
        '2025-01-10', 
        'b8'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-11 13:00:00', 
        {
            acp_sourceBatchId: 'b9', 
            commitBatchId: 'b9', 
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
        '2025-01-11', 
        'b9'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-12 13:00:00', 
        {
            acp_sourceBatchId: 'b10', 
            commitBatchId: 'b10', 
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
        '2025-01-12', 
        'b10'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-13 13:00:00', 
        {
            acp_sourceBatchId: 'b11', 
            commitBatchId: 'b11', 
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
        '2025-01-13', 
        'b11'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-14 13:00:00', 
        {
            acp_sourceBatchId: 'b12', 
            commitBatchId: 'b12', 
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
        '2025-01-14', 
        'b12'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-15 13:00:00', 
        {
            acp_sourceBatchId: 'b13', 
            commitBatchId: 'b13', 
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
        '2025-01-15', 
        'b13'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-16 13:00:00', 
        {
            acp_sourceBatchId: 'b14', 
            commitBatchId: 'b14', 
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
        '2025-01-16', 
        'b14'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-17 13:00:00', 
        {
            acp_sourceBatchId: 'b15', 
            commitBatchId: 'b15', 
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
        '2025-01-17', 
        'b15'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-18 13:00:00', 
        {
            acp_sourceBatchId: 'b16', 
            commitBatchId: 'b16', 
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
        '2025-01-18', 
        'b16'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-19 13:00:00', 
        {
            acp_sourceBatchId: 'b17', 
            commitBatchId: 'b17', 
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
        '2025-01-19', 
        'b17'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-20 13:00:00', 
        {
            acp_sourceBatchId: 'b18', 
            commitBatchId: 'b18', 
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
        '2025-01-20', 
        'b18'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-21 13:00:00', 
        {
            acp_sourceBatchId: 'b19', 
            commitBatchId: 'b19', 
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
        '2025-01-21', 
        'b19'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-22 13:00:00', 
        {
            acp_sourceBatchId: 'b20', 
            commitBatchId: 'b20', 
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
        '2025-01-22', 
        'b20'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-23 13:00:00', 
        {
            acp_sourceBatchId: 'b21', 
            commitBatchId: 'b21', 
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
        '2025-01-23', 
        'b21'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-24 13:00:00', 
        {
            acp_sourceBatchId: 'b22', 
            commitBatchId: 'b22', 
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
        '2025-01-24', 
        'b22'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-25 13:00:00', 
        {
            acp_sourceBatchId: 'b23', 
            commitBatchId: 'b23', 
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
        '2025-01-25', 
        'b23'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-26 13:00:00', 
        {
            acp_sourceBatchId: 'b24', 
            commitBatchId: 'b24', 
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
        '2025-01-26', 
        'b24'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-27 13:00:00', 
        {
            acp_sourceBatchId: 'b25', 
            commitBatchId: 'b25', 
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
        '2025-01-27', 
        'b25'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-28 13:00:00', 
        {
            acp_sourceBatchId: 'b26', 
            commitBatchId: 'b26', 
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
        '2025-01-28', 
        'b26'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-29 13:00:00', 
        {
            acp_sourceBatchId: 'b27', 
            commitBatchId: 'b27', 
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
        '2025-01-29', 
        'b27'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-30 13:00:00', 
        {
            acp_sourceBatchId: 'b28', 
            commitBatchId: 'b28', 
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
        '2025-01-30', 
        'b28'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-01-31 13:00:00', 
        {
            acp_sourceBatchId: 'b29', 
            commitBatchId: 'b29', 
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
        '2025-01-31', 
        'b29'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-01 13:00:00', 
        {
            acp_sourceBatchId: 'b30', 
            commitBatchId: 'b30', 
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
        '2025-02-01', 
        'b30'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-02 13:00:00', 
        {
            acp_sourceBatchId: 'b31', 
            commitBatchId: 'b31', 
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
        '2025-02-02', 
        'b31'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-03 13:00:00', 
        {
            acp_sourceBatchId: 'b32', 
            commitBatchId: 'b32', 
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
        '2025-02-03', 
        'b32'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-04 13:00:00', 
        {
            acp_sourceBatchId: 'b33', 
            commitBatchId: 'b33', 
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
        '2025-02-04', 
        'b33'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-05 13:00:00', 
        {
            acp_sourceBatchId: 'b34', 
            commitBatchId: 'b34', 
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
        '2025-02-05', 
        'b34'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-06 13:00:00', 
        {
            acp_sourceBatchId: 'b35', 
            commitBatchId: 'b35', 
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
        '2025-02-06', 
        'b35'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-07 13:00:00', 
        {
            acp_sourceBatchId: 'b36', 
            commitBatchId: 'b36', 
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
        '2025-02-07', 
        'b36'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-08 13:00:00', 
        {
            acp_sourceBatchId: 'b37', 
            commitBatchId: 'b37', 
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
        '2025-02-08', 
        'b37'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-09 13:00:00', 
        {
            acp_sourceBatchId: 'b38', 
            commitBatchId: 'b38', 
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
        '2025-02-09', 
        'b38'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-10 13:00:00', 
        {
            acp_sourceBatchId: 'b39', 
            commitBatchId: 'b39', 
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
        '2025-02-10', 
        'b39'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-11 13:00:00', 
        {
            acp_sourceBatchId: 'b40', 
            commitBatchId: 'b40', 
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
        '2025-02-11', 
        'b40'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-12 13:00:00', 
        {
            acp_sourceBatchId: 'b41', 
            commitBatchId: 'b41', 
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
        '2025-02-12', 
        'b41'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-13 13:00:00', 
        {
            acp_sourceBatchId: 'b42', 
            commitBatchId: 'b42', 
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
        '2025-02-13', 
        'b42'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-14 13:00:00', 
        {
            acp_sourceBatchId: 'b43', 
            commitBatchId: 'b43', 
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
        '2025-02-14', 
        'b43'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-15 13:00:00', 
        {
            acp_sourceBatchId: 'b44', 
            commitBatchId: 'b44', 
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
        '2025-02-15', 
        'b44'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-16 13:00:00', 
        {
            acp_sourceBatchId: 'b45', 
            commitBatchId: 'b45', 
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
        '2025-02-16', 
        'b45'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-17 13:00:00', 
        {
            acp_sourceBatchId: 'b46', 
            commitBatchId: 'b46', 
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
        '2025-02-17', 
        'b46'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-18 13:00:00', 
        {
            acp_sourceBatchId: 'b47', 
            commitBatchId: 'b47', 
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
        '2025-02-18', 
        'b47'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-19 13:00:00', 
        {
            acp_sourceBatchId: 'b48', 
            commitBatchId: 'b48', 
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
        '2025-02-19', 
        'b48'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-20 13:00:00', 
        {
            acp_sourceBatchId: 'b49', 
            commitBatchId: 'b49', 
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
        '2025-02-20', 
        'b49'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-21 13:00:00', 
        {
            acp_sourceBatchId: 'b50', 
            commitBatchId: 'b50', 
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
        '2025-02-21', 
        'b50'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-22 13:00:00', 
        {
            acp_sourceBatchId: 'b51', 
            commitBatchId: 'b51', 
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
        '2025-02-22', 
        'b51'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-23 13:00:00', 
        {
            acp_sourceBatchId: 'b52', 
            commitBatchId: 'b52', 
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
        '2025-02-23', 
        'b52'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-24 13:00:00', 
        {
            acp_sourceBatchId: 'b53', 
            commitBatchId: 'b53', 
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
        '2025-02-24', 
        'b53'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-25 13:00:00', 
        {
            acp_sourceBatchId: 'b54', 
            commitBatchId: 'b54', 
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
        '2025-02-25', 
        'b54'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-26 13:00:00', 
        {
            acp_sourceBatchId: 'b55', 
            commitBatchId: 'b55', 
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
        '2025-02-26', 
        'b55'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-27 13:00:00', 
        {
            acp_sourceBatchId: 'b56', 
            commitBatchId: 'b56', 
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
        '2025-02-27', 
        'b56'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-02-28 13:00:00', 
        {
            acp_sourceBatchId: 'b57', 
            commitBatchId: 'b57', 
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
        '2025-02-28', 
        'b57'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-01 13:00:00', 
        {
            acp_sourceBatchId: 'b58', 
            commitBatchId: 'b58', 
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
        '2025-03-01', 
        'b58'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-02 13:00:00', 
        {
            acp_sourceBatchId: 'b59', 
            commitBatchId: 'b59', 
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
        '2025-03-02', 
        'b59'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-03 13:00:00', 
        {
            acp_sourceBatchId: 'b60', 
            commitBatchId: 'b60', 
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
        '2025-03-03', 
        'b60'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-04 13:00:00', 
        {
            acp_sourceBatchId: 'b61', 
            commitBatchId: 'b61', 
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
        '2025-03-04', 
        'b61'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-05 13:00:00', 
        {
            acp_sourceBatchId: 'b62', 
            commitBatchId: 'b62', 
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
        '2025-03-05', 
        'b62'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-06 13:00:00', 
        {
            acp_sourceBatchId: 'b63', 
            commitBatchId: 'b63', 
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
        '2025-03-06', 
        'b63'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-07 13:00:00', 
        {
            acp_sourceBatchId: 'b64', 
            commitBatchId: 'b64', 
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
        '2025-03-07', 
        'b64'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-08 13:00:00', 
        {
            acp_sourceBatchId: 'b65', 
            commitBatchId: 'b65', 
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
        '2025-03-08', 
        'b65'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-09 13:00:00', 
        {
            acp_sourceBatchId: 'b66', 
            commitBatchId: 'b66', 
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
        '2025-03-09', 
        'b66'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-10 13:00:00', 
        {
            acp_sourceBatchId: 'b67', 
            commitBatchId: 'b67', 
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
        '2025-03-10', 
        'b67'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-11 13:00:00', 
        {
            acp_sourceBatchId: 'b68', 
            commitBatchId: 'b68', 
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
        '2025-03-11', 
        'b68'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-12 13:00:00', 
        {
            acp_sourceBatchId: 'b69', 
            commitBatchId: 'b69', 
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
        '2025-03-12', 
        'b69'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-13 13:00:00', 
        {
            acp_sourceBatchId: 'b70', 
            commitBatchId: 'b70', 
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
        '2025-03-13', 
        'b70'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-14 13:00:00', 
        {
            acp_sourceBatchId: 'b71', 
            commitBatchId: 'b71', 
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
        '2025-03-14', 
        'b71'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-15 13:00:00', 
        {
            acp_sourceBatchId: 'b72', 
            commitBatchId: 'b72', 
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
        '2025-03-15', 
        'b72'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-16 13:00:00', 
        {
            acp_sourceBatchId: 'b73', 
            commitBatchId: 'b73', 
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
        '2025-03-16', 
        'b73'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-17 13:00:00', 
        {
            acp_sourceBatchId: 'b74', 
            commitBatchId: 'b74', 
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
        '2025-03-17', 
        'b74'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-18 13:00:00', 
        {
            acp_sourceBatchId: 'b75', 
            commitBatchId: 'b75', 
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
        '2025-03-18', 
        'b75'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-19 13:00:00', 
        {
            acp_sourceBatchId: 'b76', 
            commitBatchId: 'b76', 
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
        '2025-03-19', 
        'b76'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-20 13:00:00', 
        {
            acp_sourceBatchId: 'b77', 
            commitBatchId: 'b77', 
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
        '2025-03-20', 
        'b77'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-21 13:00:00', 
        {
            acp_sourceBatchId: 'b78', 
            commitBatchId: 'b78', 
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
        '2025-03-21', 
        'b78'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-22 13:00:00', 
        {
            acp_sourceBatchId: 'b79', 
            commitBatchId: 'b79', 
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
        '2025-03-22', 
        'b79'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-23 13:00:00', 
        {
            acp_sourceBatchId: 'b80', 
            commitBatchId: 'b80', 
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
        '2025-03-23', 
        'b80'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-24 13:00:00', 
        {
            acp_sourceBatchId: 'b81', 
            commitBatchId: 'b81', 
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
        '2025-03-24', 
        'b81'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-25 13:00:00', 
        {
            acp_sourceBatchId: 'b82', 
            commitBatchId: 'b82', 
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
        '2025-03-25', 
        'b82'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-26 13:00:00', 
        {
            acp_sourceBatchId: 'b83', 
            commitBatchId: 'b83', 
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
        '2025-03-26', 
        'b83'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-27 13:00:00', 
        {
            acp_sourceBatchId: 'b84', 
            commitBatchId: 'b84', 
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
        '2025-03-27', 
        'b84'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-28 13:00:00', 
        {
            acp_sourceBatchId: 'b85', 
            commitBatchId: 'b85', 
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
        '2025-03-28', 
        'b85'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-29 13:00:00', 
        {
            acp_sourceBatchId: 'b86', 
            commitBatchId: 'b86', 
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
        '2025-03-29', 
        'b86'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-30 13:00:00', 
        {
            acp_sourceBatchId: 'b87', 
            commitBatchId: 'b87', 
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
        '2025-03-30', 
        'b87'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-03-31 13:00:00', 
        {
            acp_sourceBatchId: 'b88', 
            commitBatchId: 'b88', 
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
        '2025-03-31', 
        'b88'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-01 13:00:00', 
        {
            acp_sourceBatchId: 'b89', 
            commitBatchId: 'b89', 
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
        '2025-04-01', 
        'b89'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-02 13:00:00', 
        {
            acp_sourceBatchId: 'b90', 
            commitBatchId: 'b90', 
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
        '2025-04-02', 
        'b90'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-03 13:00:00', 
        {
            acp_sourceBatchId: 'b91', 
            commitBatchId: 'b91', 
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
        '2025-04-03', 
        'b91'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-04 13:00:00', 
        {
            acp_sourceBatchId: 'b92', 
            commitBatchId: 'b92', 
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
        '2025-04-04', 
        'b92'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-05 13:00:00', 
        {
            acp_sourceBatchId: 'b93', 
            commitBatchId: 'b93', 
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
        '2025-04-05', 
        'b93'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-06 13:00:00', 
        {
            acp_sourceBatchId: 'b94', 
            commitBatchId: 'b94', 
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
        '2025-04-06', 
        'b94'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-07 13:00:00', 
        {
            acp_sourceBatchId: 'b95', 
            commitBatchId: 'b95', 
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
        '2025-04-07', 
        'b95'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-08 13:00:00', 
        {
            acp_sourceBatchId: 'b96', 
            commitBatchId: 'b96', 
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
        '2025-04-08', 
        'b96'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-09 13:00:00', 
        {
            acp_sourceBatchId: 'b97', 
            commitBatchId: 'b97', 
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
        '2025-04-09', 
        'b97'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-10 13:00:00', 
        {
            acp_sourceBatchId: 'b98', 
            commitBatchId: 'b98', 
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
        '2025-04-10', 
        'b98'
        );

INSERT INTO gs_summary_metrics VALUES(
        '2025-04-11 13:00:00', 
        {
            acp_sourceBatchId: 'b99', 
            commitBatchId: 'b99', 
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
        '2025-04-11', 
        'b99'
        );
COPY gs_summary_metrics TO 'gs_summary_metrics.parquet' (FORMAT PARQUET);

