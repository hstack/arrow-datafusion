import datetime

date = datetime.datetime(2025, 1, 1)
for i in range(0, 100):
    date = date + datetime.timedelta(days=1)
    print()
    date_fmt = date.strftime("%Y-%m-%d")
    sql = f"""
        INSERT INTO gs_summary_metrics VALUES(
        '{date_fmt} 13:00:00', 
        {{
            acp_sourceBatchId: 'b{i}', 
            commitBatchId: 'b{i}', 
            ingestTime: 1, 
            isDeleted: false, 
            rowId: '1', 
            rowVersion: 1, 
            trackingId: 't1'
        }}, 
        {{
            genStudioInsights: {{
                entityIDs: {{
                    account: {{
                        accountID: 'a1', 
                        accountGUID: 'Meta_2974530739347344', 
                    }}
                }}, 
                breakdownType: 'AccountCampaignAdGroupAd'
            }}
        }}, 
        '{date_fmt}', 
        'b{i}'
        );
    """
    print(sql.strip())
