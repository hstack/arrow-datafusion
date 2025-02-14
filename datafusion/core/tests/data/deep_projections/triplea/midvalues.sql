CREATE OR REPLACE TABLE midvalues(
  timestamp TIMESTAMP_S,  -- 0
  web STRUCT(
    webPageDetails STRUCT(
      pageViews STRUCT(value INT8)
    )
  ), 
  endUserIDs STRUCT(
    _experience STRUCT(
      mcid STRUCT(
        id VARCHAR, 
        extra1 VARCHAR
      ),
      aaid STRUCT(
        id VARCHAR, 
        extra1 VARCHAR
      )
    )
  )
);

INSERT INTO midvalues VALUES
(
  '2025-01-15 00:00:01',
  struct_pack(
    webPageDetails := struct_pack(
      pageViews := struct_pack(value := 100)
    )
  ), 
  struct_pack(
    _experience := struct_pack(
      mcid := struct_pack(
        id := 'mcid1', 
        extra1 := 'extram1'
      ), 
      aaid := struct_pack(
        id := 'mcid1', 
        extra1 := 'extram1'
      )
    )
  )
);

COPY midvalues TO 'midvalues.parquet' (FORMAT PARQUET);

