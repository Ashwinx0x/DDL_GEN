CREATE OR REPLACE PROCEDURE `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_DDL_CREATE_SIMPLE`()
BEGIN

  DECLARE new_ddl STRING;
  DECLARE join_cond STRING;
  DECLARE final_select STRING;
  DECLARE col_name STRING;
  DECLARE col_alias STRING;
  DECLARE col_list ARRAY<STRING>;

  -- Metadata values from each view row
  DECLARE view_dataset STRING;
  DECLARE view_name STRING;

  -- Declare metadata DK fields
  DECLARE pii_column_name STRING;
  DECLARE xref_table_name STRING;
  DECLARE xref_dk_column_name STRING;
  DECLARE xref_join_column_name STRING;
  DECLARE dk_column_alias STRING;

  -- Error logging
  DECLARE err_msg STRING;

  -- Loop through each metadata entry
  FOR view_row IN (
    SELECT DISTINCT 
      PII_VIEW_DATASET_NAME,PII_VIEW_NAME,PII_COLUMN_NAME,DK_COLUMN_ALIAS,XREF_TABLE_NAME,XREF_DK_COLUMN_NAME,XREF_JOIN_COLUMN_NAME
      FROM `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_MTDATA_DDL_GEN_T`
  ) DO

    -- Initialize values for this loop
    SET view_dataset = view_row.PII_VIEW_DATASET_NAME;
    SET view_name = view_row.PII_VIEW_NAME;
    SET new_ddl = '';
    SET join_cond = '';
    SET final_select = '';

    -- Get column list from INFORMATION_SCHEMA for current view
    EXECUTE IMMEDIATE (
      'SELECT ARRAY_AGG(column_name ORDER BY ordinal_position) FROM `cs-cdwp-data-pp2182.' || view_dataset || '`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = @view_name'
    )
    INTO col_list
    USING view_name;

    -- Loop through each column in the view
    FOR col_record IN (
      SELECT col, idx
      FROM UNNEST(col_list) AS col WITH OFFSET AS idx
    ) DO
      SET col_name = col_record.col;

      -- Try to get metadata match for this column
      BEGIN
        SELECT 
          PII_COLUMN_NAME,
          XREF_TABLE_NAME,
          XREF_DK_COLUMN_NAME,
          XREF_JOIN_COLUMN_NAME,
          DK_COLUMN_ALIAS
        INTO
          pii_column_name,
          xref_table_name,
          xref_dk_column_name,
          xref_join_column_name,
          dk_column_alias
        FROM `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_MTDATA_DDL_GEN_T`
        WHERE 
          PII_VIEW_DATASET_NAME = view_dataset
          AND PII_VIEW_NAME = view_name
          AND PII_COLUMN_NAME = col_name
        LIMIT 1;

        -- If metadata found, add original and DK column
        SET final_select = final_select || 't.' || col_name || ', ';

        -- DK column with alias
        IF dk_column_alias IS NOT NULL THEN
          SET col_alias = ' AS ' || dk_column_alias;
        ELSE
          SET col_alias = ' AS ' || xref_dk_column_name;
        END IF;

        SET final_select = final_select || 'COALESCE(CAST(xr_' || col_name || '.' || xref_dk_column_name || ' AS STRING), "-1")' || col_alias || ', ';

        -- Add JOIN condition
        SET join_cond = join_cond || ' LEFT JOIN `cs-cdwp-data-pp2182.CDW_XREF_DATA.' || xref_table_name || '` xr_' || col_name 
          || ' ON t.' || col_name || ' = xr_' || col_name || '.' || xref_join_column_name;

      EXCEPTION WHEN ERROR THEN
        -- Column not in metadata, add normally
        SET final_select = final_select || 't.' || col_name || ', ';
      END;

    END FOR;

    -- Remove trailing comma and space
    SET final_select = RTRIM(final_select, ', ');

    -- Assemble final DDL
    SET new_ddl = 'CREATE OR REPLACE VIEW `cs-cdwp-data-pp2182.' || view_dataset || '.' || view_name || '` AS SELECT '
      || final_select
      || ' FROM `cs-cdwp-data-pp2182.' || view_dataset || '.' || view_name || '` t'
      || join_cond
      || ';';

    -- Execute the DDL
    BEGIN
      EXECUTE IMMEDIATE new_ddl;
    EXCEPTION WHEN ERROR THEN
      SET err_msg = @@error.message;
      INSERT INTO `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_MTDATA_DDL_GEN_ERR_T`
      (VIEW_DATASET,VIEW_NAME, DDL_SQL, ERROR_MESSAGE, LOG_TS)
      VALUES (view_dataset,view_name, new_ddl, err_msg, CURRENT_TIMESTAMP());
    END;

    -- Update metadata table with DDL_GENERATED_TS
    UPDATE `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_MTDATA_DDL_GEN_T`
    SET DDL_GENERATED_TS = CURRENT_TIMESTAMP()
    WHERE 
      PII_VIEW_DATASET_NAME = view_dataset
      AND PII_VIEW_NAME = view_name;

  END FOR;

END;
