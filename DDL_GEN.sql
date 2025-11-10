CREATE OR REPLACE PROCEDURE `IDW_ETL_DATA.PII_VIEW_DDL_CREATE_FINAL`()
BEGIN
  -- Declare variables
  DECLARE curr_ddl STRING;
  DECLARE new_ddl STRING;
  DECLARE join_cond STRING;
  DECLARE pii_view_name STRING;
  DECLARE pii_dataset_name STRING;
  DECLARE view_select_part STRING;
  DECLARE counter INT64;
  DECLARE error_message STRING;

  -- Loop through list of Views to process
  FOR pii_view IN (
    SELECT DISTINCT PII_VIEW_DATASET_NAME, PII_VIEW_NAME
    FROM `IDW_ETL_DATA.PII_VIEW_METADATA_T`
    WHERE DDL_GENERATED_TS IS NULL
  ) DO
  
    -- Initialize
    SET counter = 1;
    SET join_cond = '';

    SET pii_view_name = pii_view.PII_VIEW_NAME;
    SET pii_dataset_name = pii_view.PII_VIEW_DATASET_NAME;

    -- Fetch current DDL
    EXECUTE IMMEDIATE (
      'SELECT CONCAT(view_definition) 
       FROM `' || pii_dataset_name || '.INFORMATION_SCHEMA.VIEWS`
       WHERE table_name = "' || pii_view_name || '"'
    ) INTO curr_ddl;

    -- Start building new DDL
    SET new_ddl = '/* View Auto-generated as part of PII Remediation on ' || CAST(CURRENT_TIMESTAMP() AS STRING) || ' */\n';
    SET new_ddl = new_ddl || 'CREATE OR REPLACE VIEW `' || pii_dataset_name || '.' || pii_view_name || '` AS ( \nSELECT \n';

    -- Assume that view_definition starts after SELECT and before FROM
    -- Extract SELECT columns part (simple split logic)
    SET view_select_part = SUBSTR(curr_ddl, STRPOS(curr_ddl, 'SELECT') + 6, STRPOS(curr_ddl, 'FROM') - STRPOS(curr_ddl, 'SELECT') - 6);

    -- Loop through PII columns to modify
    FOR pii_col IN (
      SELECT PII_COLUMN_NAME, XREF_TABLE_NAME, XREF_JOIN_COLUMN_NAME, XREF_DK_COLUMN_NAME, DK_COLUMN_ALIAS,BASE_TBL_DB,BASE_TBL_NM
      FROM `IDW_ETL_DATA.PII_VIEW_METADATA_T`
      WHERE PII_VIEW_DATASET_NAME = pii_dataset_name
        AND PII_VIEW_NAME = pii_view_name
    ) DO

      -- Search for t.PII_COLUMN_NAME in view_select_part
      IF REGEXP_CONTAINS(view_select_part, r'\bt\.' || pii_col.PII_COLUMN_NAME || r'\b') THEN

        -- Insert DK column after the original column
        SET view_select_part = REGEXP_REPLACE(
          view_select_part,
          r'(\bt\.' || pii_col.PII_COLUMN_NAME || r'\b)',
          r'\1, \nCASE WHEN t.' || pii_col.PII_COLUMN_NAME || ' IS NULL THEN NULL ELSE COALESCE(CAST(xr' || CAST(counter AS STRING) || '.' || pii_col.XREF_DK_COLUMN_NAME || ' AS STRING), "-1") END AS ' || IFNULL(pii_col.DK_COLUMN_ALIAS, pii_col.XREF_DK_COLUMN_NAME)
        );

        -- Build LEFT JOIN
        SET join_cond = join_cond || 'LEFT JOIN `CDW_XREF_DATA.' || pii_col.XREF_TABLE_NAME || '` xr' || CAST(counter AS STRING) || ' \nON t.' || pii_col.PII_COLUMN_NAME || ' = xr' || CAST(counter AS STRING) || '.' || pii_col.XREF_JOIN_COLUMN_NAME || '\n';

        -- Increment counter
        SET counter = counter + 1;

      END IF;

    END FOR;

    -- Final Assembly of DDL
    SET new_ddl = new_ddl || view_select_part;
    SET new_ddl = new_ddl || '\nFROM `' || BASE_TBL_DB || '.' || BASE_TBL_NM || '` t \n';
    SET new_ddl = new_ddl || join_cond;
    SET new_ddl = new_ddl || ');';

    -- Execute the new DDL
    BEGIN
      EXECUTE IMMEDIATE new_ddl;
    EXCEPTION WHEN ERROR THEN
      -- In case of errors, capture
      SET error_message = @@error.message;
      INSERT INTO `IDW_ETL_DATA.PII_VIEW_METADATA_ERROR_T`
      (VIEW_NAME, DDL_SQL, ERROR_MESSAGE, ERROR_TIMESTAMP)
      VALUES (pii_view_name, new_ddl, error_message, CURRENT_TIMESTAMP());
    END;

    -- Update metadata table
    UPDATE `IDW_ETL_DATA.PII_VIEW_METADATA_T`
    SET DDL_GENERATED_TS = CURRENT_TIMESTAMP()
    WHERE PII_VIEW_DATASET_NAME = pii_dataset_name
      AND PII_VIEW_NAME = pii_view_name;

  END FOR;

END;
