CREATE OR REPLACE PROCEDURE `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_DDL_CREATE_FINAL`()
BEGIN
  -- Declare variables
  DECLARE curr_ddl STRING;
  DECLARE new_ddl STRING;
  DECLARE select_part STRING;
  DECLARE from_part STRING;
  DECLARE join_part STRING DEFAULT '';
  DECLARE where_part STRING DEFAULT '';
  DECLARE final_ddl STRING;
  DECLARE error_message STRING;
  DECLARE pii_view_name STRING;
  DECLARE pii_dataset_name STRING;
  DECLARE base_tbl STRING;
  DECLARE counter INT64 DEFAULT 1;

  -- Cursor to loop through views
  FOR view_rec IN (
    SELECT DISTINCT PII_VIEW_DATASET_NAME, PII_VIEW_NAME
    FROM `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_METADATA_T`
    WHERE DDL_GENERATED_TS IS NULL
  ) DO

    -- Initialize
    SET pii_view_name = view_rec.PII_VIEW_NAME;
    SET pii_dataset_name = view_rec.PII_VIEW_DATASET_NAME;
    SET counter = 1;
    SET join_part = '';

    -- Fetch the full DDL
    EXECUTE IMMEDIATE (
      'SELECT ddl 
       FROM `cs-cdwp-data-pp2182.' || pii_dataset_name || '.INFORMATION_SCHEMA.VIEWS`
       WHERE table_name = "' || pii_view_name || '"'
    ) INTO curr_ddl;

    -- Basic parsing - split DDL into SELECT and FROM parts
    SET select_part = SUBSTR(curr_ddl, STRPOS(curr_ddl, 'SELECT') + 6, STRPOS(curr_ddl, 'FROM') - STRPOS(curr_ddl, 'SELECT') - 6);
    SET from_part = SUBSTR(curr_ddl, STRPOS(curr_ddl, 'FROM'));

    -- Now loop through PII columns
    FOR pii_col IN (
      SELECT PII_COLUMN_NAME, XREF_TABLE_NAME, XREF_JOIN_COLUMN_NAME, XREF_DK_COLUMN_NAME, DK_COLUMN_ALIAS
      FROM `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_METADATA_T`
      WHERE PII_VIEW_DATASET_NAME = pii_dataset_name
        AND PII_VIEW_NAME = pii_view_name
    ) DO

      -- Check if column exists in SELECT part
      IF REGEXP_CONTAINS(select_part, r'\bt\.' || pii_col.PII_COLUMN_NAME || r'\b') THEN

        -- Insert DK column immediately after original
        SET select_part = REGEXP_REPLACE(
          select_part,
          r'(\bt\.' || pii_col.PII_COLUMN_NAME || r'\b)',
          r'\1, \nCASE WHEN t.' || pii_col.PII_COLUMN_NAME || ' IS NULL THEN NULL ELSE COALESCE(CAST(xr' || counter || '.' || pii_col.XREF_DK_COLUMN_NAME || ' AS STRING), "-1") END AS ' || IFNULL(pii_col.DK_COLUMN_ALIAS, pii_col.XREF_DK_COLUMN_NAME)
        );

        -- Add LEFT JOIN
        SET join_part = join_part || 'LEFT JOIN `cs-cdwp-data-pp2182.CDW_XREF_DATA.' || pii_col.XREF_TABLE_NAME || '` xr' || counter
                    || ' ON t.' || pii_col.PII_COLUMN_NAME || ' = xr' || counter || '.' || pii_col.XREF_JOIN_COLUMN_NAME || '\n';

        -- Increment counter
        SET counter = counter + 1;

      END IF;

    END FOR;

    -- Rebuild full DDL
    SET new_ddl = '/* Auto-generated View for PII DK mapping */\n';
    SET new_ddl = new_ddl || 'CREATE OR REPLACE VIEW `cs-cdwp-data-pp2182.' || pii_dataset_name || '.' || pii_view_name || '` AS\n';
    SET new_ddl = new_ddl || 'SELECT ' || select_part || '\n';
    SET new_ddl = new_ddl || from_part || '\n';
    SET new_ddl = new_ddl || join_part || ';';

    -- Try executing new DDL
    BEGIN
      EXECUTE IMMEDIATE new_ddl;

      -- On Success, update PASS
      UPDATE `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_METADATA_T`
      SET DDL_GENERATED_TS = CURRENT_TIMESTAMP(),
          STATUS = 'PASS'
      WHERE PII_VIEW_DATASET_NAME = pii_dataset_name
        AND PII_VIEW_NAME = pii_view_name;

    EXCEPTION WHEN ERROR THEN
      -- Capture error details
      SET error_message = @@error.message;

      -- Insert into error table
      INSERT INTO `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_METADATA_ERROR_T`
      (VIEW_NAME, DDL_SQL, ERROR_MESSAGE, ERROR_TIMESTAMP)
      VALUES (pii_view_name, new_ddl, error_message, CURRENT_TIMESTAMP());

      -- Update FAIL status
      UPDATE `cs-cdwp-data-pp2182.IDW_ETL_DATA.PII_VIEW_METADATA_T`
      SET DDL_GENERATED_TS = CURRENT_TIMESTAMP(),
          STATUS = 'FAIL'
      WHERE PII_VIEW_DATASET_NAME = pii_dataset_name
        AND PII_VIEW_NAME = pii_view_name;

    END;

  END FOR;

END;
