CREATE OR REPLACE PROCEDURE `cs-cdwp-data-dev6124.IDW_ETL_DATA.proc_generate_llq_format`()
BEGIN

  DECLARE v_db STRING;
  DECLARE v_view STRING;
  DECLARE v_llq_format STRING;
  DECLARE v_status STRING;
  DECLARE done BOOL DEFAULT FALSE;

  DECLARE cur CURSOR FOR
    SELECT pii_view_db_nm, pii_view_nm
    FROM `cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T`;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur;

  read_loop:
  LOOP
    FETCH cur INTO v_db, v_view;
    IF done THEN
      LEAVE read_loop;
    END IF;

    BEGIN
      DECLARE temp_format ARRAY<STRING>;

      -- Construct LLQ formatted content line by line
      SET temp_format = (
        SELECT ARRAY_AGG(for_sql_file ORDER BY col1) FROM (
		
          SELECT 1 AS col1, '--liquibase formatted sql' AS for_sql_file UNION DISTINCT
          SELECT 2, '--changeset greeshma.kurup:1 labels:CDW-383066' UNION DISTINCT
          SELECT 3, '--comment New View' UNION DISTINCT
          SELECT 4, '' UNION DISTINCT
          SELECT 5, REPLACE(DK_VIEW_DDL, 'cs-cdwp-data-dev6124.', '')
            FROM `cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T`
            WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view UNION DISTINCT
          SELECT 6, '' UNION DISTINCT
          SELECT 7, '--rollback ' || REPLACE(REPLACE(REPLACE(PII_VIEW_DDL, 'cs-cdwp-data-dev6124.', ''),'CREATE VIEW', 'CREATE OR REPLACE VIEW' ), '\n', '\n--rollback ')          FROM `cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T` WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view UNION DISTINCT
          SELECT 8, '' UNION DISTINCT
          SELECT 9, '--liquibase formatted sql' UNION DISTINCT
          SELECT 10, '--changeset greeshma.kurup:1 labels:CDW-383066 Context filter:PP' UNION DISTINCT
          SELECT 11, '--comment New View' UNION DISTINCT
          SELECT 12, 'GRANT ' || role || ' ON SCHEMA ' || project_id || '.' || dataset_id || ' TO ' || grantee || ';' FROM `cs-cdwp-data-dev6124.IDW_ETL_DATA.GRANT_INVT_T`
            WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view UNION DISTINCT
          SELECT 13, '--rollback GRANT ' || role || ' ON SCHEMA ' || project_id || '.' || dataset_id || ' TO ' || grantee || ';' FROM `cs-cdwp-data-dev6124.IDW_ETL_DATA.GRANT_INVT_T` WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view 
		  
		  )
      );

      -- Join array into final string
      SET v_llq_format = ARRAY_TO_STRING(temp_format, '\n');

      -- Update the main table with generated LLQ format
      UPDATE `cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T`
      SET
        LLQ_FORMAT = v_llq_format,
        STATUS = 'PASS',
        DDL_GEN_TS = CURRENT_TIMESTAMP()
      WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view;

    EXCEPTION WHEN ERROR THEN
      -- In case of error, mark as FAIL
      UPDATE `cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T`
      SET
        STATUS = 'FAIL',
        DDL_GEN_TS = CURRENT_TIMESTAMP()
      WHERE pii_view_db_nm = v_db AND pii_view_nm = v_view;
    END;
  END LOOP;

  CLOSE cur;

END;
