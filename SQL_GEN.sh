#!/bin/bash

# Get parameters passed to the script
PARAM1="$1" 
PARAM2="$2" 

# Set output file name as VIEW_NM.sql
OUTPUT_FILE="/opt/pr/data/retrieve/PII/${PARAM2}.sql"

# Echo to verify
echo "Running BigQuery export for:"
echo "param1: $PARAM1"
echo "param2: $PARAM2"
echo "Output file: $OUTPUT_FILE"

# Run query to mimic procedure logic and export output
bq query --nouse_legacy_sql --format=csv "
SELECT for_sql_file FROM (
  SELECT 1 col1, '--liquibase formatted sql' AS for_sql_file UNION DISTINCT
  SELECT 2 col1, '--changeset greeshma.kurup:1 labels:CDW-383066' UNION DISTINCT
  SELECT 3 col1, '--comment New View' UNION DISTINCT
  SELECT 4 col1, '' UNION DISTINCT
  SELECT 5 col1, REPLACE(DK_VIEW_DDL, 'cs-cdwp-data-dev6124.', '') 
    FROM \`cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T\`
    WHERE pii_view_db_nm = '${PARAM1}' AND pii_view_nm = '${PARAM2}' UNION DISTINCT
  SELECT 6 col1, '' UNION DISTINCT
  SELECT 7 col1, '--rollback ' || 
    REPLACE(
      REPLACE(
        REPLACE(PII_VIEW_DDL, 'cs-cdwp-data-dev6124.', ''), 
        'CREATE VIEW', 
        'CREATE OR REPLACE VIEW'
      ),
      '\n', 
      '\n--rollback '
    )
    FROM \`cs-cdwp-data-dev6124.IDW_ETL_DATA.PII_DDL_INVTRY_T\`
    WHERE pii_view_db_nm = '${PARAM1}' AND pii_view_nm = '${PARAM2}'
) ORDER BY col1
" > $OUTPUT_FILE
echo "End of script"
