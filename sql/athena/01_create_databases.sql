CREATE DATABASE aq_raw
COMMENT 'Raw layer: source data exactly as received, plus load metadata'
LOCATION 's3://aq-health-239939506885-lake/warehouse/aq_raw/';


CREATE DATABASE aq_audit
COMMENT 'Pipeline monitoring: one row per ingestion run'
LOCATION 's3://aq-health-239939506885-lake/warehouse/aq_audit/';
