CREATE schema if not exists aa_replica; 


CREATE TABLE dev.aa_replica.FACT_WORKHISTORY (
	WH_KEY bigint PRIMARY KEY,
	WH_ID nvarchar(350),
	WH_TOTAL_COST float ,
	WH_MAINT_COST float ,
	WH_START_DATE datetime ,
	WH_BDN_KEY bigint ,
	WH_EVT_TYPE_KEY bigint ,
	WH_PRT_KEY bigint ,
	WH_DET_MTHD_KEY bigint ,
	WH_CORP_KEY bigint ,
	WH_MAINT_START_DATE datetime ,
	WH_MAINT_COMPL_DATE datetime ,
	WH_PROD_LOSS_COST float ,
	FAILURE_CODE_KEY bigint  ,
	WH_SITE_KEY bigint ,
	EQ_KEY bigint ,
	WH_DESC nvarchar(350) ,
	UNIT_KEY bigint ,
	EQ_TYPE_KEY bigint,
	REPAIR_TIME int ,
	EQ_CRITI_INDI_KEY bigint ,
	EQ_MFR_KEY bigint,
	EQ_MODEL_NO_KEY bigint ,
	WH_MAINT_COMPL_DATE_ST date ,
	WH_START_DATE_ST date 
);


--- Insert Data

copy dev.aa_replica.FACT_WORKHISTORY from 's3://adapa-cloud/aa_replica/FACT_WORKHISTORY/FACT_WORKHISTORY_202105180255.csv.gz' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
delimiter ',' region 'ap-south-1' csv GZIP IGNOREHEADER 1;



--- 

-- AssetAnswers_Demo.dbo.EQUIPMENT_NBR_DAYS definition

-- Drop table

-- DROP TABLE AssetAnswers_Demo.dbo.EQUIPMENT_NBR_DAYS;

CREATE TABLE dev.aa_replica.EQUIPMENT_NBR_DAYS (
	EQ_KEY bigint NOT NULL,
	DAYS_ACTIVE bigint NULL,
	MONTH_START_DATE date NULL,
	QUARTER_START_DATE date NULL,
	EQ_CRITI_INDI_KEY bigint NOT NULL,
	EQ_MFR_KEY bigint NOT NULL,
	EQ_TYPE_KEY bigint NOT NULL,
	UNIT_KEY bigint NOT NULL,
	FL_KEY bigint NOT NULL,
	EQ_MODEL_NO_KEY bigint NOT NULL
);


copy dev.aa_replica.EQUIPMENT_NBR_DAYS from 's3://adapa-cloud/aa_replica/EQUIPMENT_NBR_DAYS/EQUIPMENT_NBR_DAYS_202105180453.csv.gz' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
delimiter ',' region 'ap-south-1' csv GZIP IGNOREHEADER 1;


-- AssetAnswers_Demo.dbo.FACT_EQUIPMENT definition

-- Drop table

-- DROP TABLE AssetAnswers_Demo.dbo.FACT_EQUIPMENT;

CREATE TABLE dev.aa_replica.FACT_EQUIPMENT (
	EQ_KEY bigint IDENTITY(1,1)PRIMARY KEY,
	EQ_VLD_FRM_DATE datetime NULL,
	EQ_ID nvarchar(350)   NOT NULL,
	EQ_DESC nvarchar(350)   NULL,
	EQ_CRITI_INDI_KEY bigint NOT NULL,
	EQ_MFR_KEY bigint NOT NULL,
	EQ_TYPE_KEY bigint NOT NULL,
	UNIT_KEY bigint NOT NULL,
	EQ_TECH_ID nvarchar(350)   NOT NULL,
	FL_KEY bigint NOT NULL,
	EQ_MODEL_NO_KEY bigint NOT NULL,
	EQ_OBS_START_DATE datetime NULL,
	EQ_OBS_END_DATE datetime NULL
);
 
copy dev.aa_replica.FACT_EQUIPMENT from 's3://adapa-cloud/aa_replica/FACT_EQUIPMENT/part-00000-fe6e4cfc-1b62-455e-83aa-a67d569692a0-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;

CREATE TABLE dev.aa_replica.TAXONOMY_HIERARCHY (
	EQ_TYPE_KEY bigint NOT NULL PRIMARY KEY,
	EQ_TYPE_ID int NOT NULL,
	EQ_TYPE_NAME nvarchar(255) NOT NULL,
	EQ_CLASS_ID int NOT NULL,
	EQ_CLASS_NAME nvarchar(255)  NOT NULL,
	EQ_CATEGORY_ID int NOT NULL,
	EQ_CATEGORY_NAME nvarchar(255)  NOT NULL
);


copy dev.aa_replica.TAXONOMY_HIERARCHY from 's3://adapa-cloud/aa_replica/TAXONOMY_HIERARCHY/part-00000-7ce28212-f984-450b-9769-7cfaa4381a96-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE dev.aa_replica.FACT_EQ_DOWNTIME_EVENTS (
	WH_KEY bigint NOT NULL,
	EQ_KEY bigint NOT NULL,
	"MONTH" datetime NOT NULL,
	MONTH_ST date NULL,
	UNIT_KEY bigint NOT NULL,
	EQ_CRITI_INDI_KEY bigint NOT NULL,
	EQ_MFR_KEY bigint NOT NULL,
	FL_KEY bigint NOT NULL,
	EQ_MODEL_NO_KEY bigint NOT NULL,
	EQ_TYPE_KEY bigint NOT NULL,
	CONSTRAINT PK_FACT_EQ_DOWNTIME_EVENTS PRIMARY KEY (WH_KEY,"MONTH",EQ_KEY)
);


copy dev.aa_replica.FACT_EQ_DOWNTIME_EVENTS from 's3://adapa-cloud/aa_replica/FACT_EQ_DOWNTIME_EVENTS/part-00000-258b9baf-f92c-4369-880e-3032a8914c1e-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE dev.aa_replica.EQ_MODEL_NO (
	EQ_MODEL_NO_KEY bigint IDENTITY(1,1) NOT NULL,
	EQ_MODEL_NO_ID nvarchar(255)  NOT NULL,
	EQ_MODEL_NO_DESC nvarchar(255)  NULL,
	CONSTRAINT PK_EQ_MODEL_NO PRIMARY KEY (EQ_MODEL_NO_KEY)
);


copy dev.aa_replica.EQ_MODEL_NO from 's3://adapa-cloud/aa_replica/EQ_MODEL_NO/part-00000-735dfe53-190f-486d-8c95-abddd3196e2c-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;

CREATE TABLE dev.aa_replica.DIM_WH_PRIORITY (
	WH_PRT_KEY bigint IDENTITY(1,1) NOT NULL,
	WH_PRT_ID nvarchar(50)  NOT NULL,
	WH_PRT_DESC nvarchar(255) NOT NULL,
	CONSTRAINT PK_DIM_WH_PRIORITY PRIMARY KEY (WH_PRT_KEY)
);

copy dev.aa_replica.DIM_WH_PRIORITY from 's3://adapa-cloud/aa_replica/DIM_WH_PRIORITY/part-00000-e87dd368-a77b-4240-8699-93fd2ae604ba-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE  dev.aa_replica.DIM_WH_EVENT_TYPE (
	WH_EVT_TYPE_KEY bigint IDENTITY(1,1) NOT NULL,
	WH_EVT_TYPE_ID nvarchar(255)  NOT NULL,
	WH_EVT_TYPE_DESC nvarchar(255) NOT NULL,
	CONSTRAINT PK_DIM_WH_EVENT_TYPE PRIMARY KEY (WH_EVT_TYPE_KEY)
);

copy dev.aa_replica.DIM_WH_EVENT_TYPE from 's3://adapa-cloud/aa_replica/DIM_WH_EVENT_TYPE/part-00000-f79f94f2-ade6-46f4-a76e-18221d89c8e0-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE dev.aa_replica.DIM_WH_DETECTION_METHOD (
	WH_DET_MTHD_KEY bigint IDENTITY(1,1) NOT NULL,
	WH_DET_MTHD_ID nvarchar(255)  NOT NULL,
	WH_DET_MTHD_DESC nvarchar(255)  NOT NULL,
	CONSTRAINT PK_DIM_WH_DETECTION_METHOD PRIMARY KEY (WH_DET_MTHD_KEY)
);


copy dev.aa_replica.DIM_WH_DETECTION_METHOD from 's3://adapa-cloud/aa_replica/DIM_WH_DETECTION_METHOD/part-00000-ad93b3b9-ac5a-46d1-85c0-e44b0298adc8-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


-- AssetAnswers_Demo.dbo.DIM_WH_BREAKDOWN definition

-- Drop table

-- DROP TABLE AssetAnswers_Demo.dbo.DIM_WH_BREAKDOWN;

CREATE TABLE dev.aa_replica.DIM_WH_BREAKDOWN (
	WH_BDN_KEY bigint IDENTITY(1,1) NOT NULL,
	WH_BDN_ID nvarchar(255)  NOT NULL,
	WH_BDN_DESC nvarchar(255) NOT NULL,
	CONSTRAINT PK_DIM_WH_BREAKDOWN PRIMARY KEY (WH_BDN_KEY)
);

copy dev.aa_replica.DIM_WH_BREAKDOWN from 's3://adapa-cloud/aa_replica/DIM_WH_BREAKDOWN/part-00000-9a7c6daa-0465-4cc2-b331-4491b5b3a4e4-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;



CREATE TABLE dev.aa_replica.DIM_FAILURE_CODE (
	FAILURE_CODE_KEY bigint IDENTITY(1,1) NOT NULL,
	FAILURE_CODE_ID nvarchar(350)  NOT NULL,
	FAILURE_CODE_DESC nvarchar(500) NOT NULL,
	CONSTRAINT PK_DIM_FAILURE_CODE PRIMARY KEY (FAILURE_CODE_KEY)
);

copy dev.aa_replica.DIM_FAILURE_CODE from 's3://adapa-cloud/aa_replica/DIM_FAILURE_CODE/part-00000-7f019a5a-4ec9-4a7e-841f-0e0be2ba5591-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE dev.aa_replica.DIM_EQ_MFR (
	EQ_MFR_KEY bigint IDENTITY(1,1) NOT NULL,
	EQ_MFR_ID nvarchar(255) NOT NULL,
	EQ_MFR_DESC nvarchar(255) NOT NULL,
	BRAND_KEY bigint NOT NULL,
	CONSTRAINT PK_DIM_EQ_MFR PRIMARY KEY (EQ_MFR_KEY)
);

copy dev.aa_replica.DIM_EQ_MFR from 's3://adapa-cloud/aa_replica/DIM_EQ_MFR/part-00000-de79e455-1177-434c-85fb-f65f05ded55a-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;


CREATE TABLE dev.aa_replica.DIM_EQ_CRITICALITY (
	EQ_CRITI_INDI_KEY bigint IDENTITY(1,1) NOT NULL,
	EQ_CRITI_INDI_ID nvarchar(255)  NOT NULL,
	EQ_CRITI_INDI_DESC nvarchar(255) NOT NULL,
	CONSTRAINT PK_DIM_EQ_CRITICALITY PRIMARY KEY (EQ_CRITI_INDI_KEY)
);

copy dev.aa_replica.DIM_EQ_CRITICALITY from 's3://adapa-cloud/aa_replica/DIM_EQ_CRITICALITY/part-00000-fc220a67-2700-402f-8136-e5ed93ea074a-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;



CREATE TABLE dev.aa_replica.CORPORATION_HIERARCHY (
	UNIT_KEY bigint NOT NULL,
	UNIT_NAME nvarchar(350) NOT NULL,
	AREA_KEY bigint NOT NULL,
	AREA_NAME nvarchar(350) NOT NULL,
	SITE_KEY bigint NOT NULL,
	SITE_NAME nvarchar(350) NOT NULL,
	SITE_LAT nvarchar(350) NOT NULL,
	SITE_LONG nvarchar(350) NOT NULL,
	DIVISION_KEY bigint NOT NULL,
	DIVISION_NAME nvarchar(350) NOT NULL,
	COMP_KEY bigint NOT NULL,
	COMP_NAME nvarchar(350)  NOT NULL,
	CORP_KEY bigint NOT NULL,
	CORP_NAME nvarchar(350) NOT NULL,
	REG_KEY int NOT NULL,
	REG_NAME nvarchar(255) NOT NULL,
	SUB_IND_KEY int NOT NULL,
	SUB_IND_NAME nvarchar(255)  NOT NULL,
	IND_KEY int NOT NULL,
	IND_NAME nvarchar(255) NOT NULL,
	IND_GRP_KEY int NOT NULL,
	IND_GRP_NAME nvarchar(255) NOT NULL,
	IND_SECT_KEY int NOT NULL,
	IND_SECT_NAME nvarchar(255) NOT NULL,
	CONSTRAINT PK_CORPORATION_HIERARCHY PRIMARY KEY (UNIT_KEY)
);

copy dev.aa_replica.CORPORATION_HIERARCHY from 's3://adapa-cloud/aa_replica/CORPORATION_HIERARCHY/part-00000-d834d83d-fc07-4966-8cb6-93cbe3e8fafc-c000.snappy.parquet' 
credentials 'aws_iam_role=arn:aws:iam::645239897846:role/myRedshiftRole.' 
FORMAT AS parquet ;

