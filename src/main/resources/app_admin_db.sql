DROP TABLE public.asset_admin_denorm;

CREATE TABLE public.asset_admin_denorm (
                                           base_table varchar NOT NULL,
                                           join_table_list text NOT NULL,
                                           table_column_list text[] NULL,
                                           CONSTRAINT asset_admin_join_pkey PRIMARY KEY (base_table)
);

INSERT INTO public.asset_admin_denorm (base_table,join_table_list,table_column_list) VALUES
('fact_workhistory',
 'DIM_FAILURE_CODE -> FAILURE_CODE_KEY, ' ||
 'CORPORATION_HIERARCHY -> UNIT_KEY,' ||
 'DIM_WH_BREAKDOWN -> WH_BDN_KEY,' ||
 'DIM_WH_DETECTION_METHOD -> WH_DET_MTHD_KEY,' ||
 'DIM_WH_EVENT_TYPE -> WH_EVT_TYPE_KEY,' ||
 'DIM_WH_PRIORITY -> WH_PRT_KEY,' ||
 'DIM_EQ_CRITICALITY -> EQ_CRITI_INDI_KEY,' ||
 'DIM_EQ_MFR-> EQ_MFR_KEY,' ||
 'EQ_MODEL_NO -> EQ_MODEL_NO_KEY,' ||
 'TAXONOMY_HIERARCHY -> EQ_TYPE_KEY',
 '{,
	,
	,
	,
	,
	}')
;


DROP TABLE public.asset_admin_denorm;

CREATE TABLE public.asset_admin_denorm (
                                           base_table varchar NOT NULL,
                                           join_table_list text NOT NULL,
                                           table_column_list text[] NULL,
                                           CONSTRAINT asset_admin_join_pkey PRIMARY KEY (base_table)
);

INSERT INTO public.asset_admin_denorm (base_table,join_table_list,table_column_list) VALUES
('fact_workhistory',

 'DIM_FAILURE_CODE -> FAILURE_CODE_KEY, ' ||
 'CORPORATION_HIERARCHY -> UNIT_KEY,' ||
 'DIM_WH_BREAKDOWN -> WH_BDN_KEY,' ||
 'DIM_WH_DETECTION_METHOD -> WH_DET_MTHD_KEY,' ||
 'DIM_WH_EVENT_TYPE -> WH_EVT_TYPE_KEY,' ||
 'DIM_WH_PRIORITY -> WH_PRT_KEY,' ||
 'DIM_EQ_CRITICALITY -> EQ_CRITI_INDI_KEY,' ||
 'DIM_EQ_MFR-> EQ_MFR_KEY,' ||
 'EQ_MODEL_NO -> EQ_MODEL_NO_KEY,' ||
 'TAXONOMY_HIERARCHY -> EQ_TYPE_KEY',

 Array['DIM_FAILURE_CODE -> FAILURE_CODE_KEY:FAILURE_CODE_ID:FAILURE_CODE_DESC ',
 'CORPORATION_HIERARCHY -> ',
 'DIM_WH_BREAKDOWN -> WH_BDN_KEY:WH_BDN_ID:WH_BDN_DESC',
 'DIM_WH_DETECTION_METHOD -> WH_DET_MTHD_KEY:WH_DET_MTHD_ID:WH_DET_MTHD_DESC',
 'DIM_WH_EVENT_TYPE -> WH_EVT_TYPE_KEY:WH_EVT_TYPE_ID:WH_EVT_TYPE_DESC',
 'DIM_WH_PRIORITY -> UNIT_KEY:UNIT_NAME:AREA_KEY:AREA_NAME:SITE_KEY:SITE_NAME:SITE_LAT:SITE_LONG:DIVISION_KEY:DIVISION_NAME:CORPORATION_HIERARCHY:COMP_KEY:COMP_NAME:CORP_KEY:CORP_NAME:REG_KEY:REG_NAME:SUB_IND_KEY:SUB_IND_NAME:IND_KEY:IND_NAME:IND_GRP_KEY:IND_GRP_NAME:IND_SECT_KEY:IND_SECT_NAME',
 'DIM_EQ_CRITICALITY -> ',
 'DIM_EQ_MFR-> ',
 'EQ_MODEL_NO -> ',
 'TAXONOMY_HIERARCHY -> ']);
