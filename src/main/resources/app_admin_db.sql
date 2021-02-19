
DROP TABLE public.asset_admin_join;

CREATE TABLE public.asset_admin_join (
	base_table varchar NOT NULL,
	join_table_list text NOT NULL,
	join_on_col_list text NOT NULL,
	table_column_list text NULL,
	CONSTRAINT asset_admin_join_pkey PRIMARY KEY (base_table)
);

INSERT INTO public.asset_admin_join (base_table,join_table_list,join_on_col_list,table_column_list) VALUES
('fact_workhistory',
'DIM_FAILURE_CODE,CORPORATION_HIERARCHY,DIM_WH_BREAKDOWN,DIM_WH_DETECTION_METHOD,DIM_WH_EVENT_TYPE,DIM_WH_PRIORITY,DIM_EQ_CRITICALITY,DIM_EQ_MFR,EQ_MODEL_NO,TAXONOMY_HIERARCHY',
'FAILURE_CODE_KEY,UNIT_KEY,WH_BDN_KEY,WH_DET_MTHD_KEY,WH_EVT_TYPE_KEY,WH_PRT_KEY,EQ_CRITI_INDI_KEY,EQ_MFR_KEY,EQ_MODEL_NO_KEY,EQ_TYPE_KEY',
'')
;