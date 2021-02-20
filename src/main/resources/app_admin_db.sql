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
 '{adaadd,ddddd}')
;