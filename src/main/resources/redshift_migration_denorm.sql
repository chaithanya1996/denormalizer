
CREATE schema if not exists aa_denorm; 

DROP table dev.aa_denorm.fact_wh_denorm;
CREATE TABLE dev.aa_denorm.fact_wh_denorm (
    wh_key bigint,
    area_key bigint,
    area_name text,
    brand_key bigint,
    comp_key bigint,
    comp_name text,
    corp_key bigint,
    corp_name text,
    division_key bigint,
    division_name text,
    eq_category_id int,
    eq_category_name text,
    eq_class_id int,
    eq_class_name text,
    eq_criti_indi_desc text,
    eq_criti_indi_id text,
    eq_criti_indi_key bigint,
    eq_key bigint,
    eq_mfr_desc text,
    eq_mfr_id text,
    eq_mfr_key bigint,
    eq_model_no_desc text,
    eq_model_no_id text,
    eq_model_no_key bigint,
    eq_type_id int,
    eq_type_key bigint,
    eq_type_name text,
    failure_code_desc text,
    failure_code_id text,
    failure_code_key bigint,
    ind_grp_key int,
    ind_grp_name text,
    ind_key int,
    ind_name text,
    ind_sect_key int,
    ind_sect_name text,
    reg_key int,
    reg_name text,
    repair_time int,
    site_key bigint,
    site_lat text,
    site_long text,
    site_name text,
    sub_ind_key int,
    sub_ind_name text,
    unit_key bigint,
    unit_name text,
    wh_bdn_desc text,
    wh_bdn_id text,
    wh_bdn_key bigint,
    wh_corp_key bigint,
    wh_desc text,
    wh_det_mthd_desc text,
    wh_det_mthd_id text,
    wh_det_mthd_key bigint,
    wh_evt_type_desc text,
    wh_evt_type_id text,
    wh_evt_type_key bigint,
    wh_id text,
    wh_maint_compl_date timestamp,
    wh_maint_compl_date_st date,
    wh_maint_cost float,
    wh_maint_start_date timestamp,
    wh_prod_loss_cost float,
    wh_prt_desc text,
    wh_prt_id text,
    wh_prt_key bigint,
    wh_site_key bigint,
    wh_start_date timestamp,
    wh_start_date_st date,
    wh_total_cost float,
    PRIMARY KEY ( wh_key)
);


-- 'DIM_FAILURE_CODE -> FAILURE_CODE_KEY, ' ||
-- 'CORPORATION_HIERARCHY -> UNIT_KEY,' ||
-- 'DIM_WH_BREAKDOWN -> WH_BDN_KEY,' ||
-- 'DIM_WH_DETECTION_METHOD -> WH_DET_MTHD_KEY,' ||
-- 'DIM_WH_EVENT_TYPE -> WH_EVT_TYPE_KEY,' ||
-- 'DIM_WH_PRIORITY -> WH_PRT_KEY,' ||
-- 'DIM_EQ_CRITICALITY -> EQ_CRITI_INDI_KEY,' ||
-- 'DIM_EQ_MFR-> EQ_MFR_KEY,' ||
-- 'EQ_MODEL_NO -> EQ_MODEL_NO_KEY,' ||
-- 'TAXONOMY_HIERARCHY -> EQ_TYPE_KEY',



INSERT into dev.aa_denorm.fact_wh_denorm 
(
WH_KEY,
	WH_ID ,
	WH_TOTAL_COST  ,
	WH_MAINT_COST  ,
	WH_START_DATE  ,
	WH_CORP_KEY  ,
	WH_MAINT_START_DATE  ,
	WH_MAINT_COMPL_DATE  ,
	WH_PROD_LOSS_COST  ,
	WH_SITE_KEY  ,
	EQ_KEY  ,
	WH_DESC  ,
	REPAIR_TIME  ,
	WH_MAINT_COMPL_DATE_ST  ,
	WH_START_DATE_ST ,
	
	FAILURE_CODE_KEY,
	FAILURE_CODE_ID,
	FAILURE_CODE_DESC,
	
	UNIT_KEY, 
	unit_name, 
	area_key, 
	area_name, 
	site_key, 
	site_name, 
	site_lat, 
	site_long, 
	division_key, 
	division_name, 
	comp_key, 
	comp_name, 
	corp_key, 
	corp_name, 
	reg_key, 
	reg_name, 
	sub_ind_key, 
	sub_ind_name, 
	ind_key, 
	ind_name, 
	ind_grp_key, 
	ind_grp_name, 
	ind_sect_key, 
	ind_sect_name,
	
	WH_BDN_KEY,
	WH_BDN_ID,
	WH_BDN_DESC,
	
	WH_DET_MTHD_KEY,
	WH_DET_MTHD_ID,
	WH_DET_MTHD_DESC,
	
	WH_EVT_TYPE_KEY,
	WH_EVT_TYPE_ID,
	WH_EVT_TYPE_DESC,
	
	WH_PRT_KEY  ,
	wh_prt_id,
	wh_prt_desc,
	
	EQ_CRITI_INDI_KEY,
	eq_criti_indi_id, 
	eq_criti_indi_desc,
	
	EQ_MFR_KEY,
	eq_mfr_id, 
	eq_mfr_desc, 
	brand_key,
	
	EQ_MODEL_NO_KEY  ,
	eq_model_no_id, 
	eq_model_no_desc,
	
	EQ_TYPE_KEY ,
	eq_type_id, 
	eq_type_name, 
	eq_class_id, 
	eq_class_name, 
	eq_category_id, 
	eq_category_name
)	
select 
	fwh.WH_KEY,
	fwh.WH_ID ,
	fwh.WH_TOTAL_COST  ,
	fwh.WH_MAINT_COST  ,
	fwh.WH_START_DATE  ,
	fwh.WH_CORP_KEY  ,
	fwh.WH_MAINT_START_DATE  ,
	fwh.WH_MAINT_COMPL_DATE  ,
	fwh.WH_PROD_LOSS_COST  ,
	fwh.WH_SITE_KEY  ,
	fwh.EQ_KEY  ,
	fwh.WH_DESC  ,
	fwh.REPAIR_TIME  ,
	fwh.WH_MAINT_COMPL_DATE_ST  ,
	fwh.WH_START_DATE_ST ,
	
	fwh.FAILURE_CODE_KEY   ,
	dfc.FAILURE_CODE_ID as FAILURE_CODE_ID,
	dfc.FAILURE_CODE_DESC as FAILURE_CODE_DESC,
	
	fwh.UNIT_KEY  ,
	ch.unit_name, 
	ch.area_key, 
	ch.area_name, 
	ch.site_key, 
	ch.site_name, 
	ch.site_lat, 
	ch.site_long, 
	ch.division_key, 
	ch.division_name, 
	ch.comp_key, 
	ch.comp_name, 
	ch.corp_key, 
	ch.corp_name, 
	ch.reg_key, 
	ch.reg_name, 
	ch.sub_ind_key, 
	ch.sub_ind_name, 
	ch.ind_key, 
	ch.ind_name, 
	ch.ind_grp_key, 
	ch.ind_grp_name, 
	ch.ind_sect_key, 
	ch.ind_sect_name,
	
	fwh.WH_BDN_KEY,
	dwb.WH_BDN_ID,
	dwb.WH_BDN_DESC,
	
	fwh.WH_DET_MTHD_KEY,
	dwdm.WH_DET_MTHD_ID,
	dwdm.WH_DET_MTHD_DESC,
	
	fwh.WH_EVT_TYPE_KEY,
	dwet.WH_EVT_TYPE_ID,
	dwet.WH_EVT_TYPE_DESC,
	
	fwh.WH_PRT_KEY  ,
	dwp.wh_prt_id,
	dwp.wh_prt_desc,
	
	fwh.EQ_CRITI_INDI_KEY,
	deqc.eq_criti_indi_id, 
	deqc.eq_criti_indi_desc,
	
	fwh.EQ_MFR_KEY,
	deqm.eq_mfr_id, 
	deqm.eq_mfr_desc, 
	deqm.brand_key,
	
	fwh.EQ_MODEL_NO_KEY  ,
	eqmn.eq_model_no_id, 
	eqmn.eq_model_no_desc,
	
	fwh.EQ_TYPE_KEY ,
	th.eq_type_id, 
	th.eq_type_name, 
	th.eq_class_id, 
	th.eq_class_name, 
	th.eq_category_id, 
	th.eq_category_name
	
 from dev.aa_replica.FACT_WORKHISTORY fwh
 	Left Join  dev.aa_replica.DIM_FAILURE_CODE dfc on fwh.failure_code_key = dfc.failure_code_key 
 	Left Join  dev.aa_replica.CORPORATION_HIERARCHY ch on fwh.UNIT_KEY = ch.UNIT_KEY 
 	Left Join  dev.aa_replica.DIM_WH_BREAKDOWN dwb on fwh.WH_BDN_KEY = dwb.WH_BDN_KEY 
 	Left Join  dev.aa_replica.DIM_WH_DETECTION_METHOD dwdm on fwh.WH_DET_MTHD_KEY = dwdm.WH_DET_MTHD_KEY 
 	Left Join  dev.aa_replica.DIM_WH_EVENT_TYPE dwet on fwh.WH_EVT_TYPE_KEY = dwet.WH_EVT_TYPE_KEY 
 	Left Join  dev.aa_replica.DIM_WH_PRIORITY dwp on fwh.WH_PRT_KEY = dwp.WH_PRT_KEY 
 	Left Join  dev.aa_replica.DIM_EQ_CRITICALITY deqc on fwh.EQ_CRITI_INDI_KEY = deqc.EQ_CRITI_INDI_KEY 
 	Left Join  dev.aa_replica.DIM_EQ_MFR deqm on fwh.EQ_MFR_KEY = deqm.EQ_MFR_KEY 
 	Left Join  dev.aa_replica.EQ_MODEL_NO eqmn on fwh.EQ_MODEL_NO_KEY = eqmn.EQ_MODEL_NO_KEY 
 	Left Join  dev.aa_replica.TAXONOMY_HIERARCHY th on fwh.EQ_TYPE_KEY = th.EQ_TYPE_KEY  ;

 
 
 
 
create or replace procedure denormalize_fact_workhistory()
language plpgsql
as $$
declare

begin
INSERT into dev.aa_denorm.fact_wh_denorm 
(
WH_KEY,
	WH_ID ,
	WH_TOTAL_COST  ,
	WH_MAINT_COST  ,
	WH_START_DATE  ,
	WH_CORP_KEY  ,
	WH_MAINT_START_DATE  ,
	WH_MAINT_COMPL_DATE  ,
	WH_PROD_LOSS_COST  ,
	WH_SITE_KEY  ,
	EQ_KEY  ,
	WH_DESC  ,
	REPAIR_TIME  ,
	WH_MAINT_COMPL_DATE_ST  ,
	WH_START_DATE_ST ,
	
	FAILURE_CODE_KEY,
	FAILURE_CODE_ID,
	FAILURE_CODE_DESC,
	
	UNIT_KEY, 
	unit_name, 
	area_key, 
	area_name, 
	site_key, 
	site_name, 
	site_lat, 
	site_long, 
	division_key, 
	division_name, 
	comp_key, 
	comp_name, 
	corp_key, 
	corp_name, 
	reg_key, 
	reg_name, 
	sub_ind_key, 
	sub_ind_name, 
	ind_key, 
	ind_name, 
	ind_grp_key, 
	ind_grp_name, 
	ind_sect_key, 
	ind_sect_name,
	
	WH_BDN_KEY,
	WH_BDN_ID,
	WH_BDN_DESC,
	
	WH_DET_MTHD_KEY,
	WH_DET_MTHD_ID,
	WH_DET_MTHD_DESC,
	
	WH_EVT_TYPE_KEY,
	WH_EVT_TYPE_ID,
	WH_EVT_TYPE_DESC,
	
	WH_PRT_KEY  ,
	wh_prt_id,
	wh_prt_desc,
	
	EQ_CRITI_INDI_KEY,
	eq_criti_indi_id, 
	eq_criti_indi_desc,
	
	EQ_MFR_KEY,
	eq_mfr_id, 
	eq_mfr_desc, 
	brand_key,
	
	EQ_MODEL_NO_KEY  ,
	eq_model_no_id, 
	eq_model_no_desc,
	
	EQ_TYPE_KEY ,
	eq_type_id, 
	eq_type_name, 
	eq_class_id, 
	eq_class_name, 
	eq_category_id, 
	eq_category_name
)	
select 
	fwh.WH_KEY,
	fwh.WH_ID ,
	fwh.WH_TOTAL_COST  ,
	fwh.WH_MAINT_COST  ,
	fwh.WH_START_DATE  ,
	fwh.WH_CORP_KEY  ,
	fwh.WH_MAINT_START_DATE  ,
	fwh.WH_MAINT_COMPL_DATE  ,
	fwh.WH_PROD_LOSS_COST  ,
	fwh.WH_SITE_KEY  ,
	fwh.EQ_KEY  ,
	fwh.WH_DESC  ,
	fwh.REPAIR_TIME  ,
	fwh.WH_MAINT_COMPL_DATE_ST  ,
	fwh.WH_START_DATE_ST ,
	
	fwh.FAILURE_CODE_KEY   ,
	dfc.FAILURE_CODE_ID as FAILURE_CODE_ID,
	dfc.FAILURE_CODE_DESC as FAILURE_CODE_DESC,
	
	fwh.UNIT_KEY  ,
	ch.unit_name, 
	ch.area_key, 
	ch.area_name, 
	ch.site_key, 
	ch.site_name, 
	ch.site_lat, 
	ch.site_long, 
	ch.division_key, 
	ch.division_name, 
	ch.comp_key, 
	ch.comp_name, 
	ch.corp_key, 
	ch.corp_name, 
	ch.reg_key, 
	ch.reg_name, 
	ch.sub_ind_key, 
	ch.sub_ind_name, 
	ch.ind_key, 
	ch.ind_name, 
	ch.ind_grp_key, 
	ch.ind_grp_name, 
	ch.ind_sect_key, 
	ch.ind_sect_name,
	
	fwh.WH_BDN_KEY,
	dwb.WH_BDN_ID,
	dwb.WH_BDN_DESC,
	
	fwh.WH_DET_MTHD_KEY,
	dwdm.WH_DET_MTHD_ID,
	dwdm.WH_DET_MTHD_DESC,
	
	fwh.WH_EVT_TYPE_KEY,
	dwet.WH_EVT_TYPE_ID,
	dwet.WH_EVT_TYPE_DESC,
	
	fwh.WH_PRT_KEY  ,
	dwp.wh_prt_id,
	dwp.wh_prt_desc,
	
	fwh.EQ_CRITI_INDI_KEY,
	deqc.eq_criti_indi_id, 
	deqc.eq_criti_indi_desc,
	
	fwh.EQ_MFR_KEY,
	deqm.eq_mfr_id, 
	deqm.eq_mfr_desc, 
	deqm.brand_key,
	
	fwh.EQ_MODEL_NO_KEY  ,
	eqmn.eq_model_no_id, 
	eqmn.eq_model_no_desc,
	
	fwh.EQ_TYPE_KEY ,
	th.eq_type_id, 
	th.eq_type_name, 
	th.eq_class_id, 
	th.eq_class_name, 
	th.eq_category_id, 
	th.eq_category_name
	
 from dev.aa_replica.FACT_WORKHISTORY fwh
 	Left Join  dev.aa_replica.DIM_FAILURE_CODE dfc on fwh.failure_code_key = dfc.failure_code_key 
 	Left Join  dev.aa_replica.CORPORATION_HIERARCHY ch on fwh.UNIT_KEY = ch.UNIT_KEY 
 	Left Join  dev.aa_replica.DIM_WH_BREAKDOWN dwb on fwh.WH_BDN_KEY = dwb.WH_BDN_KEY 
 	Left Join  dev.aa_replica.DIM_WH_DETECTION_METHOD dwdm on fwh.WH_DET_MTHD_KEY = dwdm.WH_DET_MTHD_KEY 
 	Left Join  dev.aa_replica.DIM_WH_EVENT_TYPE dwet on fwh.WH_EVT_TYPE_KEY = dwet.WH_EVT_TYPE_KEY 
 	Left Join  dev.aa_replica.DIM_WH_PRIORITY dwp on fwh.WH_PRT_KEY = dwp.WH_PRT_KEY 
 	Left Join  dev.aa_replica.DIM_EQ_CRITICALITY deqc on fwh.EQ_CRITI_INDI_KEY = deqc.EQ_CRITI_INDI_KEY 
 	Left Join  dev.aa_replica.DIM_EQ_MFR deqm on fwh.EQ_MFR_KEY = deqm.EQ_MFR_KEY 
 	Left Join  dev.aa_replica.EQ_MODEL_NO eqmn on fwh.EQ_MODEL_NO_KEY = eqmn.EQ_MODEL_NO_KEY 
 	Left Join  dev.aa_replica.TAXONOMY_HIERARCHY th on fwh.EQ_TYPE_KEY = th.EQ_TYPE_KEY  ;

end; $$

call denormalize_fact_workhistory();

select COUNT(*) from dev.aa_denorm.fact_wh_denorm fwd ; 
