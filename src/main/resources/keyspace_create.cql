create keyspace context_model WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
create table fact_workhistory
(
    eq_type_key            double,
    eq_model_no_key        double,
    eq_mfr_key             double,
    eq_criti_indi_key      double,
    wh_prt_key             double,
    wh_evt_type_key        double,
    wh_det_mthd_key        double,
    wh_bdn_key             double,
    unit_key               double,
    failure_code_key       double,
    wh_id                  text,
    wh_total_cost          double,
    wh_maint_cost          double,
    wh_start_date          timestamp,
    wh_corp_key            double,
    wh_maint_start_date    timestamp,
    wh_maint_compl_date    timestamp,
    wh_prod_loss_cost      double,
    wh_site_key            double,
    eq_key                 double,
    wh_desc                text,
    repair_time            int,
    wh_maint_compl_date_st date,
    wh_start_date_st       date,
    failure_code_id        text,
    failure_code_desc      text,
    unit_name              text,
    area_key               double,
    area_name              text,
    site_key               double,
    site_name              text,
    site_lat               text,
    site_long              text,
    division_key           double,
    division_name          text,
    comp_key               double,
    comp_name              text,
    corp_key               double,
    corp_name              text,
    reg_key                int,
    reg_name               text,
    sub_ind_key            int,
    sub_ind_name           text,
    ind_key                int,
    ind_name               text,
    ind_grp_key            int,
    ind_grp_name           text,
    ind_sect_key           int,
    ind_sect_name          text,
    wh_bdn_id              text,
    wh_bdn_desc            text,
    wh_det_mthd_id         text,
    wh_det_mthd_desc       text,
    wh_evt_type_id         text,
    wh_evt_type_desc       text,
    wh_prt_id              text,
    wh_prt_desc            text,
    eq_criti_indi_id       text,
    eq_criti_indi_desc     text,
    eq_mfr_id              text,
    eq_mfr_desc            text,
    brand_key              double,
    eq_model_no_id         text,
    eq_model_no_desc       text,
    eq_type_id             int,
    eq_type_name           text,
    eq_class_id            int,
    eq_class_name          text,
    eq_category_id         int,
    eq_category_name       text,
    tenant                 UUID,
    partition_key         bigint,
    wh_key                 double,
    PRIMARY KEY ((tenant,partition_key),wh_key)
);

CREATE INDEX on fact_workhistory (wh_key);

