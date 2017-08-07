/*
Payment Power User Look-a-like
Date: 06/30/2017

step 01: create cross reference table for acct_dim_nb to cust_dim_nb
step 02: create cross reference table for prof_id to cust_dim_nb
step 03: pull LOB by cust_dim_nb from RDM tables
step 04: pull LOB by cust_dim_nb from MAP tables
step 05: pull product enrollment information
step 06: pull QuickPay, Merchant BP, External/Internal Transfer from PYMT tables
step 07: pull wire from pmttrm landing table
step 08: pull Quick Deposit from TRN_MSR table by slot code
step 09: pull card payments from CARD data mart
step 10: merge all payments
step 11: merge all characteristics
*/

options compress = yes;
options mprint mlogic details source source2 symbolgen;
%let sandbox = /gpfs_nonhsm02/dmg/mis/sandbox/i637308;
%let project = adhoc;

%let datadir = &sandbox./data/&project;
libname sb "&datadir.";

/*
 Set up mobile & online channel ID macro variables - iPhone, Private Banking iPhone, iPad, Private Banking iPad, Android, 
 Private Banking android, enhanced mobile browser, mobile web browswer, Windows, Blackberry, iPhone Freedom Pay, Android Freedom Pay 
 */
%let mob_chnl_id='MON','PBN','MOP','PBP','MOD','PBD','MOE','MWB','MCW','BRY','MWD','MWN';
%let web_chnl_id='COL','C30';

%let yymm = 201705;
%let from_date = '2017-05-01';
%let to_date = '2017-05-31';


/******************************** step 01: map acct_dim_nb to cust_dim_nb ********************************/
proc sql;
connect to teradata(authdomain=idwprd server=idwprd mode=teradata connection=global);
create table sb.acct_to_cust_&yymm. as select * from connection to teradata (
select 
	acct_dim_nb, 
	cust_dim_nb
from
ICDW_CB_PRSN_V.RDM_FCT_ACCT
where TIME_DIM_NB = &to_date.
group by 1, 2
order by 1, 2
);
disconnect from teradata;
quit;


/********************************* step 02: map prof_id to cust_dim_nb ***********************************/
proc sql;
connect to teradata(authdomain=IDWPRD server=IDWPRD mode=teradata connection=global); 
 create table sb.prof_to_cust_&yymm. as select * from connection to teradata (
		select  
			A.CURR_CUST_DIM_NB,
			C.PROF_ID
		from 
			ICDW_CB_PRSN_V.DCM_OLM_DIM_CUST A
			inner join ICDW_CB_PRSN_V.DCM_OLM_XREF_PROF_CUST B on A.OLM_CUST_DIM_NB = B.OLM_CUST_DIM_NB 
			inner join ICDW_CB_PRSN_V.DCM_OLM_DIM_PROF C on C.OLM_PROF_DIM_NB = B.OLM_PROF_DIM_NB 
		where 
			A.EFF_STRT_DT <= &to_date. and (A.EFF_END_DT >= &to_date. or A.EFF_END_DT is null)
			and B.EFF_STRT_DT <= &to_date. and (B.EFF_END_DT >= &to_date. or B.EFF_END_DT is null)
			and C.EFF_STRT_DT <= &to_date. and (C.EFF_END_DT >= &to_date. or C.EFF_END_DT is null)
			and C.STS_IN = 'A'
			and B.PRIM_IN = '1'
			and A.CURR_CUST_DIM_NB > 0
			and C.PROF_ID > 0
 );
 disconnect from teradata;
quit;


/************************************** step 03: LOB by cust_dim_nb **************************************/
proc sql;
connect to teradata(authdomain = IDWPRD server = IDWPRD mode = TERADATA connection = GLOBAL); 
create table sb.cust_rdm_&yymm. as select * from connection to teradata (
 select
 	A.CUST_DIM_NB,

	max(E.CUST_AGE_RNG_CD) as age_range,

	max(case when UNVS_PROD_CLS_CD in ('D') and UNVS_PROD_FAM_CD not in ('TIMD') then 'Y' else 'N' end) 		as dep_ind,
	max(case when UNVS_PROD_CLS_CD in ('D') and UNVS_PROD_GP_CD in ('ICK','CHK','MMS') then 'Y' else 'N' end) 	as dda_ind,
	max(case when UNVS_PROD_FAM_CD = 'CARD' then 'Y' else 'N' end) 												as card_ind,
	max(case when UNVS_PROD_GP_CD in ('MTG','EMS') then 'Y' else 'N' end) 										as mortgage_ind,
	max(case when UNVS_PROD_GP_CD = 'ALS' then 'Y' else 'N' end) 												as auto_ind

 from 
	ICDW_CB_PRSN_V.RDM_FCT_ACCT 						as A
	inner join ICDW_CB_PRSN_V.RDM_DIM_ACCT_PROD 		as B	on A.ACCT_PROD_DIM_NB = B.ACCT_PROD_DIM_NB
	inner join ICDW_CB_PRSN_V.RDM_DIM_HHLD_SEG 			as C	on A.HHLD_SEG_DIM_NB = C.HHLD_SEG_DIM_NB
	inner join ICDW_CB_PRSN_V.RDM_DIM_CUST_DTL			as E	on E.CUST_DIM_NB = A.CUST_DIM_NB

 where 
	A.TIME_DIM_NB = &to_date.
	and CRM_CLS_CD not in ('CML','PCD','PCB','PVB','WTH')

 group by 1
);
disconnect from teradata;
quit;


/******************************** step 04: pull demographic info from MAP **********************************/
proc sql;
connect to greenplm as db(authdomain=GPAUTH server=bdtcstr15n1.svr.us.jpmchase.net db=dg port=5432);
create table sb.cust_map_&yymm. as select * from connection to db (
	select 
		cust_dim_nb, 
		file_date,
		crm_class_cd,

		/*LOB*/
	    (case when pers_checking_ind+pers_saving_ind+pers_cd_ind+pers_invst_ind > 0 then 1 else 0 end)        as reta_ind,
    	(case when freedom_ccrd_in+slate_ccrd_in+palladium_card_in+amazon_card_ind
          			+(case when (saphre_reg_ccrd_in = 1 or saphre_pref_ccrd_in = 1) then 1 else 0 end)
                	+united_card_ind+southwest_card_ind+ihg_card_in+hyatt_card_in+aarp_card_in
                	+disney_card_ind+marriott_card_ind+ritz_card_in+british_airway_card_in
                	+mary_kay_card_in+military_card_in+fairmont_card_in+ink_card_ind
                	+marriott_bus_card_in+southwest_bus_card_in+united_bus_card_in > 0 then 1 else 0 end)     as card_ind,
    	(case when mortgage_ind+home_equity_ind > 0 then 1 else 0 end)                                        as mort_ind,
    	(case when auto_lending_ind > 0 then 1 else 0 end)                                                    as auto_ind,
    	(case when busn_checking_ind+busn_saving_ind+busn_credit_ind+paymentech_in > 0 then 1 else 0 end)     as busi_ind,

		/* digital */
		all_trans_days,
		onln_trans_days,
		mobile_trans_days,
		all_signon_days,
		onln_signon_days,
		mobile_signon_days,

		/* call center */
		cc_total_days,
		cc_liverep_days,
		cc_vru_days,
		cc_liverep_advisor_days,
		cc_liverep_fraud_days,
		cc_liverep_actvtn_days,
		cc_vru_advisor_days,
		cc_vru_fraud_days,
		cc_vru_actvtn_days,
		tbc_wrapup_days,
		tbc_ivr_unique_days,
		tbcdays,/*tbc transaction days*/
		tbcdays_liverep,
		tbcdays_ivr,

		/* branch */
		branchdays,
		branchdays_pb,
		branchdays_teller,

		/* atm */
		atmdays,
		cc_atm_cash_adv_days,
		cc_atm_pymnt_days,

		/* demos */
		ltst_geo_rgn_nm, ltst_geo_mkt_nm, ltst_geo_sbmkt_nm, state_cd, crm_seg_path_cd,
		age, prsr_gndr_tx, mari_sts_cd, ocp_tx, segment_cd, footprint, in_market, open_year, open_month,

		dep_wallet, inv_wallet, dep_bal, prim_bank_hhld, oldst_acct_opn_dt,
		first_prod_class, multi_prod_st_dt_cust, multi_prod_st_dt_hhld, chk_inactive_flag,
		sav_inactive_flag, ccrd_inactive_flag, cpc_lead_hhld_flag

	from dm.intg_cdim_cust_profile
	where
		cust_dim_nb > 0
	    and crm_class_cd not in ('CML','WTH','PVB','PCB','PCD') /* this restricts to customers in CCB Households only */
	    and file_date = &from_date.        
	order by 1, 2
);
disconnect from db;
quit;


/******************************** step 05a: pull QD QP MBP enrollment info **********************************/
proc sql;
connect to teradata(authdomain=IDWPRD server=IDWPRD mode=teradata connection=global);
 create table sb.enrollment_&yymm. as select * from connection to teradata (
	  select
	    t1.profile_id as prof_id,
	    max(case when t3.product_code in (2024,2037,2034,2035) then 1 else 0 end) as ind_qd_enrl,
	    max(case when t3.product_code = 2016 then 1 else 0 end) as ind_qp_enrl,
	    max(case when t3.product_code = 2005 then 1 else 0 end) as ind_bp_enrl

	  from 
	    icdw_fl_gbl_v.lda_cig_prod_prf_prof_daily    t1 
	    join icdw_fl_gbl_v.lda_cig_xref_prf_prfprd_daily  t2  on  t1.profile_id=t2.profile_id 
	    join icdw_fl_gbl_v.lda_cig_prod_prf_prod_daily    t3  on  t2.product_id=t3.product_id
	  where 
		t1.status = 101 
	    and t3.STATE = 331 
	    and t3.STATUS = 101 
	    and t3.product_code in (2024,2037,2034,2035,2016,2005)
	    and t3.modified_dt <= &to_date.
	  group by 1
	  order by 1
 );
disconnect from teradata;
quit;


proc sort data = sb.enrollment_&yymm.; by prof_id; 
proc sort data = sb.prof_to_cust_&yymm.; by prof_id; 
data enrl_w_cust;
merge
	sb.enrollment_&yymm.(in=in1)
	sb.prof_to_cust_&yymm.(in=in2);
by prof_id;

 if in1 and in2 then output;
run;	


proc sql;
create table sb.enrl_by_cust_&yymm. as
select
	cust_dim_nb,
	max(ind_qd_enrl) 	 as ind_qd_enrl,
	max(ind_qp_enrl) 	 as ind_qp_enrl,
	max(ind_bp_enrl) 	 as ind_mbp_enrl

from enrl_w_cust
where cust_dim_nb > 0
group by 1
order by 1
;
quit;


/******************************** step 05b: pull mobile active cust_dim_nb **********************************/
/*See chk_waterfall*/











/******************************** step 06: pull QP MBP Int/Ext Xfer from payment table **********************************/
proc sql;
connect to teradata(authdomain=IDWPRD server=IDWPRD mode=teradata connection=global);
 create table sb.pymt_1mo_qp_bp_xfer_&yymm. as select * from connection to teradata (
 select 
	a.prof_id,
	a.pymt_add_dt,                  
	a.pymt_add_tm,                   
	a.chg_last_upd_dt,
	a.chg_last_upd_tm,
	a.pymt_id,
	a.rcur_pymt_id,
	b.chnl_id,
	b.pymt_tp_cd,
	b.pymt_tp_tx,
	b.pymt_sts_cd,
	b.pymt_sts_tx

	from 
		icdw_cb_prsn_v.vw_dcm_olm_fct_dly_pymt_cur_m1   a 
		inner join icdw_cb_prsn_v.dcm_olm_dim_pymt_trn_prfl  b on a.olm_pymt_trn_prfl_dim_nb = b.olm_pymt_trn_prfl_dim_nb 
	where 
		((b.pymt_tp_cd = '1' and b.pymt_sts_cd in ('10','52')) or /*merchant bill pay*/
		 (b.pymt_tp_cd = '4' and b.pymt_sts_cd in ('10','52')) or /*external transfer - single*/
		 (b.pymt_tp_cd = '5' and b.pymt_sts_cd in ('10','52')) or /*external transfer - recurring*/
		 (b.pymt_tp_cd = '6' and b.pymt_sts_cd in ('10','52')) or /*inbound transfer - single*/
		 (b.pymt_tp_cd = '7' and b.pymt_sts_cd in ('10','52')) or /*inbound transfer - recurring*/
		 (b.pymt_tp_cd = '8' and b.pymt_sts_cd in ('10','52')) or /*internal transfer - single*/
		 (b.pymt_tp_cd = '9' and b.pymt_sts_cd in ('10','52')) or /*internal transfer - recurring*/
	 	 (b.pymt_tp_cd in ('12','14','15','16','17','21','22','23') and b.pymt_sts_cd = '52')) /*quickpay - Recurring,Xfer,Inbound,Outbound,CardInternal,CardExternal,ONS*/

		and a.pymt_add_dt between &from_date. and &to_date.
		and b.chnl_id in (&web_chnl_id., &mob_chnl_id.)
		and a.prof_id > 0 

 	order by 1, 2
 );
 disconnect from teradata;
quit;


/******************************** step 07: pull wire from pmttrm landing table **********************************/
proc sql;
connect to teradata(authdomain=IDWPRD server=IDWPRD mode=teradata connection=global);
 create table sb.pymt_1mo_wire_&yymm. as select * from connection to teradata (
 select 
 	cast(lctmpd.cust_personid as bigint) as prof_id, 
	lctmpd.date_added_dt as pymt_add_dt,                 
	lctmpd.date_added_tm as pymt_add_tm, 
	lctmpd.Last_Updated_DT as chg_last_upd_dt,
	lctmpd.Last_Updated_TM as chg_last_upd_tm,            
	lctmpd.PmtID as pymt_id,
	lctmpd.RecurringPmtID as rcur_pymt_id,
	/*lctmpd.ChannelID as chnl_id, -- Not Populated in ICDW */
	lctmpd.Payment_Method as pymt_tp_cd,
	case  
		when lctmpd.Wire_Type is null and lctmpd.Payment_Method = 'WIRE' then 'Other'
		else lctmpd.Wire_Type
	end as "Wire_Type",
	lctmpd.PmtStatus_Code as pymt_sts_cd,
	lctmpd.PmtStatus as pymt_sts_tx

  from 
	icdw_fl_gbl_v.lda_cig_txns_mms_pmttrm_daily lctmpd

  where 
  	lctmpd.date_added_dt between &from_date. and &to_date.
/*	lctmpd.Prd_End_DT between &from_date. and &to_date.*/
/*	and lctmpd.Last_Updated_DT between &from_date. and &to_date.*/
	and lctmpd.Payment_Method In ('ACHPAYROLL', 'ACHTAX', 'colpmt', 'Vendor', 'WIRE')
/*	and lctmpd.PmtStatus_Code = '46'*/

  order by 1, 2
 );
 disconnect from teradata;
quit;



/******************** step 08: pull Quick Deposit from TRN_MSR table by slot code *******************/
proc sql;
connect to teradata(authdomain=IDWPRD server=IDWPRD mode=teradata connection=global);
 create table sb.pymt_1mo_qd_&yymm. as select * from connection to teradata (
	 select
		c.prof_id,
		a.tran_dt,
		a.tran_tm,
		a.olm_sess_key_nb,
		a.pymt_id,
		a.rcur_pymt_id,
		b.chnl_id,
		b.tran_tp_cd,
		b.tran_tp_tx

	 from
		ICDW_CB_PRSN_V.DCM_OLM_FCT_DLY_TRN_MSR A
	    inner join ICDW_CB_PRSN_V.DCM_OLM_DIM_TRN_PRFL B    on A.OLM_TRAN_PRFL_DIM_NB=B.OLM_TRAN_PRFL_DIM_NB
	    inner join ICDW_CB_PRSN_V.DCM_OLM_DIM_PROF C        on A.OLM_PROF_DIM_NB=C.OLM_PROF_DIM_NB
	 where
	 c.prof_id > 0
		and B.CHNL_ID in (&web_chnl_id., &mob_chnl_id.)
		and B.TRAN_TP_CD in ('7520')
		and A.TRAN_DT between &from_date. and &to_date.
 	order by 
 		1, 2, 3
 );
 disconnect from teradata;
quit;


/******************************** step 09: pull card payments from CARD data mart **********************************/
proc sql;
connect to teradata(authdomain=idwprd server=idwprd mode=teradata connection=global);
create table sb.pymt_1mo_card_&yymm. as select * from connection to teradata (
select 
	c.acct_ref_nb, /*=acct_dim_nb in olm table*/
	c.tr_num,
	c.check_account,
	c.check_number,
	c.check_amount,
	c.cr_acct_curr_bal_am,
	c.eff_dt,
	c.post_date,
	c.card_nb,
	c.firm_dda_in,
	c.firm_bnk_rgn_id,
	c.entp_cust_id,
	c.processor_id,
	c.sourcesystem_key,
	case when c.processor_id in ('09','21','50') then 'COL'
		 when c.processor_id in ('14','16') then 'Mobile'
		 when c.processor_id in ('19','51','53','54','55','56','57','58','63') then 'Branch'
		 when c.processor_id in ('22','30','31') then 'VRU'
		 when c.processor_id in ('07','08') then 'CSR'
		 when c.processor_id in ('12') then 'Autopay'
		 when c.processor_id in ('65') then 'ATM'
		 when c.processor_id in ('39','42','44','72') then 'Non-Chase BP'
		 when c.processor_id in ('4','5') then 'Paper_Check'
		 when c.processor_id in ('11','13','32','75') then 'Terminated'
		 else 'Other'
	end as pymt_chnl
from dwhmgr.checks c
where 
	c.post_date between &from_dt. and &to_dt.
/*	c.eff_dt between &from_dt. and &to_dt.*/

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
);
disconnect from teradata;
quit;

proc sql;
create table epay_by_acct as 
select distinct
	t2.acct_ref_nb,

	sum(case when t2.processor_id in (12) then 1 else 0 end) as cnt_rec,
	sum(case when t2.processor_id in (9) then 1 else 0 end) as cnt_web,
	sum(case when t2.processor_id in (16) then 1 else 0 end) as cnt_mob,
	sum(case when t2.processor_id in (14) then 1 else 0 end) as cnt_emb,
	sum(case when t2.processor_id in (30,31) then 1 else 0 end) as cnt_vru,
	sum(case when t2.processor_id in (7,8) then 1 else 0 end) as cnt_rep,
	sum(case when t2.processor_id in (42,44,72,39,67) then 1 else 0 end) as cnt_3bp,

	sum(case when t2.processor_id in (21,50) then 1 else 0 end) as cnt_ret_trn,
	sum(case when t2.processor_id in (65) then 1 else 0 end) as cnt_ret_atm,
	sum(case when t2.processor_id in (22) then 1 else 0 end) as cnt_ret_ivr,

	sum(case when t2.processor_id in (41,68,69,32) then 1 else 0 end) as cnt_other_ele,

	sum(case when t2.processor_id in (4,5,25,24,34,35,36,37,38) then 1 else 0 end) as cnt_crd_ppr,
	sum(case when t2.processor_id in (19,51,53,54,55,56,57,58,63) then 1 else 0 end) as cnt_ret_ppr

from 
	sb.pymt_1mo_card_&yymm. t2

where 
	t2.acct_ref_nb > 0
	and ^missing(t2.eff_dt) 

group by 1
order by 1
;
quit;


proc sort data = sb.acct_to_cust_&yymm.; by acct_dim_nb;
data epay_w_cust;
format cust_dim_nb 15.;
merge 
	epay_by_acct(in=a) 
	sb.acct_to_cust_&yymm.(in=b rename=(acct_dim_nb=acct_ref_nb));
by acct_ref_nb;
if a;
run;
proc sort data = epay_w_cust; by cust_dim_nb;

proc sql;
create table sb.pymt_card_by_cust_&yymm. as
select
	cust_dim_nb,
	max(case when cnt_rec > 0 or cnt_web > 0 or cnt_mob > 0 or cnt_emb > 0 then 1 else 0 end) as ind_epay,
	sum(cnt_rec) as cnt_epay_auto,
	sum(cnt_web) as cnt_epay_web,
	sum(cnt_mob) as cnt_epay_mob,
	sum(cnt_emb) as cnt_epay_emb

from epay_w_cust
where cust_dim_nb > 0
group by 1
order by 1
;
quit;

data sb.pymt_card_by_cust_&yymm.;
set sb.pymt_card_by_cust_&yymm.;
cnt_epay = sum(cnt_epay_auto, cnt_epay_web, cnt_epay_mob, cnt_epay_emb);
keep cust_dim_nb ind_epay cnt_epay;
run;


/******************************** step 10: merge all payment at prof_id level **********************************/
data multi;
set sb.pymt_1mo_qp_bp_xfer_&yymm.
	sb.pymt_1mo_wire_&yymm.
	sb.pymt_1mo_qd_&yymm.
;
run;
proc sort data = multi; by prof_id; run;

proc sql;
create table pymt_by_prof as 
select 
	prof_id,
	max(case when tran_tp_cd = '7520' then 1 else 0 end) as ind_qd,
	max(case when pymt_tp_cd in ('1') and pymt_sts_cd = '10' then 1 else 0 end) as ind_mbp,
	max(case when pymt_tp_cd in ('4','5','6','7') and pymt_sts_cd in ('10','52') then 1 else 0 end) as ind_ext,
	max(case when pymt_tp_cd in ('8','9') and pymt_sts_cd = '52' then 1 else 0 end) as ind_int,
	max(case when pymt_tp_cd in ('11','12','14','15','16','17') and pymt_sts_cd = '52' then 1 else 0 end) as ind_qp,
	max(case when pymt_tp_cd in ('WIRE') and pymt_sts_cd = '46' then 1 else 0 end) as ind_wire,

	sum(case when tran_tp_cd = '7520' then 1 else 0 end) as cnt_qd,
	sum(case when pymt_tp_cd in ('1') and pymt_sts_cd = '10' then 1 else 0 end) as cnt_mbp,
	sum(case when pymt_tp_cd in ('4','5','6','7') and pymt_sts_cd in ('10','52') then 1 else 0 end) as cnt_ext,
	sum(case when pymt_tp_cd in ('8','9') and pymt_sts_cd = '52' then 1 else 0 end) as cnt_int,
	sum(case when pymt_tp_cd in ('11','12','14','15','16','17') and pymt_sts_cd = '52' then 1 else 0 end) as cnt_qp,
	sum(case when pymt_tp_cd in ('WIRE') and pymt_sts_cd = '46' then 1 else 0 end) as cnt_wire,

	max(case when tran_tp_cd = '7520' and chnl_id in (&mob_chnl_id.) then 1 else 0 end) as ind_qd_mob,
	sum(case when tran_tp_cd = '7520' and chnl_id in (&mob_chnl_id.) then 1 else 0 end) as cnt_qd_mob,

	max(case when pymt_tp_cd in ('11','12','14','15','16','17') and pymt_sts_cd = '52' and chnl_id in (&mob_chnl_id.) then 1 else 0 end) as ind_qp_mob,
	sum(case when pymt_tp_cd in ('11','12','14','15','16','17') and pymt_sts_cd = '52' and chnl_id in (&mob_chnl_id.) then 1 else 0 end) as cnt_qp_mob

from multi
group by 1
;
run;

proc sort data = pymt_by_prof; by prof_id; 
proc sort data = sb.prof_to_cust_&yymm.; by prof_id; 
data pymt_w_cust;
merge
	pymt_by_prof(in=in1)
	sb.prof_to_cust_&yymm.(in=in2);
by prof_id;

 if in1 and in2 then output;
run;	


proc sql;
create table sb.pymt_dda_by_cust_&yymm. as
select
	cust_dim_nb,
	max(ind_qd) 	 as ind_qd,
	max(ind_mbp) 	 as ind_mbp,
	max(ind_ext) 	 as ind_ext,
	max(ind_int) 	 as ind_int,
	max(ind_qp) 	 as ind_qp,
	max(ind_wire) 	 as ind_wire,

	sum(cnt_qd) 	 as cnt_qd,
	sum(cnt_mbp) 	 as cnt_mbp,
	sum(cnt_ext) 	 as cnt_ext,
	sum(cnt_int) 	 as cnt_int,
	sum(cnt_qp) 	 as cnt_qp,
	sum(cnt_wire) 	 as cnt_wire,

	max(ind_qd_mob)	 as ind_qd_mob,
	sum(cnt_qd_mob)	 as cnt_qd_mob,
	max(ind_qp_mob)	 as ind_qp_mob,
	sum(cnt_qp_mob)	 as cnt_qp_mob

from pymt_w_cust
where cust_dim_nb > 0
group by 1
order by 1
;
quit;


proc sort data = sb.pymt_dda_by_cust_&yymm. ; by cust_dim_nb ;
proc sort data = sb.pymt_card_by_cust_&yymm. ; by cust_dim_nb ;

data sb.pymt_all_by_cust_&yymm.;
merge 
	sb.pymt_dda_by_cust_&yymm.
	sb.pymt_card_by_cust_&yymm.
;
by cust_dim_nb;
array chg ind_: cnt_:;
do over chg;
if chg = . then chg = 0;
end;
run;


proc delete data = sb.pymt_dda_by_cust_&yymm. ; run;
proc delete data = sb.pymt_card_by_cust_&yymm. ; run;



/********************** step 11: merge all characteristics (MAP & RDM) ***********************/

proc sort data = sb.cust_rdm_&yymm.; by cust_dim_nb;
proc sort data = sb.cust_map_&yymm.; by cust_dim_nb;

data sb.cust_info_&yymm.;
merge
	sb.cust_rdm_&yymm.(in=a)
	sb.cust_map_&yymm.(in=b drop=age reta_ind card_ind mort_ind auto_ind busi_ind)
;
by cust_dim_nb;
format tenure 15. tenure_grp dep_wallet_grp $20.;

tenure = (2017-open_year)*12 + open_month;

	 if dep_wallet >= 0 	 and dep_wallet <= 500 	  then dep_wallet_grp = '01: 0-500';
else if dep_wallet >  500 	 and dep_wallet <= 5000   then dep_wallet_grp = '02: 500<-5k';
else if dep_wallet >  5000 	 and dep_wallet <= 10000  then dep_wallet_grp = '03: 5k<-10k';
else if dep_wallet >  10000  and dep_wallet <= 15000  then dep_wallet_grp = '04: 10k<-15k';
else if dep_wallet >  15000  and dep_wallet <= 25000  then dep_wallet_grp = '05: 15k<-25k';
else if dep_wallet >  25000  and dep_wallet <= 50000  then dep_wallet_grp = '06: 25k<-50k';
else if dep_wallet >  50000  and dep_wallet <= 75000  then dep_wallet_grp = '07: 50k<-75k';
else if dep_wallet >  75000  and dep_wallet <= 100000 then dep_wallet_grp = '08: 75k<-100k';
else if dep_wallet >  100000 and dep_wallet <= 250000 then dep_wallet_grp = '09: 100k<-250k';
else if dep_wallet >  250000 					 	  then dep_wallet_grp = '10: >250k';

	 if tenure <  0 	  			   then tenure_grp = '00: invalid';
else if tenure >= 0   and tenure < 12  then tenure_grp = '01: 0-<12 months';
else if tenure >= 12  and tenure < 18  then tenure_grp = '02: 12-<18 months';
else if tenure >= 18  and tenure < 24  then tenure_grp = '03: 18-<24 months';
else if tenure >= 24  and tenure < 30  then tenure_grp = '04: 24-<30 months';
else if tenure >= 30  and tenure < 36  then tenure_grp = '05: 30-<36 months';
else if tenure >= 36  and tenure < 48  then tenure_grp = '06: 36-<48 months';
else if tenure >= 48  and tenure < 54  then tenure_grp = '07: 48-<54 months';
else if tenure >= 54  and tenure < 60  then tenure_grp = '08: 54-<60 months';
else if tenure >= 60  and tenure < 72  then tenure_grp = '09: 5-<6 years';
else if tenure >= 72  and tenure < 84  then tenure_grp = '10: 6-<7 years';
else if tenure >= 84  and tenure < 96  then tenure_grp = '11: 7-<8 years';
else if tenure >= 96  and tenure < 108 then tenure_grp = '12: 8-<9 years';
else if tenure >= 108 and tenure < 120 then tenure_grp = '13: 9-<10 years';
else if tenure >= 120 and tenure < 144 then tenure_grp = '14: 10-<12 years';
else if tenure >= 144 and tenure < 156 then tenure_grp = '15: 12-<13 years';
else if tenure >= 156 and tenure < 180 then tenure_grp = '16: 13-<15 years';
else if tenure >= 180 and tenure < 216 then tenure_grp = '17: 15-<18 years';
else if tenure >= 216 and tenure < 252 then tenure_grp = '18: 18-<21 years';
else if tenure >= 252 and tenure < 300 then tenure_grp = '19: 21-<25 years';
else if tenure >= 300  				   then tenure_grp = '20: >=25 years';

if a;
run;


proc contents data = sb.cust_info_&yymm.; run;
proc univariate data = sb.cust_info_&yymm. noprint;
var tenure dep_wallet;
output out = pctl_out 
	   pctlpre = tenureP_ depwalletP_
	   pctlpts = 0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100
;
run;

data chk;
set sb.cust_info_&yymm.;
where tenure <= 0 ;
run;

/**********************************************************************************************/
/*identify targets*/
proc freq data = sb.pymt_all_by_cust_&yymm.;
table cnt_qp_mob / list missing;
where cnt_qp_mob > 0;
run;

proc means data = sb.pymt_all_by_cust_&yymm.(keep=cnt_qp_mob) 
		   n mean min p5 p10 p25 p50 p75 p90 p95 max;
where cnt_qp_mob > 0;
run;



%let keeplist = 
age_range
dep_ind
dda_ind
card_ind
mortgage_ind
auto_ind
tbc_wrapup_days
tbc_ivr_unique_days
tbcdays
tbcdays_liverep
tbcdays_ivr
branchdays
branchdays_pb
branchdays_teller
atmdays
cc_atm_cash_adv_days
cc_atm_pymnt_days
ltst_geo_mkt_nm
segment_cd
footprint
in_market
prim_bank_hhld
prsr_gndr_tx
mari_sts_cd
ocp_tx
tenure_grp
dep_wallet_grp
;


data sb.input;
merge 
	sb.pymt_all_by_cust_&yymm.(in=a keep=cust_dim_nb cnt_qp_mob)
	sb.cust_info_&yymm(in=b keep=cust_dim_nb &keeplist.)
	sb.enrl_by_cust_&yymm.
;
by cust_dim_nb;

	 if dda_ind = 'N' then depvar_qp = -1;
else if dda_ind = 'Y' and cnt_qp_mob = 0 then depvar_qp = 0;
else if dda_ind = 'Y' and cnt_qp_mob > 0 then depvar_qp = 1;

	 if dda_ind = 'N' then depvar_qpp = -1;
else if dda_ind = 'Y' and cnt_qp_mob = 0 then depvar_qpp = -1;
else if dda_ind = 'Y' and cnt_qp_mob > 0 and cnt_qp_mob <= 3 then depvar_qpp = 0;
else if dda_ind = 'Y' and cnt_qp_mob > 3 then depvar_qpp = 1;

days_tbc_all = input(tbcdays, $30.);
days_tbc_rep = input(tbcdays_liverep, $30.);
days_tbc_ivr = input(tbcdays_ivr, $30.);
days_branch = input(branchdays, $30.);
days_branch_pb = input(branchdays_pb, $30.);
days_branch_teller = input(branchdays_teller, $30.);
days_atm = input(atmdays, $30.);
days_atm_cash_adv = input(cc_atm_cash_adv_days, $30.);
days_atm_pymnt = input(cc_atm_pymnt_days, $30.);
foot_print = input(footprint, $2.);

if b then output;
run;
/*NOTE: The data set SB.INPUT has 74,224,109 observations and 44 variables.*/


/**********************************************************************************************/
/*input dataset for QP lookalikes*/
data sb.model sb.test;
set sb.input(where=(depvar_qp>=0));
ransel   = ranuni(246)*1000;
good = depvar_qp;

if ^missing(good) then do;
	if ransel <= 800 then output sb.model;
	else output sb.test;
end;
run;
/*NOTE: The data set SB.MODEL has 12,300,915 observations and 46 variables.*/
/*NOTE: The data set SB.TEST has 3,078,747 observations and 46 variables.*/


proc freq data = sb.model;
table good / list;
run;
proc freq data = sb.test;
table good / list;
run;
/*
good	Freq		Pct
0		9,999,319	81.29
1		2,301,596	18.71
				
good	Freq		Pct
0		2,502,521	81.28	
1		576,226		18.72
*/			



/**********************************************************************************************/
/*input dataset for QP Power Users*/
data sb.model2 sb.test2;
set sb.input(where=(depvar_qpp>=0));
ransel   = ranuni(246)*1000;
good = depvar_qpp;

if ^missing(good) then do;
	if ransel <= 800 then output sb.model2;
	else output sb.test2;
end;
run;
/*NOTE: The data set SB.MODEL2 has 2,301,072 observations and 46 variables.*/
/*NOTE: The data set SB.TEST2 has 576,750 observations and 46 variables.*/

proc freq data = sb.model2;
table good / list;
run;
proc freq data = sb.test2;
table good / list;
run;
/*
good	Freq		Pct
0		1,742,680	75.73
1		558,392		24.27
				
good	Freq		Pct
0		436,973		75.76
1		139,777		24.24
*/			



