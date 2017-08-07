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

/***********************************************************************************/
/*scoring the all customers with a DDA accounts*/
%macro dummy_char(var, value, ind);
if ( &var = &value ) then &var._&ind = 1; else &var._&ind =0;
label &var._&ind = &var._&ind. : &value;
%mend;

%macro dummy_num(var, low, high, ind);
if (&low <= &var <= &high) then &var._&ind = 1; else &var._&ind =0;
label &var._&ind = "&var._&ind. : &low - &high ";
%mend;

/**************************************QP lookalike*********************************/
data qp_active;
set sb.input(where=(dda_ind='Y'));

%dummy_num(days_atm, 0, 0, a);
%dummy_num(days_atm, 3, 31, c);
%dummy_char(card_ind, 'Y', b);
%dummy_char(tenure_grp, '01: 0-<12 months', a);
%dummy_char(tenure_grp, '02: 12-<18 months', b);
%dummy_char(tenure_grp, '03: 18-<24 months', c);
%dummy_char(tenure_grp, '04: 24-<30 months', d);
%dummy_char(tenure_grp, '05: 30-<36 months', e);
%dummy_char(tenure_grp, '06: 36-<48 months', f);
%dummy_char(tenure_grp, '07: 48-<54 months', g);
%dummy_char(tenure_grp, '08: 54-<60 months', h);
%dummy_char(tenure_grp, '09: 5-<6 years', i);
%dummy_char(tenure_grp, '10: 6-<7 years', j);
%dummy_char(tenure_grp, '11: 7-<8 years', k);
%dummy_char(tenure_grp, '12: 8-<9 years', l);
%dummy_char(tenure_grp, '17: 15-<18 years', q);
%dummy_char(tenure_grp, '18: 18-<21 years', r);
%dummy_char(tenure_grp, '19: 21-<25 years', s);
%dummy_char(tenure_grp, '20: >=25 years', t);
%dummy_num(days_tbc_rep, 1, 31, b);
%dummy_char(age_range, '1', a);
%dummy_char(age_range, '6', a);
%dummy_char(age_range, '7', b);
%dummy_char(dep_wallet_grp, '02: 500<-5k', b);
%dummy_char(dep_wallet_grp, '03: 5k<-10k', c);
%dummy_char(dep_wallet_grp, '09: 100k<-250k', i);
%dummy_char(dep_wallet_grp, '10: >250k', j);
%dummy_char(segment_cd, 'C00', a);
%dummy_char(segment_cd, 'C01', b);
%dummy_char(segment_cd, 'C02', c);
%dummy_char(segment_cd, 'C03', d);
%dummy_char(segment_cd, 'CP4', d);
%dummy_char(prim_bank_hhld, 'Y', b);
%dummy_num(days_branch_teller, 1, 31, b);
%dummy_char(ltst_geo_mkt_nm, 'Central Indiana', a);
%dummy_char(ltst_geo_mkt_nm, 'Chicago', b);
%dummy_char(ltst_geo_mkt_nm, 'Northeast', c);

logit_qp_active = 
-2.714901204
+ (-0.182001525 * days_atm_a)
+ (0.126212252 * days_atm_c)
+ (-0.800300749 * card_ind_b)
+ (0.202115111 * tenure_grp_a)
+ (0.323773477 * tenure_grp_b)
+ (0.442626562 * tenure_grp_c)
+ (0.30517736 * tenure_grp_d)
+ (0.379651605 * tenure_grp_e)
+ (0.292776565 * tenure_grp_f)
+ (0.227855533 * tenure_grp_g)
+ (0.259537497 * tenure_grp_h)
+ (0.2192266 * tenure_grp_i)
+ (0.180159513 * tenure_grp_j)
+ (0.154251453 * tenure_grp_k)
+ (0.10952323 * tenure_grp_l)
+ (-0.154814465 * tenure_grp_q)
+ (-0.305658591 * tenure_grp_r)
+ (-0.544171324 * tenure_grp_s)
+ (-1.052504095 * tenure_grp_t)
+ (0.234508741 * days_tbc_rep_b)
+ (1.071369711 * age_range_a)

+ (0.849942601 * age_range_b)
+ (0.019002499 * dep_wallet_grp_b)
+ (0.026398056 * dep_wallet_grp_c)
+ (-0.072864609 * dep_wallet_grp_i)
+ (-0.055355388 * dep_wallet_grp_j)
+ (-1.768292484 * segment_cd_a)
+ (0.280962043 * segment_cd_b)
+ (0.162313294 * segment_cd_c)
+ (-0.180805183 * segment_cd_d)

+ (0.934484704 * prim_bank_hhld_b)
+ (-0.280532611 * days_branch_teller_b)
+ (0.422223995 * ltst_geo_mkt_nm_a)
+ (1.112914362 * ltst_geo_mkt_nm_b)
+ (0.587162355 * ltst_geo_mkt_nm_c)
;

prob_qp_active = 1/(1+exp((-1)*logit_qp_active));

run;





%let data = qp_active;
%let pred = prob_qp_active;
%let decl = rank_qp_active;

proc univariate data = &data. noprint;
format _all_;
var &pred.;
output out = pctl pctlpre = pctl
pctlpts = 5 to 100 by 5;
run;

proc transpose data = pctl out = pctl_out; run;
data _null_;
set pctl_out;
call symput(_name_, col1);
run;


data &data. ;
set &data. ;

	if &pred. >= &pctl95 then &decl. = 1;
	else if &pred. >= &pctl90 then &decl. = 2;
	else if &pred. >= &pctl85 then &decl. = 3;
	else if &pred. >= &pctl80 then &decl. = 4;
	else if &pred. >= &pctl75 then &decl. = 5;
	else if &pred. >= &pctl70 then &decl. = 6;
	else if &pred. >= &pctl65 then &decl. = 7;
	else if &pred. >= &pctl60 then &decl. = 8;
	else if &pred. >= &pctl55 then &decl. = 9;
	else if &pred. >= &pctl50 then &decl. = 10;
	else if &pred. >= &pctl45 then &decl. = 11;
	else if &pred. >= &pctl40 then &decl. = 12;
	else if &pred. >= &pctl35 then &decl. = 13;
	else if &pred. >= &pctl30 then &decl. = 14;
	else if &pred. >= &pctl25 then &decl. = 15;
	else if &pred. >= &pctl20 then &decl. = 16;
	else if &pred. >= &pctl15 then &decl. = 17;
	else if &pred. >= &pctl10 then &decl. = 18;
	else if &pred. >= &pctl5 then &decl. = 19;
	else &decl. = 20;
run;



/**************************************QP power user*********************************/
data qp_power;
set qp_active;
%dummy_num(days_atm, 0, 0, a);
%dummy_num(days_atm, 5, 6, d);
%dummy_num(days_atm, 7, 31, e);
%dummy_num(days_tbc_all, 0, 0, a);
%dummy_num(days_tbc_all, 2, 31, c);
%dummy_num(days_branch, 2, 31, a);
%dummy_char(segment_cd, 'C01', a);
%dummy_char(age_range, '6', a);
%dummy_char(age_range, 'A', b);
%dummy_char(in_market, 'OUT', a);
%dummy_char(dep_wallet_grp, '02: 500<-5k', a);
%dummy_char(tenure_grp, '02: 12-<18 months', a);
%dummy_char(tenure_grp, '03: 18-<24 months', b);
%dummy_char(tenure_grp, '04: 24-<30 months', c);
%dummy_char(card_ind, 'N', a);
%dummy_char(prim_bank_hhld, 'N', a);
%dummy_char(ltst_geo_mkt_nm, 'Chicago', a);
%dummy_char(ltst_geo_mkt_nm, 'Northeast', b);
%dummy_char(ltst_geo_mkt_nm, 'Upstate NY', d);

logit_qp_power = 
-1.443248304
+ (-0.0333338467264725 * days_atm_a)
+ (0.171856459650128 * days_atm_d)
+ (0.33910422081926 * days_atm_e)
+ (-0.131912643391092 * days_tbc_all_a)
+ (0.0615257631606693 * days_tbc_all_c)
+ (0.0622828296702955 * days_branch_a)
+ (0.155367766058297 * segment_cd_a)
+ (0.185707128752422 * age_range_a)
+ (-0.152809656448341 * age_range_b)
+ (-0.227838199764973 * in_market_a)
+ (0.0351372303292049 * dep_wallet_grp_a)
+ (0.100951996011213 * tenure_grp_a)
+ (0.18103088836733 * tenure_grp_b)
+ (0.0956562058121876 * tenure_grp_c)
+ (0.241660551861831 * card_ind_a)
+ (-1.07508835765158 * prim_bank_hhld_a)
+ (0.274500108733168 * ltst_geo_mkt_nm_a)
+ (0.25210727365426 * ltst_geo_mkt_nm_b)
+ (0.254870484669751 * ltst_geo_mkt_nm_d)
;


prob_qp_power = 1/(1+exp((-1)*logit_qp_power));

run;


%let data = qp_power;
%let pred = prob_qp_power;
%let decl = rank_qp_power;

proc univariate data = &data. noprint;
format _all_;
var &pred.;
output out = pctl pctlpre = pctl
pctlpts = 5 to 100 by 5;
run;

proc transpose data = pctl out = pctl_out; run;
data _null_;
set pctl_out;
call symput(_name_, col1);
run;


data &data. ;
set &data. ;

	if &pred. >= &pctl95 then &decl. = 1;
	else if &pred. >= &pctl90 then &decl. = 2;
	else if &pred. >= &pctl85 then &decl. = 3;
	else if &pred. >= &pctl80 then &decl. = 4;
	else if &pred. >= &pctl75 then &decl. = 5;
	else if &pred. >= &pctl70 then &decl. = 6;
	else if &pred. >= &pctl65 then &decl. = 7;
	else if &pred. >= &pctl60 then &decl. = 8;
	else if &pred. >= &pctl55 then &decl. = 9;
	else if &pred. >= &pctl50 then &decl. = 10;
	else if &pred. >= &pctl45 then &decl. = 11;
	else if &pred. >= &pctl40 then &decl. = 12;
	else if &pred. >= &pctl35 then &decl. = 13;
	else if &pred. >= &pctl30 then &decl. = 14;
	else if &pred. >= &pctl25 then &decl. = 15;
	else if &pred. >= &pctl20 then &decl. = 16;
	else if &pred. >= &pctl15 then &decl. = 17;
	else if &pred. >= &pctl10 then &decl. = 18;
	else if &pred. >= &pctl5 then &decl. = 19;
	else &decl. = 20;
run;


/************************************save output dataset****************************************/
data sb.chk_dda_customers;
set qp_power;
keep
	cust_dim_nb
	ind_qd_enrl
	ind_qp_enrl
	ind_mbp_enrl
	cnt_qp_mob
	dep_ind
	dda_ind
	card_ind
	mortgage_ind
	auto_ind
	age_range
	ltst_geo_mkt_nm
	prsr_gndr_tx
	mari_sts_cd
	ocp_tx
	segment_cd
	in_market
	prim_bank_hhld
	tenure_grp
	dep_wallet_grp
	depvar_qp
	depvar_qpp
	days_tbc_all
	days_tbc_rep
	days_tbc_ivr
	days_branch
	days_branch_pb
	days_branch_teller
	days_atm
	logit_qp_active
	prob_qp_active
	rank_qp_active
	logit_qp_power
	prob_qp_power
	rank_qp_power
;

if cnt_qp_mob =. then cnt_qp_mob = 0;
if ind_qd_enrl =. then ind_qd_enrl = 0;
if ind_qp_enrl =. then ind_qp_enrl = 0;
if ind_mbp_enrl =. then ind_mbp_enrl = 0;
if depvar_qp < 1 then depvar_qp = 0;
if depvar_qpp < 1 then depvar_qpp = 0;
if age_range = '?' then age_range = '';
if mari_sts_cd = '?' then mari_sts_cd = '';
if ocp_tx = '?' then ocp_tx = '';
if ltst_geo_mkt_nm = '?' then ltst_geo_mkt_nm = '';
run;











