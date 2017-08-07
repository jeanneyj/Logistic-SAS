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

/*define input dataset*/
%let indata = sb.model;
%let holdout = sb.test;

%let mdl_desc = QuickPay Lookalike;
/*define dependent and independent variables*/
%let depvar = depvar_qp;
%let outdir = &datadir. ;


/*final list of input variables*/
%macro final_var;
days_atm_a
days_atm_c
card_ind_b
tenure_grp_a
tenure_grp_b
tenure_grp_c
tenure_grp_d
tenure_grp_e
tenure_grp_f
tenure_grp_g
tenure_grp_h
tenure_grp_i
tenure_grp_j
tenure_grp_k
tenure_grp_l
tenure_grp_q
tenure_grp_r
tenure_grp_s
tenure_grp_t
days_tbc_rep_b
age_range_a
age_range_b
dep_wallet_grp_b
dep_wallet_grp_c
dep_wallet_grp_i
dep_wallet_grp_j
segment_cd_a
segment_cd_b
segment_cd_c
segment_cd_d
prim_bank_hhld_b
days_branch_teller_b
ltst_geo_mkt_nm_a
ltst_geo_mkt_nm_b
ltst_geo_mkt_nm_c

%mend final_var;


/*logistic regression*/
ods output parameterestimates =_pe oddsratios =_or; 
ods graphics on;
proc logistic 
	descending 
	data = &indata.
	namelen = 100
	outest = est_out 
	outmodel = mdl_detail 
	plots(only maxpoints=none)=(roc(id=obs));

	pred: 
		model &depvar = %final_var /  
		ctable 
		pprob=(.05 to 1.0 by .05)
		selection = none
/*		selection = stepwise */
/*		slentry = 0.999 slstay = 0.9995 lackfit fast*/
		parmlabel;

	*weight tdwgt;
	output out = mdl_out p = p_1;
	score data = &indata. out = input_scored ;
	score data = &holdout. out = holdout_scored ;
run;
ods graphics off;
ods output close;


/*scorecard*/

/*calculate the actual correlation btw depedent and input variables*/
proc corr data = input_scored noprob outp=corrs noprint;
with &depvar;
var %final_var;
run;

proc transpose data = corrs
				out = corr_list(keep=_name_ corr); 
id _type_;
run;

/*and... check if any wrong sign in estimates*/
proc sql;
create table sb.scorecard as 
select 
	a.*, 
	b.oddsratioest,
	b.lowercl,
	b.uppercl,
	c.corr,
	case when (a.variable ^= 'intercept' and a.estimate*c.corr < 0) then 'x' else '' end as sign

	from _pe as a
	left join _or as b on b.effect = a.variable
	left join corr_list as c on c._name_ = a.variable;
quit;



ods listing close;
ods csvall file = "&outdir./scorecard.csv";

proc print data = sb.scorecard ; 
title "scorecard - model : &mdl_desc.";
run;

ods csvall close;
ods listing;




/***********************************************validation********************************************************/

/*define dependent and independent variables*/
%macro getKS(dat, depvar, pred);

proc univariate data = &dat noprint;
format _all_;
var &pred;
output out = pctl pctlpre = pctl
pctlpts = 5 to 100 by 5;
*weight tdwgt;
run;

proc transpose data = pctl out = pctl_out; run;
data _null_;
set pctl_out;
call symput(_name_, col1);
run;


data out;
set &dat ;

	if &pred >= &pctl95 then decile = 1;
	else if &pred >= &pctl90 then decile = 2;
	else if &pred >= &pctl85 then decile = 3;
	else if &pred >= &pctl80 then decile = 4;
	else if &pred >= &pctl75 then decile = 5;
	else if &pred >= &pctl70 then decile = 6;
	else if &pred >= &pctl65 then decile = 7;
	else if &pred >= &pctl60 then decile = 8;
	else if &pred >= &pctl55 then decile = 9;
	else if &pred >= &pctl50 then decile = 10;
	else if &pred >= &pctl45 then decile = 11;
	else if &pred >= &pctl40 then decile = 12;
	else if &pred >= &pctl35 then decile = 13;
	else if &pred >= &pctl30 then decile = 14;
	else if &pred >= &pctl25 then decile = 15;
	else if &pred >= &pctl20 then decile = 16;
	else if &pred >= &pctl15 then decile = 17;
	else if &pred >= &pctl10 then decile = 18;
	else if &pred >= &pctl5 then decile = 19;
	else decile = 20;
run;

proc sql;
select count(*), sum(&depvar) into :total, :good from out;
create table rank as
select 
	put(decile, 5.) as rank,
	min(&pred) as low,
	max(&pred) as high,
	count(*) as cnt,
	calculated cnt / &total as pct,
	sum(&depvar) as cnt_good,
	calculated cnt_good / &good as pct_good,
	calculated cnt - calculated cnt_good as cnt_bad,
	calculated cnt_bad / (&total - &good) as pct_bad,
	calculated cnt_good / calculated cnt as resp_rate,
	round(calculated resp_rate / (&good/&total)*100, 1) as lift

	from out
	group by rank
	order by rank;
quit;

data &dat._out;
retain maxKS;
set rank end=eof;
obs = _n_;

cum_pct + pct;
cum_pct_good + pct_good;
cum_pct_bad + pct_bad;
KS = round(cum_pct_good - cum_pct_bad, 0.0000000001);
if KS > maxKS then maxKS = KS;
cum_resp_rate = (cum_pct_good*&good) / (cum_pct*&total);

if obs = 1 then do;
	cum_pct = pct;
	cum_pct_good = pct_good;
	cum_pct_bad = pct_bad;
	maxKS = KS;
end;

output;

if eof then do;
	rank = 'TOTAL';
	cnt = &total;
	cnt_good = &good;
	cnt_bad = &total - &good;
	resp_rate = &good/&total;
	KS = maxKS;
	low = .;
	high = .;
	pct = .;
	cum_pct = .;
	pct_good = .;
	cum_pct_good = .;
	pct_bad = .;
	cum_pct_bad = .;
	cum_resp_rate = .;
	lift = .;
	output;
end;
run;

%mend getKS;


data all_scored;
set input_scored holdout_scored;
keep &depvar p_1;
run;

%getKS(input_scored, &depvar., p_1);
%getKS(holdout_scored, &depvar., p_1);
%getKS(all_scored, &depvar., p_1);


data sb.KS;
format 
	Sample $20.	rank $5. low high 12.10 
	cnt comma9. pct cum_pct percent12.6
	cnt_good comma9. pct_good cum_pct_good percent12.6
	cnt_bad comma9. pct_bad cum_pct_bad percent12.6
	resp_rate cum_resp_rate percent12.6 
	lift 3. KS percent10.4;

set all_scored_out(in=a)
	input_scored_out(in=b)
	holdout_scored_out(in=c) end=eof;

if a then Sample = 'Overall';
if b then Sample = 'Modeling';
if c then Sample = 'Holdout';

drop maxKS obs;
output;
if eof then do;
	cnt = .; cnt_good = .; cnt_bad = .; resp_rate = .; KS = .; cum_resp_rate = .;
	cum_pct_good = 0; cum_pct_bad = 0;
	rank = put(0, 5.); 
	Sample = 'Overall'; output;
	Sample = 'Modeling'; output;
	Sample = 'Holdout'; output;
	Sample = 'Random';
	cum_pct_good = 0; cum_pct_bad = 0; output;
	cum_pct_good = 0.05; cum_pct_bad = 0.05; output;
	cum_pct_good = 0.10; cum_pct_bad = 0.10; output;
	cum_pct_good = 0.15; cum_pct_bad = 0.15; output;
	cum_pct_good = 0.20; cum_pct_bad = 0.20; output;
	cum_pct_good = 0.25; cum_pct_bad = 0.25; output;
	cum_pct_good = 0.30; cum_pct_bad = 0.30; output;
	cum_pct_good = 0.35; cum_pct_bad = 0.35; output;
	cum_pct_good = 0.40; cum_pct_bad = 0.40; output;
	cum_pct_good = 0.45; cum_pct_bad = 0.45; output;
	cum_pct_good = 0.50; cum_pct_bad = 0.50; output;
	cum_pct_good = 0.55; cum_pct_bad = 0.55; output;
	cum_pct_good = 0.60; cum_pct_bad = 0.60; output;
	cum_pct_good = 0.65; cum_pct_bad = 0.65; output;
	cum_pct_good = 0.70; cum_pct_bad = 0.70; output;
	cum_pct_good = 0.75; cum_pct_bad = 0.75; output;
	cum_pct_good = 0.80; cum_pct_bad = 0.80; output;
	cum_pct_good = 0.85; cum_pct_bad = 0.85; output;
	cum_pct_good = 0.90; cum_pct_bad = 0.90; output;
	cum_pct_good = 0.95; cum_pct_bad = 0.95; output;
	cum_pct_good = 1.00; cum_pct_bad = 1.00; output;
end;
run;
proc sort data = sb.KS; by descending Sample Rank; run;


	goptions reset=all border;
	title "ROC Graph - Model : &mdl_desc.";

	proc gplot data = sb.KS(where=(rank^='TOTAL'));

	symbol1 interpol=line width=3 value=triangle c=steelblue;
	symbol2 interpol=line width=2 value=circle c=indigo;
	symbol3 interpol=line width=1 value=square c=orchid;
	symbol4 interpol=line width=1 value=star c=black;


	axis1 label=("% of Cummulative Non-Targets")
	      order = 0 to 1 by 0.2;
	axis2 label=(angle=90 "% of Cummulative Targets")
	      order = 0 to 1 by 0.2;

	legend1 label=("Sample") position=(inside bottom right) order=descending across=1 mode=protect;

	format cum_pct_good cum_pct_bad percent6.;

	plot cum_pct_good*cum_pct_bad = Sample / haxis=axis1 vaxis=axis2 legend=legend1;

	run;
	quit;


	ods listing close;
	ods csvall file = "&outdir./ks.csv";
		proc print data = sb.ks;
		where rank ^= put(0, 5.) and sample ^= 'random';
		title "gain table - model : &mdl_desc.";
		run;
	ods csvall close;
	ods listing;






       



