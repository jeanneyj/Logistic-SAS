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



/************************************ crosstab ***************************************************/
/*define input variables*/
%let varlist = 
age_range
dep_ind
dda_ind
card_ind
mortgage_ind
auto_ind
ltst_geo_mkt_nm
segment_cd
foot_print
in_market
prim_bank_hhld
prsr_gndr_tx
mari_sts_cd
ocp_tx
tenure_grp
dep_wallet_grp
days_tbc_all
days_tbc_rep
days_tbc_ivr
days_branch
days_branch_pb
days_branch_teller
days_atm
days_atm_cash_adv
days_atm_pymnt
;


/*define dependent variables*/
%let depvar = depvar_qp;

/*define input dataset*/
%let indata = sb.model;
%let holdout = sb.test;


/*save total & all_good counts in macro variables*/
proc sql;
select count(*) into :total from &indata. where &depvar >= 0;
select count(*) into :all_good from &indata. where &depvar = 1;
quit;
%put &total &all_good;



/*create macro variables for all inputs*/
proc contents data = &indata.(keep=&varlist)
              out  = allvar(keep=name type label) noprint;
run;

data _null_;
set allvar nobs=nchar;
call symput('char'||left(_n_), name);
call symput('nchar', nchar);
run;


/*iniate empty tables for output*/
proc sql;
create table chk_char 
(var char(30), value char(50), count num, obspct num, good num, bad num, goodpct num, badpct num, index num);
quit;

proc sql;
create table entropy_char 
(var char(30), importance num);
quit;



/*calculation of importance and xtab for all variables*/
%macro calcchar(var);
proc sql;

	insert into chk_char
		select 
			put("&var.", $30.) as var,
			&var. as value,
			count(*) as count format=comma9.,
			calculated count / &total as obspct,
			sum(good) as good,
			calculated count - calculated good as bad,
			calculated good / calculated count as goodpct,
			calculated bad / calculated count as badpct,
			round(calculated goodpct/(&all_good/&total)*100, 1) as index
		from &indata.
		group by 1, 2
	;

	insert into entropy_char
	select 
		put("&var.", $30.) as var,
		-log(sum(numer_unit)/sum(bsum) + 0.00000000000000000001) as importance
		from (
				select 
					t1.&var, 
					t1.&depvar, 
					count(*) as count,
					t2.bsum,
					- calculated count * log(calculated count/t2.bsum) as numer_unit

					from &indata. as t1
					inner join (select &var, count(*) as bsum from &indata. group by &var) as t2 on t1.&var = t2.&var
					group by 1, 2
				);
quit;
%mend calcchar;

/*call a do-loop macro to run through all variables*/
%macro allvar;
%do i=1 %to &nchar;
	%calcchar(&&char&i.);
%end;
%mend allvar;

%allvar;


/*pull xtab to calculate more stats: ks, woe, zscore*/
proc sort data = entropy_char; by var ;
proc sort data = chk_char; by var value ;
data char;
merge entropy_char chk_char;
by var; 

if obspct not in (1,0) then 
zscore = (goodpct-obspct)/sqrt(obspct*(1-obspct))*(1/&all_good. + 1/&total.);

goodcum+good;
badcum+bad;
obscum+count;
if first.var then do;
	goodcum = good;
	badcum = bad;
	obscum = count;
end;

goodpcum = goodcum/&all_good; 
badpcum = badcum/(&total - &all_good);
obspcum = obscum/&total;

ks = abs(goodpcum-badpcum);
if goodpct ^= 0 and badpct ^= 0 then do;
	woe = log(goodpct/badpct)*100;
	infoval = ((goodpct-badpct)*log(goodpct/badpct))*100;
end;

cumrate = goodcum/obscum;
cumindex = round(cumrate/(&all_good/&total)*100, 1);

keep var importance value count obspct good bad goodpct index zscore ks woe infoval cumrate cumindex;
run;

proc sort data = char; by descending importance var value; run;

/*print out results*/
ods listing close;
ods csvall file = "&datadir./xtab.csv";
	proc print data = char; run;
ods csvall close;
ods listing;



/************************************ binning ***************************************************/
%macro dummy_char(var, value, ind);
if ( &var = &value ) then &var._&ind = 1; else &var._&ind =0;
label &var._&ind = &var._&ind. : &value;
%mend;

%macro dummy_num(var, low, high, ind);
if (&low <= &var <= &high) then &var._&ind = 1; else &var._&ind =0;
label &var._&ind = "&var._&ind. : &low - &high ";
%mend;

data &indata. ;
set &indata. ;
%dummy_num(days_atm, 0, 0, a);
%dummy_num(days_atm, 1, 2, b);
%dummy_num(days_atm, 3, 31, c);
%dummy_char(card_ind, 'N', a);
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
%dummy_char(tenure_grp, '13: 9-<10 years', m);
%dummy_char(tenure_grp, '14: 10-<12 years', n);
%dummy_char(tenure_grp, '15: 12-<13 years', o);
%dummy_char(tenure_grp, '16: 13-<15 years', p);
%dummy_char(tenure_grp, '17: 15-<18 years', q);
%dummy_char(tenure_grp, '18: 18-<21 years', r);
%dummy_char(tenure_grp, '19: 21-<25 years', s);
%dummy_char(tenure_grp, '20: >=25 years', t);
%dummy_num(days_tbc_rep, 0, 0, a);
%dummy_num(days_tbc_rep, 1, 31, b);
%dummy_char(age_range, '1', a);
%dummy_char(age_range, '6', a);
%dummy_char(age_range, '7', b);
%dummy_char(age_range, '8', c);
%dummy_char(age_range, '9', d);
%dummy_char(age_range, '?', d);
%dummy_char(age_range, 'A', d);
%dummy_char(age_range, '', d);
%dummy_num(days_tbc_all, 0, 0, a);
%dummy_num(days_tbc_all, 1, 31, b);
%dummy_num(days_tbc_ivr, 0, 0, a);
%dummy_num(days_tbc_ivr, 1, 31, b);
%dummy_char(dep_wallet_grp, '01: 0-500', a);
%dummy_char(dep_wallet_grp, '02: 500<-5k', b);
%dummy_char(dep_wallet_grp, '03: 5k<-10k', c);
%dummy_char(dep_wallet_grp, '04: 10k<-15k', d);
%dummy_char(dep_wallet_grp, '05: 15k<-25k', e);
%dummy_char(dep_wallet_grp, '06: 25k<-50k', f);
%dummy_char(dep_wallet_grp, '07: 50k<-75k', g);
%dummy_char(dep_wallet_grp, '08: 75k<-100k', h);
%dummy_char(dep_wallet_grp, '09: 100k<-250k', i);
%dummy_char(dep_wallet_grp, '10: >250k', j);
%dummy_char(segment_cd, 'C00', a);
%dummy_char(segment_cd, 'C01', b);
%dummy_char(segment_cd, 'C02', c);
%dummy_char(segment_cd, 'C03', d);
%dummy_char(segment_cd, 'CP4', d);
%dummy_num(days_branch, 0, 0, a);
%dummy_num(days_branch, 1, 31, b);
%dummy_char(prim_bank_hhld, 'N', a);
%dummy_char(prim_bank_hhld, 'Y', b);
%dummy_num(days_branch_teller, 0, 0, a);
%dummy_num(days_branch_teller, 1, 31, b);
%dummy_char(ltst_geo_mkt_nm, 'Central Indiana', a);
%dummy_char(ltst_geo_mkt_nm, 'Chicago', b);
%dummy_char(ltst_geo_mkt_nm, 'Northeast', c);

run;




data &holdout. ;
set &holdout. ;
%dummy_num(days_atm, 0, 0, a);
%dummy_num(days_atm, 1, 2, b);
%dummy_num(days_atm, 3, 31, c);
%dummy_char(card_ind, 'N', a);
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
%dummy_char(tenure_grp, '13: 9-<10 years', m);
%dummy_char(tenure_grp, '14: 10-<12 years', n);
%dummy_char(tenure_grp, '15: 12-<13 years', o);
%dummy_char(tenure_grp, '16: 13-<15 years', p);
%dummy_char(tenure_grp, '17: 15-<18 years', q);
%dummy_char(tenure_grp, '18: 18-<21 years', r);
%dummy_char(tenure_grp, '19: 21-<25 years', s);
%dummy_char(tenure_grp, '20: >=25 years', t);
%dummy_num(days_tbc_rep, 0, 0, a);
%dummy_num(days_tbc_rep, 1, 31, b);
%dummy_char(age_range, '1', a);
%dummy_char(age_range, '6', a);
%dummy_char(age_range, '7', b);
%dummy_char(age_range, '8', c);
%dummy_char(age_range, '9', d);
%dummy_char(age_range, '?', d);
%dummy_char(age_range, 'A', d);
%dummy_char(age_range, '', d);
%dummy_num(days_tbc_all, 0, 0, a);
%dummy_num(days_tbc_all, 1, 31, b);
%dummy_num(days_tbc_ivr, 0, 0, a);
%dummy_num(days_tbc_ivr, 1, 31, b);
%dummy_char(dep_wallet_grp, '01: 0-500', a);
%dummy_char(dep_wallet_grp, '02: 500<-5k', b);
%dummy_char(dep_wallet_grp, '03: 5k<-10k', c);
%dummy_char(dep_wallet_grp, '04: 10k<-15k', d);
%dummy_char(dep_wallet_grp, '05: 15k<-25k', e);
%dummy_char(dep_wallet_grp, '06: 25k<-50k', f);
%dummy_char(dep_wallet_grp, '07: 50k<-75k', g);
%dummy_char(dep_wallet_grp, '08: 75k<-100k', h);
%dummy_char(dep_wallet_grp, '09: 100k<-250k', i);
%dummy_char(dep_wallet_grp, '10: >250k', j);
%dummy_char(segment_cd, 'C00', a);
%dummy_char(segment_cd, 'C01', b);
%dummy_char(segment_cd, 'C02', c);
%dummy_char(segment_cd, 'C03', d);
%dummy_char(segment_cd, 'CP4', d);
%dummy_num(days_branch, 0, 0, a);
%dummy_num(days_branch, 1, 31, b);
%dummy_char(prim_bank_hhld, 'N', a);
%dummy_char(prim_bank_hhld, 'Y', b);
%dummy_num(days_branch_teller, 0, 0, a);
%dummy_num(days_branch_teller, 1, 31, b);
%dummy_char(ltst_geo_mkt_nm, 'Central Indiana', a);
%dummy_char(ltst_geo_mkt_nm, 'Chicago', b);
%dummy_char(ltst_geo_mkt_nm, 'Northeast', c);

run;

