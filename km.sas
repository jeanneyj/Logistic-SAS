options compress = yes;
options mprint mlogic details source source2 symbolgen;
%let dmgdir = /gpfs_nonhsm/sasji01rptga/u04/data/cig_ebi/dmg;
%let sandbox = /u03/data/cig_ebi/sandbox/i637308;
%let project = research/growth;

%let codedir = &sandbox./code/&project;
%let datadir = &sandbox./data/&project;
libname codedir "&codedir.";
libname datadir "&datadir.";
libname psnap "&dmgdir./prod/data/L3/enterprise/onlineusage";
libname msnap "&dmgdir./prod/data/L1/mob/mobileusage";

/*run cluster analysis with extracted factors*/

%let login_var =
day_active
sum_login
sum_touch
sum_preview
freq_per_day
sum_all
;


/****************************************EXTRACTING FACTORS****************************************/
data datadir.input;
set datadir.txn_mob_metrics_2015q4;

array chg &login_var. ;
do over chg;
	if chg=. then chg=0;
end;

keep prof_id cust_dim_nb &login_var. ;
*ransel  = ranuni(246)*1000;
*if ransel <= 100 then output input;
run;




/*2 factors*/
proc factor data = datadir.input 
			nfactors = 2  
			rotate = varimax 
			method = principal 
			mineigen = 0
			out = datadir.fact_out
			outstat = datadir.stat_2fct
			corr scree ev reorder score;
var day_active sum_login sum_touch sum_preview sum_all freq_per_day;
run;
/*
Factor1 -- days & logins & preview
Factor2 -- touch
*/
data datadir.fact_out;
merge 
	datadir.fact_out(in=a) 
	single_factor(in=b keep=prof_id Factor1 rename=(Factor1=single_factor))
;
by prof_id;
run;




/****************************************CLUSTER W/ 2 FACTORS****************************************/

/*1st run*/
/*Preliminary Clustering*/
proc fastclus data = datadir.fact_out
			  outseed = mean1 
			  maxc = 20
			  converge = 0
			  out = out1
			  summary;
var Factor1 Factor2 ;
run;


/*2nd run*/
/*remove low frequency clusters and create inital seeds*/
/*minimize the mean absolute difference between the data and the corresponding cluster medians*/
data datadir.seed;
set mean1;
if _freq_ > 100;
run;
proc fastclus data = datadir.fact_out
			  seed = datadir.seed
			  outseed = mean2
			  maxc = 6
			  least = 1 
			  out = out2
			  converge = 0;
var Factor1 Factor2 ;
run;


/*3rd run*/
/*prevent outliers from assigning a cluster*/
proc fastclus data = datadir.fact_out
			  seed = mean2
			  outseed = datadir.mean_2fct
			  maxc = 6
			  strict = 5.0 
			  out = datadir.out_2fct;
var Factor1 Factor2 ;
run;

proc means data = datadir.out_2fct
n mean std min p1 p5 p10 q1 median q3 p90 p95 p99 max;
var Factor1 Factor2 &login_var. ;
class cluster;
run;


data plot;
set datadir.out_2fct;
var1 = 0.954863917*Factor1 + (-0.2970436)*Factor2;
var2 = 0.2970435995*Factor1 + 0.954863917*Factor2;

ransel  = ranuni(246)*10000;
if ransel <= 10 then output;
run;

data myattrmap;
informat id $4. value $1. markercolor $20. ;
input id $ value $ markercolor $ ;
datalines;
myid 1 lightblue
myid 2 orange
myid 3 mediumgrey
myid 4 darkred
myid 5 darkgrayishblue
; 
proc sgplot data = plot dattrmap = myattrmap ;
scatter y = var1 x = var2 / group = cluster attrid = myid;
yaxis label = 'Factor 1 : Login Frequency';
xaxis label = 'Factor 2 : Touch ID';
refline 0 / axis = y;
refline 0 / axis = x;
run;



/*compare with grouping by simple cutoffs*/
data datadir.validate;
set datadir.out_2fct;
format segment new_to_mob LOB $50. ;

if freq_per_day > 10 then segment = 'likely aggregator';
else if day_active = 1  or sum_all = 1  then segment = 'Very Light';
else if day_active < 4  or sum_all < 5  then segment = 'Light';
else if day_active < 15 or sum_all < 19 then segment = 'Moderate';
else if day_active < 38 or sum_all < 54 then segment = 'Heavy';
else segment = 'Addicted';

	 if first_mob_login_dt >= '01OCT2015'd then new_to_mob = 'first mob in 2015Q4';
else if first_mob_login_dt >= '01JUL2015'd then new_to_mob = 'first mob in 2015Q3';
else if first_mob_login_dt >= '01JAN2015'd then new_to_mob = 'first mob in 2015';
else new_to_mob = 'first mob before 2015';

if missing(dep_ind) or missing(crd_ind) or missing(hom_ind) or missing(aut_ind) then LOB = 'missing';
else if dep_ind = 'Y' and crd_ind = 'N' and hom_ind = 'N' and aut_ind = 'N' then LOB = 'deposit only';
else if dep_ind = 'N' and crd_ind = 'Y' and hom_ind = 'N' and aut_ind = 'N' then LOB = 'card only';
else if dep_ind = 'N' and crd_ind = 'N' and hom_ind = 'Y' and aut_ind = 'N' then LOB = 'mortgage only';
else if dep_ind = 'N' and crd_ind = 'N' and hom_ind = 'N' and aut_ind = 'Y' then LOB = 'auto only';
else if dep_ind = 'Y' and crd_ind = 'Y' and hom_ind = 'N' and aut_ind = 'N' then LOB = 'deposit & card';
else LOB = 'other';
run;

proc freq data = datadir.validate;
*table segment*cluster / norow nocol nopercent nocum missing;
table chk / list missing;
run;




















