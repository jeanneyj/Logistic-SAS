options compress = yes;
options mprint mlogic details source source2 symbolgen;
%let dmgdir = /gpfs_nonhsm/sasji01rptga/u04/data/cig_ebi/dmg;
%let sandbox = /u03/data/cig_ebi/sandbox/i637308;
%let project = research/growth/followup;

%let codedir = &sandbox./code/&project;
%let datadir = &sandbox./data/&project;
libname codedir "&codedir.";
libname datadir "&datadir.";
libname psnap "&dmgdir./prod/data/L3/enterprise/onlineusage";
libname msnap "&dmgdir./prod/data/L1/mob/mobileusage";

/*
mobile channel macro variables 
- iPhone, Private Banking iPhone, iPad, 
- Private Banking iPad, Android, Private Banking android, 
- enhanced mobile browser, mobile web browswer, Windows, Blackberry, 
- iPhone Freedom Pay, Android Freedom Pay 
*/
%let mob_chnl  = 'MON','PBN','MOP','PBP','MOD','PBD','MOE','MWB','MCW','BRY','MWD','MWN' ;
%let enrl_chnl = 'MON','PBN','MOP','PBP','MOD','PBD' ;

%let iPad    = 'MOP','PBP','MWP' ; 
%let iPhone  = 'MON','PBN','MWN' ; 
%let Android = 'MOD','PBD','MWD' ; 


data datadir.input;
format date yymmdd10. ;
set datadir.by_chnl;
date = intnx('month', '01DEC2012'd, _n_, 'end');
run;


/********iPhone users UCM pre 01/2015********/
ODS GRAPHICS ON ;

proc ucm data = datadir.input printall ;
id date interval = month ;
model cnt_iPhone ;

irregular variance = 0 noest ; /*stepwise 1: not significant*/
level variance = 1000000 noest ; /*stepwise 3: not random*/
slope variance = 1000000 ; /*stepwise 4: specify properly*/

season length = 12 type = trig variance = 3 noest ; /*stepwise 2: not random*/
*deplag lags = 1 ; 

estimate back = 12 plot = (residual normal acf) outest = est_1;
forecast back = 12 lead = 24 plot = (forecasts decomp) outfor = for_1 ;

run;

ODS GRAPHICS OFF;






/********Android users UCM pre 01/2015********/
ODS GRAPHICS ON ;

proc ucm data = datadir.input printall ;
id date interval = month ;
model cnt_Android;

irregular variance = 0 noest ; /*stepwise 1: not significant*/
level variance = 100000 noest ; /*stepwise 3: not random*/
slope variance = 100 noest ; /*stepwise 2: not random*/

season length = 12 type = trig variance = 0 noest /*droph = 2 3 4*/ ; /*stepwise 4: not significant*/
*deplag lags = 1 ; 

estimate back = 12 plot = (residual normal acf) outest = est_1;
forecast back = 12 lead = 24 plot = (forecasts decomp) outfor = for_1 ;

run;

ODS GRAPHICS OFF;


/*to compare actual with forecast*/
/*dataset for_1 will include the forecast values as well as the residuals*/

