* Program_Name : sequential_transaction.sas ;
* Description  : this program creates a report that list the sequencial transactions ;
* by Jing Yang - 08/30/2016 ;


/**********************data pre-processing**********************/
/*this is only an example*/
/*please manipulate the data as what you need*/

/*
libname in "/u03/data/cig_ebi/sandbox/i637308/data/research/pymt" ;
data in.sample ;
set in.chk_qd_dtl(keep=prof_id olm_sess_key_nb chnl_id tran_dt tran_tm tran_tp_cd) ; 
format txn $10. ;
if tran_tp_cd in ('7351','7803','7805','7807','7809','7811','7897','7899','7901','7903','7905','7907','7909','7913') then txn = 'Berr' ;
else txn = tran_tp_cd;
run;
*/

/**********************************************************************************/
/*********************************input parameters*********************************/
/*******************change the following lines with your inputs********************/
/**********************************************************************************/

/*data name & direction*/
%let datadir = /u03/data/cig_ebi/sandbox/i637308/data/research/pymt ;     
%let input = sample ;                        

/*output report name & direction*/
%let HTMLdir = /u03/data/cig_ebi/sandbox/i637308/data/research/pymt ;
%let HTMLname = top_qd_sequences ;
%let detail = Top Quick Deposit Sequential Transactions ;

/*sequence itemset(ranked)*/
%let itemset = 
Berr
7855
7856
7827
7826
7828
7813
7814
7815
7817
7907
7908
7831
; 
/*sequence stopper*/
%let stopper = 
7520
7530
;

/*timestamp to sort by*/
%let sort = 
tran_dt 
tran_tm
;

/*session ID & transaction variable*/
%let session_id = olm_sess_key_nb ;
%let txn = txn ;

/*consider duplicates in sequence(?)*/
%let duplicate_item_ind = Y;

/*minimum % of occurency*/
%let min_support = 0.5% ;


/**********************************************************************************/
/***********************************end of change**********************************/
/**********************************************************************************/



/***************************************************************
* helper macro 1 - list all items and their order
****************************************************************/
%macro itemset_stopper(itemset, stopper);

%global n_item n_stop ;
%let n_item = %sysfunc(countw(%str(&itemset), %str( )));
%let n_stop = %sysfunc(countw(%str(&stopper), %str( )));

%do i = 1 %to &n_item;
	%global item&i ;
	%let item&i = %unquote(%qscan(&itemset, &i., %str( )));
%end; 

%do k = 1 %to &n_stop;
	%global stop&k ;
	%let stop&k = %unquote(%qscan(&stopper, &k., %str( )));
%end; 

%mend itemset_stopper;


/***************************************************************
* helper macro 2 - identify sequences within a session
****************************************************************/
%macro sequences(session_id, sort, txn) ;

data temp1 ;
format order $10. ;
set datadir.&input(keep=&session_id &sort &txn) ;

	%do i = 1 %to &n_item ;
	if &txn. in ("&&item&i..") then order = 'item_'||put(&i. , z2.) ;
	%end ;
	%do k = 1 %to &n_stop ;
	if &txn. in ("&&stop&k..") then order = 'done_'||put(&k. , z2.) ;
	%end ;
run ;

proc sort 	
	data = temp1(where=(^missing(order))) 
	out  = temp2  
	%if (&duplicate_item_ind = N) %then %do; nodupkey %end; 
; 
by &session_id &sort order &txn ; 
run ;



data temp3;
set temp2;
by &session_id &sort order &txn ; 
format sequence complete 8. ;
retain 
	%do i = 1 %to &n_item ; cnt_&&item&i. %end ;
	%do k = 1 %to &n_stop ; cnt_&&stop&k. %end ;
;
array cnt cnt_: ;

if 	substr(order,1,4) in ('done') 
	and substr(lag(order),1,4) not in ('done') 
	and complete < sequence 
	then complete + 1;

if substr(order,1,4) in ('item') and sequence = complete then do;
	sequence + 1;
	do over cnt;
		cnt=0;
	end;
end;

if first.&session_id then do; 
	sequence = 1; 
	complete = 0; 
	do over cnt;
		cnt=0;
	end;
end;

%do i = 1 %to &n_item ; 
	if &txn. in ("&&item&i..") then cnt_&&item&i. + 1 ;
%end ;

%do k = 1 %to &n_stop ; 
	if &txn. in ("&&stop&k..") then cnt_&&stop&k. + 1 ;
%end ;
run;


data sequence;
set temp3;
by &session_id sequence ;
if last.sequence then output;
run;

proc transpose data = temp3(where=(substr(order,1,4)='item'))
			   out = seq1	
			   prefix = item_
;
by &session_id sequence ;
var &txn. ;
run ;

proc transpose data = temp3(where=(substr(order,1,4)='done'))
			   out = seq2	
			   prefix = done_
;
by &session_id sequence ;
var &txn. ;
run ;

data seq ;
format LHS RHS $5000. ;
merge seq1(drop=_name_) 
	  seq2(drop=_name_) ;
by &session_id sequence ;
LHS = catx('-', of item_:) ;
RHS = catx('-', of done_:) ;
run;


%mend sequences ;


/***************************************************************
* helper macro 3 - calculate support
****************************************************************/
%macro calculate_support(min_support) ;

%let minsup = %sysfunc(inputn(&min_support, percent.), 10.4) ;

proc sql noprint ;
select count(*), count(distinct LHS), count(distinct RHS)
into :n_total trimmed, :n_LHS trimmed, :n_RHS trimmed
from seq
;

create table LHS as 
select LHS, count(*) as cnt_LHS, calculated cnt_LHS / &n_total. as LHS_support
from seq
group by LHS
;

create table RHS as 
select RHS, count(*) as cnt_RHS, calculated cnt_RHS / &n_total. as RHS_support
from seq
group by RHS
;

create table support as
select LHS, RHS, count(*) as support, calculated support / &n_total. as pct_support
from seq
group by LHS, RHS
;
quit;

proc sort data = support ; by LHS ;
proc sort data = LHS ; by LHS ;
data confidence ;
merge support LHS ;
by LHS ;
confidence = pct_support / LHS_support ;
run ;

proc sort data = confidence; by RHS ;
proc sort data = RHS; by RHS ;
data lift ;
merge confidence RHS ;
by RHS ;
lift = confidence / RHS_support ;
run ;
proc sort data = lift ; by descending support ; run ;

data datadir.report ;
set lift(where=(pct_support>=&minsup.)) ;
if missing(RHS) then RHS = 'n/a';
label
	LHS = "Sequential Pattern"
	RHS = "Associated Conversion"
	support = "Count of the Association"
	pct_support = "Percent of the Associations"
	cnt_LHS = "Count of the Sequential Pattern"
	LHS_support = "Percent of the Sequential Pattern"
	confidence = "Probability of the Conversion after the Sequential Pattern"
	cnt_RHS = "Count of the Conversion"
	RHS_support = "Percent of the Conversion"
	lift = "X times more likely to convert after the sequential pattern"
;
run;

%mend calculate_support ;


/***************************************************************
* helper macro 4 - output to html
****************************************************************/

%macro report_html(HTMLdir, HTMLname, detail) ;


%let date = %sysfunc(inputn(&sysdate, date.), yymmdd10.) ;

proc sort data = datadir.report ; by descending support ; run ;

***  Build the report ;
data Rpt ;
file "&HTMLdir/&HTMLname..html" ;
set datadir.report end = eof ;
by descending support ;
RHS = "  ==>  "||RHS;

	if _N_ = 1 then do;
		put "<html><head><title> Digital Analytics - Sequential Transaction Report</title></head>" /
			"<div align=center><font color='#0000A0'><b> Digital Analytics - Sequential Transaction Report </b></font></div>" /
			"<table border=1>" /
			"<tr><td> Time </td><td> &date </td></tr>" /
			"<tr><td> Detail </td><td> &detail </td></tr>" /
			"<tr><td> Note </td><td> % Support > &min_support </td></tr>" /
			"</table>" /
			"<br><table border=1>" ;
		put "<tr><td align=left nowrap><b> Sequential Pattern </b></td>" /
			"    <td align=left nowrap><b> Associated Decision </b></td>" /
			"    <td align=left wrap><b> Support: Count of the Association </b></td>" /
			"    <td align=left wrap><b> % Support: Percent of the Association </b></td>" /
			"    <td align=left wrap><b> Confidence: Probability of the decision given the Pattern </b></td>" /
			"    <td align=left wrap><b> Lift: X times more than average probability of the decision </b></td>" /          
			"    <td align=left wrap><i> Count of the Sequential Pattern </i></td>" /
			"    <td align=left wrap><i> Percent of the Sequential Pattern </i></td>" /          
			"    <td align=left wrap><i> Count of the Decision </i></td>" /
			"    <td align=left wrap><i> Percent of the Decision </i></td>" /          
			"</tr>" ;
	end;

	if lift > 1.2 then
		put "<tr><td align=left nowrap>" LHS "</td>" /
			"    <td align=left nowrap>" RHS "</td>" /
			"    <td align=right wrap>" support comma9. "</td>" /
			"    <td align=right wrap>" pct_support percent8.2 "</td>" /
			"    <td align=right wrap>" confidence percent8.2 "</td>" /
			"    <td align=right wrap><font color='#008000'><strong>" lift 5.2 "</strong></font></td>" /          
			"    <td align=right wrap><i>" cnt_LHS comma9. "</i></td>" /
			"    <td align=right wrap><i>" LHS_support percent8.2 "</i></td>" /          
			"    <td align=right wrap><i>" cnt_RHS comma9. "</i></td>" /
			"    <td align=right wrap><i>" RHS_support percent8.2 "</i></td>" /          
			"</tr>" ;

	else if lift >= 0.8 then
		put "<tr><td align=left nowrap>" LHS "</td>" /
			"    <td align=left nowrap>" RHS "</td>" /
			"    <td align=right wrap>" support comma9. "</td>" /
			"    <td align=right wrap>" pct_support percent8.2 "</td>" /
			"    <td align=right wrap>" confidence percent8.2 "</td>" /
			"    <td align=right wrap>" lift 5.2 "</td>" /          
			"    <td align=right wrap><i>" cnt_LHS comma9. "</i></td>" /
			"    <td align=right wrap><i>" LHS_support percent8.2 "</i></td>" /          
			"    <td align=right wrap><i>" cnt_RHS comma9. "</i></td>" /
			"    <td align=right wrap><i>" RHS_support percent8.2 "</i></td>" /          
			"</tr>" ;

	else
		put "<tr><td align=left nowrap>" LHS "</td>" /
			"    <td align=left nowrap>" RHS "</td>" /
			"    <td align=right nowrap>" support comma9. "</td>" /
			"    <td align=right nowrap>" pct_support percent8.2 "</td>" /
			"    <td align=right nowrap>" confidence percent8.2 "</td>" /
			"    <td align=right nowrap><font color='#FF0000'><strong>" lift 5.2 "</strong></font></td>" /          
			"    <td align=right nowrap><i>" cnt_LHS comma9. "</i></td>" /
			"    <td align=right nowrap><i>" LHS_support percent8.2 "</i></td>" /          
			"    <td align=right nowrap><i>" cnt_RHS comma9. "</i></td>" /
			"    <td align=right nowrap><i>" RHS_support percent8.2 "</i></td>" /          
			"</tr>" ;
	       
   if EOF then put "</table><br><br></body></html>" ;

run;


%mend report_html ;



options compress = yes ;
options mprint mlogic details source source2 symbolgen ;
libname datadir "&datadir." ;

%itemset_stopper(&itemset, &stopper) ;
%sequences(&session_id, &sort, &txn) ;
%calculate_support(&min_support) ;
%report_html(&HTMLdir, &HTMLname, &detail) ;

























