options compress = yes;
libname perm "T:\data\projects\jinyan\Email\HP\data"; 
libname save "T:\data\projects\jinyan\Email\HP\model\OptIn"; 

%let path = T:\data\projects\jinyan\Email\HP\model\OptIn\code\; /*your model folder*/
libname library  "T:\data\projects\jinyan\Email\HP\model\OptIn\code"; /*perm format library*/

%let outdir = T:\data\projects\jinyan\Email\HP\model\OptIn\; /*output directory*/
%let pgm = HP_OptIn;/*modify to mean your model*/

*--------------------------------*;
* Number of intervals desired    *;
*--------------------------------*;
%let numgroup=10;
%let top = 80;

*--------------------------------*;
* Input SAS data set name        *;
*--------------------------------*;
%let dataname=temp;
*--------------------------------*;
* Analysis variables macro       *;
*--------------------------------*;
*%inc "&path.varlist_01char.sas"; /* 0/1 Char */
*%inc "&path.varlist_category.sas"; /* Category */
*%inc "&path.varlist_geo.sas"; /* Geo elements */
*%inc "&path.varlist_num.sas"; /* Numberic */
%inc "&path.varlist_mi.sas";/* MI elements */
%let vardep = good;

data temp;set SAVE.input;
if ransel1 <= 750;
run;
/*proc freq data = temp;table good;run;*/

/*
 	                                                  Cumulative    Cumulative
                   GOOD    Frequency     Percent     Frequency      Percent
                   ƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒƒ
                      0       16457       79.95         16457        79.95
                      1        4127       20.05         20584       100.00
*/

filename varout "&path.selected.txt"; /* Top # variable list */
title "Source data = &dataname";
footnote;
options nodate center error=1 compress=no ls=122 ps=68 pageno=1;

*************************************************************;
* This program will rank each independent variable and      *;
* calculate the "importance" (based on entropy) to a given  *;
* dependent variable.  The top given number of variables    *;
* will be selected and outputted for model building and/or  *;
* other analysis                                            *;
*                                         *;
*************************************************************;

*--------------------------------*;
* Set numeric missing to -1 and  *;
* character missing to '-'       *;
*--------------------------------*;
data source;
   set &dataname(keep=&vardep %varlist);
   array NVar _numeric_;
   do over NVar;
      if NVar=. then NVar=-1;
   end;
  array CVar $ _character_;
   do over CVar;
      if CVar=' ' then CVar='-';
   end; 
run; 

*--------------------------------*;
* Separate the numeric variables *;
* and the character variables    *;
*--------------------------------*;
proc contents data=source(drop=&vardep)
              out=varnames(keep=name type) noprint;
run;
data numerVar charVar;
   set varnames;
   if type=1 then output numerVar;
  else if type=2 then output charVar; 
run;
*--------------------------------*;
* Calculate the importance for   *;
* all numeric variables          *;
*--------------------------------*;
%macro calcN;
   data _null_;
      set numerVar nobs=numobs;
      call symput('numvar', numobs);
   run;

   %do i=1 %to &numvar;
      data _null_;
         set numerVar(obs=&i firstobs=&i);
         call symput('varname', name);

      proc rank data=source(where=(&vardep>=0))
                out=testrank(keep=&varname &vardep bracket)
                groups=&numgroup ties=high;
         ranks bracket;
         var &varname;
      run;
      proc freq data=testrank noprint;
         table bracket*&vardep/ out=freqout ;
      run;
      proc means data=freqout noprint;
         var count;
         by bracket;
         output out=meanout sum=bsum;
      run;
      data entropy(keep=varname imptance);
         merge freqout(keep=bracket &vardep count)
               meanout(keep=bracket bsum)
               end=done;
         by bracket;
         numer+(-count*log(count/bsum));
         denom+bsum;
         if done then do;
            imptance=-log(numer/denom + 0.00000000001);
            varname="&varname";
            output;
         end;

      data entropyN;
         set entropyN entropy;
   %end;
%mend;

*--------------------------------*;
* Calculate the importance for   *;
* all character variables        *;
*--------------------------------*;
%macro calcC;
   data _null_;
      set charVar nobs=numobs;
      call symput('numvar', numobs);
   run;

   %do i=1 %to &numvar;
      data _null_;
         set charVar(obs=&i firstobs=&i);
         call symput('varname', name);

      proc freq data=source(where=(&vardep>=0)) noprint;
         table &varname*&vardep/ out=freqout ;
      run;
      proc means data=freqout noprint;
         var count;
         by &varname;
         output out=meanout sum=bsum;
      run;
      data entropy(keep=varname imptance);
         merge freqout(keep=&varname &vardep count)
               meanout(keep=&varname bsum)
               end=done;
         by &varname;
         numer+(-count*log(count/bsum));
         denom+bsum;
         if done then do;
            imptance=-log(numer/denom + 0.00000000001);
            varname="&varname";
            output;
         end;

      data entropyC;
         set entropyC entropy;
   %end;
%mend;
        
*--------------------------------*;
* Create two empty data sets     *;
*--------------------------------*;
%macro initiate;
   data entropyN;
      set _null_;
   data entropyC;
      set _null_;  
%mend;


%initiate;
%calcN;
%calcC;

*--------------------------------*;
* post processing:               *;
* combined numeric & character   *;
* variables                      *;
* sorted by the importance       *;
* print and output               *;
*--------------------------------*;
data perm.&pgm;
   set entropyN  entropyC ;
   RUN;

data _null_;
   set perm.&pgm(obs=&top);
   file varout;
   put @1 varname $32.;
run;

DATA TEMPLABEL;
SET TEMP(OBS = 10);
KEEP %varlist;
RUN;

proc contents data = templabel noprint out = lbl(keep = name label type);run;

proc sql;
create table zen
as select a.*, b.label,b.type
from perm.&pgm a, lbl b
where a.varname = b.name
order by imptance desc;
quit;

title;

ods noresults;
ODS HTML BODY = "'&outdir.test_entropy_mi.xls'" style = minimal;/*change report directory and file name*/
proc print data = zen;
var   
imptance
varname
TYPE
LABEL
;
run;
ODS HTML CLOSE;
dm "wbrowse  '&outdir.test_entropy_mi.xls'";/*change report directory and file name*/






