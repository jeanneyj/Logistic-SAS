/*check value of &sysparm to continue or abort*/

%macro check_for_errors;
   %if &syserr > 0 %then %do;
      endsas;
   %end;
%mend check_for_errors;
 
data _null_;
 if
    "&sysparm" eq ""
     or notdigit("&sysparm") > 0
     or length("&sysparm") ne 8
     or input("&sysparm",yymmdd8.) < "01jan2015"d
      or input("&sysparm",yymmdd8.) > "31dec2017"d
 then do;
     put "ERROR: sysparm argument is null or not in the format yyyymmdd";
     put "or sysparm argument violates restriction 31dec2017 > input_dt_sas > 01jan2015";
     put "aborting program!!!";
     abort;
 end;
 else do;
     input_dt_sas = input("&sysparm",yymmdd8.);
     put input_dt_sas date9.;
 end;
run;
%check_for_errors;


/*parsing &sysparm for more than one input parameter*/
%macro check_for_errors;
   %if &syserr > 0 %then %do;
      endsas;
   %end;
%mend check_for_errors;
data _null_;
 /* check for a comma.  If a comma is found, it is assumed two parameters were passed */
 if index("&sysparm",",") > 0 then do;
     put "Two or more parameters for sysparm found using comma delimiter.  Only the first two are considered.";
    /* check for the format of each parameter.  if any of the conditions are not passed, error out. */
    if
        scan("&sysparm",1,',') ne ""
         and notdigit(scan("&sysparm",1,',')) = 0
         and length(scan("&sysparm",1,',')) eq 8
         and input(scan("&sysparm",1,','),yymmdd8.) > "01jan2015"d
          and input(scan("&sysparm",1,','),yymmdd8.) < "31dec2017"d
        and scan("&sysparm",2,',') ne ""
         and notdigit(scan("&sysparm",2,',')) = 0
         and length(scan("&sysparm",2,',')) eq 8
         and input(scan("&sysparm",2,','),yymmdd8.) > "01jan2015"d
          and input(scan("&sysparm",2,','),yymmdd8.) < "31dec2017"d
    then do;
        put "winner winner";
        input_dt_sas1 = input(scan("&sysparm",1,','),yymmdd8.);
        input_dt_sas2 = input(scan("&sysparm",2,','),yymmdd8.);
        put input_dt_sas1= date9. input_dt_sas2= date9.;
    end;
    else do;
        put "ERROR: one of the two sysparm arguments is null or not in the format yyyymmdd,yyyymmdd";
        put "ERROR: or sysparm argument violates restriction 31dec2017 > parameter > 01jan2015.";
        put "ERROR: Aborting program!!!";
        abort;
    end;
 end;
 /* if a comma is not found, it is taken as a single parameter */
 else do;
     put "Single paramter for sysparm found using comma delimiter";
    /* check for the format of the parameter.  if any of the conditions are not passed, error out. */
    if
        "&sysparm" ne ""
         and notdigit("&sysparm") = 0
         and length("&sysparm") = 8
         and input("&sysparm",yymmdd8.) > "01jan2015"d
          and input("&sysparm",yymmdd8.) < "31dec2017"d
    then do;
        input_dt_sas = input("&sysparm",yymmdd8.);
        put input_dt_sas date9.;
    end;
    else do;
        put "ERROR: sysparm argument is null or not in the format yyyymmdd";
        put "ERROR: or sysparm argument violates restriction 31dec2017 > parameter > 01jan2015.";
        put "ERROR: Aborting program!!!";
        abort;
    end;
 end;
run;
%check_for_errors;



/*checking for the lastest file in a given directory*/
%macro check_max_token(token_prefix=);
 
%global max_token_date;
 
/* sets up the result of a unix list "ls" command as an input file to SAS */
filename tkndir pipe 'ls "/home/cig_edw/tokens" ';
  
data dirlist;
 length file_name $100;
  
 infile tkndir lrecl=200 truncover;                        
  
 input line $100.;              
  
 /* if the file is not a token, delete it */
 if index(line,".tok") = 0 then delete;
 /* if the file does not begin with your token prefix then delete it */
 if index(line,"&token_prefix") = 0 then delete;
  
 /* only grabs the first part of the file name (without the file extension) */
 file_name = scan(line,1,".");
 token_date = input(substr(file_name,length(file_name)-7,8),yymmdd8.);
 format token_date date9.;
 keep file_name token_date;
run;
proc sql noprint;
 select max(token_date) format=yymmdd10. into: max_token_date separated by ','
 from dirlist;
quit;
%put max token date for &token_prefix=&max_token_date;
 
%mend check_max_token;
 
%check_max_token(token_prefix=cig_olm_icdw_)
%check_max_token(token_prefix=dgtl_wllt_provn_evt_)
%check_max_token(token_prefix=icdw_rdm_refresh_)




/*import csv files to sas dataset*/
libname sandbox '/u03/data/cig_ebi/sandbox/[SID]/[FOLDER]';
 
proc import datafile="/u03/data/cig_ebi/sandbox/[SID]/[FOLDER]/[FILENAME.csv]"
    dbms=csv
    out=sb.[desired SAS dataset name]
    replace;
    getnames=yes; /*Set getnames=no when no column headers; getnames=yes to import column headers*/
run; 


data sb.wave2_list;
 infile '/u03/data/cig_ebi/sandbox/i359371/wave2_list.csv' dlm=',' dsd termstr=CRLF;
 input prof_id;
run;

/* generates 52 macro calls for 52 weeks prior to value of input_dt */
data temp;
 input_dt = '12dec2014'd;
 do i=0 to -52 by -1;
     week_end_dt = intnx('week',input_dt,i,'end');
    macro_call = '%weekly_dig(input_dt='||put(week_end_dt,mmddyy10.)||');';
    output;
 end;
 format week_end_dt mmddyy10.;
run;


/* Searches for an unknown number of data sets meeting certain name criteria. 
 Concatenates them together into a single data set */
 
libname sb '/u03/data/cig_ebi/sandbox/i359371/daily';
/* sets up the result of a unix list "ls" command as an input file to SAS */
filename dirlist pipe 'ls "/u03/data/cig_ebi/sandbox/i359371/daily" ';
 
data dirlist;
 length file_name $100;
 
 infile dirlist lrecl=200 truncover;                         
 
 input line $100.;               
 
 /* if the file is not a SAS data set, delete it */
 if index(line,".sas7bdat") = 0 then delete;
 /* if the file does not begin with 'wkly' then delete it */
 if substr(line,1,4) ne 'wkly' then delete;
 
 /* only grabs the first part of the file name (without the file extension) */
 file_name = scan(line,1,".");
 keep file_name;
run;
 
 /* puts the entire list of files into a macro variable */
proc sql noprint;
 select "sb."||strip(file_name) into: file_list separated by " "
 from dirlist;
quit;
%put file_list=&file_list;
 
 /* appends all of the data sets to one another */
data history;
 length WEEK_END_DT 8;
 set &file_list;
 WEEK_END_DT = input(PRD_END_DT,yymmdd10.);
 format WEEK_END_DT date9.;
 drop PRD_END_DT;
run;







