
----------------create partitioned table-----------------------

drop table if exists t_xxx;
create table t_xxx
(
  aaa string,
  bbb bigint
)
comment 'results'
partitioned by (prd_end_dt date)
row format delimited
fields terminated by ','
lines  terminated by '\n'
stored as orc tblproperties('orc.compression'='snappy')
;


drop view if exists v_xxx;
create view v_xxx as
    select * from t_xxx;


--------------insert data into table---------------------
set hive.auto.convert.join=true;
--set hive.auto.convert.join=false;

insert overwrite table t_xxx partition (prd_end_dt)
select
aaa,
bbb,
prd_end_dt

from tmp
where prd_end_dt = '2017-03-31'
;


---------------------------run monthly------------------------------
---shell script to call hive---

SCRIPTDIR=/data/digital-analytics/i637308/hive_scripts;
for i in {16..17}
#  1 = Jan 2016
# 17 = May 2017

do
p=$(($i-1));
mo_end_dt=$(date -d "20160101 + $i month - 1 day" +%Y-%m-%d)


echo -e "\n \n \n \n Running for $mo_end_dt \n \n \n \n"
hive -i /data/digital-analytics/scripts/hiveInit.hql  --hivevar mo_end_dt=$mo_end_dt -f $SCRIPTDIR/hv_venmo_mm.hql 


done;


# in commmand line, run the following in the
# sh filename.sh &
# sh /location/filename.sh &
# sh filename.sh > out.log &

---in hive script---
!echo -e "\n \n \n Running for trunc('${hivevar:mo_end_dt}','month') and '${hivevar:mo_end_dt}' \n \n \n";

------------------------options---------------------
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=da_adhoc;
set hive.execution.engine=mr;


-------------load data into hive table----------------
CREATE TABLE da.t_dim_country_code
(
code_2l STRING,
code_3l STRING,
country_name STRING
)
COMMENT 'Wire Country Code Lookup'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES  TERMINATED BY '\n'
tblproperties("skip.header.line.count"="1")
;
 
 
-- You have to put the .txt data at the designated location before you run the load data command in HIVE.
load data local inpath '/home/f559934/wire/Country_Code_Lookup.txt' into table da.t_dim_country_code;



------------------------select most recent record-----------------------------
-- In HIVE & DB2
select * from
(
    select
     ...
     ROW_NUMBER() OVER(PARTITION BY PYMT_ID ORDER BY TRAN_DT DESC, TRAN_TM DESC)  as rank
) as temp
where rank = 1
 
 
-- In Teradata (aka.ICDW)
 select
     ...
 from table A
 QUALIFY ROW_NUMBER() OVER(PARTITION BY PYMT_ID ORDER BY TRAN_DT DESC, TRAN_TM DESC) = 1
 

-----------------export hive table to csv/txt file-------------------
# in command line
hive -e 'set hive.cli.print.header=true; select * from i637308.chk_qd_cmplt_rate;' > test.csv
# specify the delimiter to be '|'
hive -e 'set hive.cli.print.header=true; select * from i637308.chk_qd_cmplt_rate;' | sed 's/[|]/,/g' > test.txt




