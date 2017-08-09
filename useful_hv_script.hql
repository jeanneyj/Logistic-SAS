
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



