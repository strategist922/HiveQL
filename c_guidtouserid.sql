-- update guid to useridid

use www;
set mapred.reduce.tasks=256;

insert into table guiduseridcount
select guid_str, user_id, count(*) as c
from bi.dpdw_traffic_base
where hp_log_type = 0 and hp_stat_time >='TIMELAST' and user_id >0
group by guid_str, user_id
distribute by guid_str
sort by guid_str ,c desc
;

insert overwrite table guiduseridcount
select guid, userid, sum(count) as c from guiduseridcount
group by guid, userid
having c>1
distribute by guid
sort by guid ,c desc
;

-- old = new
insert overwrite table mainuserid
select * from mainuserid_new
;

--  get new
insert overwrite table mainuserid_new
select guid ,userid from (
select guid, userid, count
from guiduseridcount
distribute by guid
sort by guid,count desc ) a
where row_number(guid)=1 
;

-- get update
insert overwrite table mainuserid_update
select new.guid as guid , new.userid as userid 
from 
mainuserid_new new
left outer join
mainuserid old
on new.guid = old.guid
where old.userid is null or old.userid <> new.userid
;