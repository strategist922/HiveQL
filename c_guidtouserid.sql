-- update guid to useridid

use www;
set mapred.reduce.tasks=256;

insert overwrite table guiduseridcount
select guid, user_id, count(*) as c
from default.hippolog 
where user_id >0 and dt>='2012-01-01' and
group by guid, user_id
having c>1
distribute by guid
sort by guid,c desc
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