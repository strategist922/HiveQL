use www;

set mapred.reduce.tasks=256;
set hive.exec.reducers.bytes.per.reducer=1000000;

add file /data/home/mainsite_dev/reducer_keyword_hot.py;

create table if not exists mwt_search_keyword_hot (guid string , cityid int ,shopid int, keyword string,dt string);
insert overwrite table mwt_search_keyword_hot
select guid_str, city_id, shop_id , regexp_extract(LOWER(referer),'/search/keyword/[0-9]+/0_(.+)',1) as keyword,hp_stat_time
from bi.dpdw_traffic_base
where
hp_log_type <= 1
and hp_stat_time >= '2013-10-12'
and city_id > 0
and city_id = regexp_extract(LOWER(referer),'/search/keyword/([0-9]+)/',1)
and LOWER(path) regexp '^/shop/.+'
and not LOWER(path) regexp '^/shop/[0-9]+/photos'
and  LOWER(referer) regexp '/search/keyword/[0-9]+/0_'
and not LOWER(referer) regexp '/search/keyword/[0-9]+/.+/'
and if(user_id>0, 1, rand()) > 0.7
DISTRIBUTE BY city_id
sort by city_id
;

insert overwrite table mwt_search_keyword_hot
select * from(
from (
select guid, cityid, shopid, keyword, dt
from mwt_search_keyword_hot
where keyword != "" and keyword is not null) t
reduce t.guid, t.cityid, t.shopid, t.keyword , t.dt
using 'reducer_keyword_hot.py' as userid, cityid, shopid, keyword,dt) rt
;

insert overwrite table mwt_search_keyword_hot
select distinct guid,cityid,shopid,keyword,dt from
mwt_search_keyword_hot
where keyword <> '美食'
;

-- create table if not exists mwt_keyword_hot_count_old(userid int ,cityid int, keyword string);
-- insert overwrite table mwt_keyword_hot_count_old
-- select distinct userid,cityid,keyword from
-- mwt_user_search_keyword_hot
-- where keyword <> '美食'  
-- and dt < '2013-08-19'
-- ;


create table if not exists mwt_keyword_hot_new(cityid int, keyword string , count float);
insert overwrite table mwt_keyword_hot_new
select cityid,keyword,count(guid) as c from mwt_search_keyword_hot
group by cityid,keyword
DISTRIBUTE by cityid
sort by cityid, c desc
;

-- insert overwrite table mwt_keyword_hot_old
-- select cityid,keyword,count(userid)  as c from mwt_keyword_hot_count_old
-- group by cityid,keyword
-- DISTRIBUTE by cityid
-- sort by cityid, c desc
-- ;

create table if not exists mwt_keyword_hot_new_tf(cityid int, keyword string , score float);
insert overwrite table mwt_keyword_hot_new_tf
select  tt.cityid, t.keyword, t.count/(tt.total + 0.00001) from
(select cityid , sum(count) as total from mwt_keyword_hot_new group by cityid ) tt
inner join 
(select * from mwt_keyword_hot_new where count >= 5 )t
on t.cityid = tt.cityid 
;

-- create table if not exists mwt_keyword_hot_old_tf(cityid int, keyword string , score float);
-- insert overwrite table mwt_keyword_hot_old_tf
-- select  tt.cityid, t.keyword, t.count/(tt.total + 0.00001) from 
-- (select cityid , sum(count) as total from mwt_keyword_hot_old group by cityid ) tt
-- inner join 
-- (select * from  mwt_keyword_hot_old where count > 20 )t
-- on t.cityid = tt.cityid 
-- ;

create table if not exists mwt_keyword_hot_update(cityid int, keyword string , count float);
insert overwrite table mwt_keyword_hot_update
select a.cityid as cityid, a.keyword  as keyword, if(b.keyword is null, a.score, a.score - b.score) from 
(select * from mwt_keyword_hot_new_tf)a
left outer join
(select * from mwt_keyword_hot_old_tf)b
on a.cityid = b.cityid
and a.keyword = b.keyword
;

insert overwrite table mwt_keyword_hot_update
select * from 
(
select * from mwt_keyword_hot_update where count >0 and count < 0.5 
DISTRIBUTE by cityid
sort by cityid, count desc
) mt
where row_number(cityid) <= 25
;

create table  if not exists mwt_keyword_hot_update_matched(keyword string,cityid int, score float);
insert overwrite table mwt_keyword_hot_update_matched
select if (t.referer is null,tt.keyword, t.shopname) as key, tt.cityid as cityid,  tt.count as score from
mwt_keyword_hot_update tt
left outer join
(SELECT * FROM mwt_keyword_refer_count WHERE SCORE >0.5) t
on t.cityid = tt.cityid and t.referer = tt.keyword
sort by cityid, score desc
;

insert overwrite table mwt_keyword_hot_update_matched
select * from (
select keyword,cityid,avg(score) as c from mwt_keyword_hot_update_matched
where cityid > 0 and length(keyword) <= 9
group by cityid,keyword
distribute by cityid
sort by cityid, c desc
)mt 
where row_number(cityid) <= 15
;

insert overwrite table mwt_keyword_hot_old_tf
select * from mwt_keyword_hot_new_tf;

insert into table mwt_keyword_hot_record_wwwcron
select cityid, keyword, score, from_unixtime(unix_timestamp(),'yyyy-MM-dd') from mwt_keyword_hot_update_matched;