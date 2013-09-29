-- 搜索推荐词
--
-- 一.数据来源： 1.前一天的访问记录 ，前10天得搜索记录
--
-- 二.基本推荐数据的生成: 搜索记录 ---> 搜了有搜， 浏览记录 ---->看了又看

-- 三.结果的修正

-- 补增      1.浏览商户的相似商户名

-- 过滤      1.根据 搜索词--商户 的概率关系，将错误的搜索词对应到商户名

use www;
set mapred.reduce.tasks=256;
set hive.exec.reducers.bytes.per.reducer=1000000;
add file /data/home/mainsite_dev/reducer_keyword_recent.py;

-- get searchword record
create table if not exists mwt_keyword_recent(guid string, city int, keyword string, count float, dt string);
insert overwrite table mwt_keyword_recent
SELECT guid, city, regexp_extract(LOWER(path),'/search/keyword/[0-9]+/0_(.+)',1) as keyword, 1, dt
FROM default.hippolog
--where dt >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),15)
where dt >= 'TIMELAST'
and LOWER(path) regexp '/search/keyword/[0-9]+/0_'
and not LOWER(path) regexp '/search/keyword/[0-9]+/.+/'
DISTRIBUTE BY city, keyword
sort by guid
;

-- guid to userid
create table if not exists mwt_user_keyword_recent(userid int, city int, keyword string, count float, dt string);
insert overwrite table mwt_user_keyword_recent
select tt.userid , t.city , t.keyword , 1, dt
from mainuserid tt
inner join mwt_keyword_recent t
on tt.guid = t.guid
DISTRIBUTE by tt.userid
sort by tt.userid, t.city
;

-- parse
insert overwrite table mwt_user_keyword_recent
select * from(
from (select userid, city, keyword,count,dt from mwt_user_keyword_recent where keyword != "" and keyword is not null) t
reduce t.userid, t.city, t.keyword,t.count,t.dt
using 'reducer_keyword_recent.py' as userid,city,keyword,count,dt) rt
;

-- count  (userid int, city int, keyword string, count float, dt string);
insert overwrite table mwt_user_keyword_recent
select userid,city,keyword,sum(count),max(dt) from
mwt_user_keyword_recent
where keyword <> '美食'  
group by userid,city,keyword
distribute by userid
sort by userid, city
;

-- count searchword by city
create table if not exists mwt_keyword_count(keyword string, cityid int, count float);
insert overwrite table mwt_keyword_count
select t.keyword as keyword , t.city as cityid, count(t.userid)  as count
from (
select userid, city,keyword from mwt_user_keyword_recent )  t 
group by  t.city, t.keyword
DISTRIBUTE by cityid , keyword
sort by cityid, count desc
;

-- filer low frequency word (userid int, city int, keyword string, count float, dt string);
insert overwrite table mwt_user_keyword_recent
select tt.userid,tt.city,tt.keyword,tt.count,tt.dt from 
(select * from mwt_keyword_count where count >= 100) t
inner join
mwt_user_keyword_recent tt
on t.cityid = tt.city and t.keyword = tt.keyword
distribute by userid
sort by userid, city
;


-- count tf
create table if not exists mwt_user_keyword_recent_tf like mwt_user_keyword_recent;
insert overwrite table mwt_user_keyword_recent_tf
select tt.userid , tt.city, t.keyword, t.count/(tt.total + 0.00001), t.dt from 
(select city , userid , sum(count) as total from  mwt_user_keyword_recent
group by city , userid ) tt
inner join  mwt_user_keyword_recent t
on t.city = tt.city and t.userid = tt.userid
;

-- devide by time  
insert overwrite table mwt_user_keyword_recent_tf
select userid,city,keyword,(count/ln(3+datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd') ,dt))),dt
from mwt_user_keyword_recent_tf
;
-----------------caculate keyword rank----------------


create table if not exists mwt_keyword_rank(cityid int, sid1 string, sid2 string, score float);
insert overwrite table mwt_keyword_rank
select * from (
select 
b.city as cityid, 
b.keyword as sid1, 
c.keyword as sid2,
log10(10+count(b.userid)) as score
from
(select * from mwt_user_keyword_recent)b
inner join
(select * from mwt_user_keyword_recent)c 
on (b.city = c.city and b.userid = c.userid)
where b.keyword <> c.keyword and c.dt >= b.dt
and datediff(c.dt,b.dt) <= 1
group by b.city, b.keyword , c.keyword
distribute by cityid, sid1
sort by cityid , sid1 , score desc
) mt
where row_number(cityid, sid1) <= 100
;

-- minimum the influence of hot_keyword
insert overwrite table mwt_keyword_rank
select tt.cityid as cityid, tt.sid1 as sid1 ,tt.sid2 as sid2, tt.score/(t.avr+0.0001) as rank
from
(select cityid, sid2, sum(score) as avr from mwt_keyword_rank group by cityid,sid2) t
inner join
mwt_keyword_rank tt
on t.cityid = tt.cityid and  t.sid2 = tt.sid2 
distribute by cityid, sid1
sort by cityid, sid1,rank desc
;

insert overwrite table mwt_keyword_rank
select * from
(
select * from
mwt_keyword_rank
distribute by cityid ,sid1
sort by cityid ,sid1
)mt
where row_number(cityid,sid1) <=20
;

create table if not exists mwt_rec_keyword(userid int, cityid int,  keyword string, score float);
insert overwrite table mwt_rec_keyword
select * from (
select w.userid as userid,  r.cityid as cityid,  r.sid2 as keyword , sum(r.score*w.count)*(0.2+rand()*0.8) as score
from mwt_keyword_rank r 
inner join 
(select * from mwt_user_keyword_recent_tf) w 
on r.cityid = w.city and r.sid1 = w.keyword
group by  w.userid , r.cityid, r.sid2
distribute by userid
sort by userid, cityid, score desc
) mt
where row_number(userid,cityid) <= 80
;

insert overwrite table mwt_rec_keyword
select * from (
select w.userid as userid,  r.cityid as cityid,  r.sid2 as keyword , sum(r.score*w.score)*(0.2+rand()*0.8) as score
from mwt_keyword_rank r 
inner join 
(select * from mwt_rec_keyword) w 
on r.cityid = w.cityid and r.sid1 = w.keyword
group by  w.userid , r.cityid, r.sid2
distribute by userid
sort by userid, cityid, score desc
) mt
where row_number(userid,cityid) <= 80
;


--insert into table mwt_rec_keyword
--select * from (
--select userid,city,keyword,count from
--mwt_user_keyword_recent_tf t
--distribute by userid,city
--sort by userid,city, count desc
--)mt
--where row_number(userid,city) <= 2
--;
--
--insert overwrite table mwt_rec_keyword
--select * from (
--select * from
--mwt_rec_keyword
--distribute by userid,cityid
--sort by userid,cityid,score desc
--)mt
--where row_number(userid,cityid) <= 30
--;

---------- searchkeyword  match to shopname ------------------------------

create table if not exists mwt_keyword_refer (cityid int , shopid int ,referer string);
insert overwrite table mwt_keyword_refer
select city , regexp_extract(LOWER(path),'^/shop/([0-9]+)',1)  as shopid ,  regexp_extract(LOWER(referer),'/search/keyword/[0-9]+/0_(.+)',1) as re
from default.hippolog
where dt >= 'TIMELAST'
and LOWER(path) regexp '^/shop/.+'
and not LOWER(path) regexp '^/shop/[0-9]+/photos'
and  LOWER(referer) regexp '/search/keyword/[0-9]+/0_'
and not LOWER(referer) regexp '/search/keyword/[0-9]+/.+/'
DISTRIBUTE BY city, re
sort by city
;


add file /data/home/mainsite_dev/reducer_keyword.py;

insert overwrite table mwt_keyword_refer
select * from(
from (select  cityid, shop ,referer from mwt_keyword_refer where referer != "" and referer is not null) t  
reduce t.cityid, t.shop, t.referer
using 'reducer_keyword.py' as cityid, shop, referer) rt
;

-- 基础表

create table if not exists mwt_keyword_refer_count(cityid int, referer string, shopname string ,score float );
insert overwrite table mwt_keyword_refer_count
select  TT.cityid as cityid, TT.referer as referer, t.shop_name as shopname, count(t.shop_name) as score
from
mwt_keyword_refer TT
INNER JOIN
(select city_id, shop_id,shop_name from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31"  and  star >= 30 and power > 3 ) t
ON TT.CITYID = t.city_id and TT.shop = t.shop_id
group by TT.cityid , TT.referer, t.shop_name
DISTRIBUTE by cityid, referer
sort by cityid , referer, score desc
;

insert overwrite table mwt_keyword_refer_count
select tt.cityid as cityid, tt.referer as referer, tt.shopname as shopname , tt.score/(t.total+0.001) as score
from 
(
select  cityid, referer, sum(score) as total from  mwt_keyword_refer_count group by cityid , referer) t
inner join mwt_keyword_refer_count tt
on t.cityid = tt.cityid and t.referer = tt.referer
DISTRIBUTE by cityid, referer
sort by cityid , referer, score desc
;

create table if not exists mwt_rec_keyword_matched like mwt_rec_keyword;
insert overwrite table mwt_rec_keyword_matched
select tt.userid as userid, tt.cityid as cityid, if (t.referer is null,tt.keyword, t.shopname) as key, tt.score as score from
mwt_rec_keyword tt
left outer join
(SELECT * FROM mwt_keyword_refer_count WHERE SCORE >0.5) t
on t.cityid = tt.cityid and t.referer = tt.keyword
;

insert overwrite table mwt_rec_keyword_matched
select * from (
select userid,cityid,keyword, round(sum(score),5) as score from mwt_rec_keyword_matched
group by userid,cityid,keyword
distribute by userid , cityid
sort by userid ,cityid ,score desc
)mt 
where row_number(userid,cityid) <= 25;


insert overwrite table mwt_rec_keyword_matched
select * from mwt_rec_keyword_matched
where length(keyword) <= 9 
or keyword regexp '^[\\w\\s]+$'
distribute by userid , cityid
sort by userid ,cityid ,score desc
;

----------------------------------------------------------------------------------
--根据访问记录推荐一部分
-- get shopcv_recent
create table if not exists mwt_shopcv_recent(userid int , shopid int, count int);
INSERT OVERWRITE TABLE mwt_shopcv_recent
select b.userid as userid, a.shopid as shopid, count(shopid) as c from
(SELECT distinct guid,regexp_extract(LOWER(path),'^/shop/([0-9]+)',1) shopid
FROM default.hippolog
WHERE dt>= 'TIMENOW'
and LOWER(path) regexp '^/shop/.+'
and not LOWER(path) regexp '^/shop/[0-9]+/photos'
and page_id = 12
) a
inner join mainuserid b
on a.guid = b.guid
group by b.userid , a.shopid
DISTRIBUTE BY userid
sort by c desc;

INSERT OVERWRITE TABLE mwt_rec_keyword_byshopcv
select t3.userid as userid, t4.city_id as cityid, t4.shop_name as keyword, sum(t3.score) as score from
(select shop_id,shop_name,city_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31"  and  star >= 30 and power > 3 and length(shop_name) < 6 ) t4
inner join
(
SELECT t1.userid as userid, t2.sid2 as shopid, sum(t1.count * t2.rank) score
FROM mwt_shopcv_recent t1
INNER JOIN
shopcv_shopcvrank t2
on t1.shopid = t2.sid1
group by t1.userid, t2.sid2
)t3
on t3.shopid = t4.shop_id
group by t3.userid, t4.city_id, t4.shop_name
distribute by userid, cityid,keyword
sort by userid, cityid, score desc
;

insert overwrite table mwt_rec_keyword_byshopcv
select tt.userid, tt.cityid ,tt.keyword, tt.score/(t.avr+0.000001) as score
from
(select keyword, count(keyword) as avr from mwt_rec_keyword_byshopcv group by keyword ) t
inner join
mwt_rec_keyword_byshopcv tt
on tt.keyword = t.keyword
distribute by userid
sort by userid, score desc
;

insert overwrite table mwt_rec_keyword_byshopcv
select mt1.userid as userid, mt1.cityid as cityid, mt1.keyword as keyword, mt1.score as score from
mwt_rec_keyword_byshopcv mt1
left outer join
(SELECT tt.userid as userid, t.shop_name as shopname from
(select shop_id,shop_name,city_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31"  and  star >= 30 and power > 3 and length(shop_name) < 6 ) t
inner join
mwt_shopcv_recent tt
on tt.shopid = t.shop_id
)mt
on mt.userid = mt1.userid and mt.shopname = mt1.keyword
where mt.shopname is null
distribute by userid
sort by userid , cityid, score desc
;


insert overwrite table mwt_rec_keyword_byshopcv
select * from
(
select * from mwt_rec_keyword_byshopcv
where length(keyword) <= 9
distribute by userid
sort by userid, cityid, score desc
)
mt
where row_number(userid, cityid) <=5
;

-- 合并结果了
insert into table mwt_rec_keyword_matched
select * from mwt_rec_keyword_byshopcv
;

insert overwrite table mwt_rec_keyword_matched
select * from
(
select * from mwt_rec_keyword_matched
distribute by userid , cityid
sort by userid, cityid, score desc
)mt
where row_number(userid, cityid) <= 25
;



