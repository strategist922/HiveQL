--- deviceid 累积的结果


--- 1.分为两步走，首先统计商圈的热门商户
--- 参考热搜词

--- 2.deviceid 所在的商圈目前没有，只有他经常访问的商户所在的商区，然后对其计算一个按照商区得来的分值

--- 1和2 合并项

use www;

set mapred.reduce.tasks=256;
set hive.exec.reducers.bytes.per.reducer=1000000;

create table if not exists mobile_shopcv_hot(deviceid string, shopid int);
insert overwrite table mobile_shopcv_hot
select distinct deviceid, shop_id
from bi.dpdw_traffic_base
where hp_log_type <= 104 
and hp_log_type >= 100
and hp_stat_time >= '2013-08-05'
and NOT deviceid REGEXP '^[0]+$'
and deviceid is not null and shop_id is not null
;


create table if not exists mwt_device_region(deviceid string , cityid int, regionid int ,score float);
INSERT OVERWRITE table mwt_device_region
select t1.deviceid as deviceid, t2.city_id as CityID ,t2.region_id as regionid, count(t1.deviceid) as score
from
(select distinct deviceid ,shopid from mobile_shopcv_hot ) t1 
inner join 
(select shop_id,city_id,region_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and region_id >0) t2 
on t2.shop_id = t1.shopid 
group by t1.deviceid, t2.city_id, t2.region_id 
distribute by deviceid,cityid
sort by deviceid, Score desc



create table if not exists mwt_device_region_entropy(deviceid string, cityid int, entropy float);
insert overwrite table mwt_device_region_entropy
select mt.deviceid, mt.cityid, -sum(mt.p*ln(mt.p)) from 
(
select t.deviceid, t.cityid, tt.regionid, tt.score/s as p from 
(select deviceid, cityid, sum(score) as s from mwt_device_region group by deviceid ,cityid) t
inner join 
mwt_device_region tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid
)mt
group by deviceid , cityid
;

insert overwrite table mwt_device_region
select t.deviceid, t.cityid, t.regionid, ln(1 + t.score)/(tt.entropy + 1)
from
mwt_device_region t
inner join mwt_device_region_entropy tt
on t.deviceid = tt.deviceid
and t.cityid = tt.cityid


inner overwrite table mwt_device_region
select t.deviceid, t.cityid, t.regionid , t.score*ln(1+tt.c)
from mwt_device_region t
inner join
(select deviceid, cityid, count(regionid) as c from mwt_device_region group by deviceid , cityid ) tt
on t.deviceid = tt.deviceid
and t.cityid = tt.cityid
;
-- create table if not exists mwt_mobile_region_new(cityid int, regionid int ,shopid int , count int);
-- insert overwrite table mwt_mobile_region_new
-- select t.city_id as cityid, t.region_id as regionid, t.shop_id as shopid, count(tt.deviceid) as count
-- from
-- (select shop_id, city_id ,region_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power > 3) t
-- inner join
-- (select distinct deviceid, shop_id from bi.dpdw_traffic_base where hp_log_type <= 104 and hp_log_type >= 100 
-- and hp_stat_time = '2013-08-18'
-- and NOT deviceid REGEXP '^[0]+$'
-- and deviceid is not null and shop_id is not null) tt
-- on t.shop_id = tt.shop_id
-- group by t.city_id, t.region_id, t.shop_id 
-- distribute by cityid ,regionid
-- sort by cityid,regionid,count desc
-- ;


-- create table if not exists mwt_mobile_region_old(cityid int, regionid int ,shopid int , count int);
-- insert overwrite table mwt_mobile_region_old
-- select t.city_id as cityid, t.region_id as regionid, t.shop_id as shopid, count(tt.deviceid) as count
-- from
-- (select shop_id, city_id ,region_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power > 3) t
-- inner join
-- (select distinct deviceid, shop_id from bi.dpdw_traffic_base where hp_log_type <= 104 and hp_log_type >= 100 
-- and hp_stat_time = '2013-08-17'
-- and NOT deviceid REGEXP '^[0]+$'
-- and deviceid is not null and shop_id is not null) tt
-- on t.shop_id = tt.shop_id
-- group by t.city_id, t.region_id, t.shop_id 
-- distribute by cityid ,regionid
-- sort by cityid,regionid,count desc
-- ;

-- create table if not exists mwt_mobile_region_update2(cityid int, regionid int ,shopid int , count int);
-- insert overwrite table mwt_mobile_region_update2
-- select * from (
-- select a.cityid as cityid, a.regionid  as regionid, a.shopid as shopid, if(b.shopid is null, 0.1, abs(a.count - b.count)/log10(1+b.count) ) as count from 
-- (select * from mwt_mobile_region_new where count > 10)a
-- left outer join
-- (select * from mwt_mobile_region_old where count > 10)b
-- on a.cityid = b.cityid
-- and a.regionid = b.regionid
-- and a.shopid = b.shopid
-- distribute by cityid,regionid
-- sort by cityid,regionid,count desc
-- ) mt 
-- where row_number(cityid,regionid) <= 20
-- ;


-- create table if not exists mwt_device_region_rec(deviceid string,cityid int,shopid int, score float);
-- insert overwrite table mwt_device_region_rec
-- select * from 
-- (
-- select t1.deviceid, t2.cityid, t2.shopid, rand() * (log2(2 + t2.count) * log2(2 + t1.score)) as score from 
-- mwt_mobile_region_update2 t2
-- inner join 
-- mwt_device_region t1
-- on t1.cityid = t2.cityid and t1.regionid = t2.regionid
-- where rand() > 0.5 
-- distribute by deviceid,cityid
-- sort by deviceid,cityid,score desc
-- )mt
-- where row_number(deviceid,cityid) <= 10
-- ;




