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


