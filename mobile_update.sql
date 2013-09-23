use www;
set mapred.reduce.tasks=256;
set hive.exec.reducers.bytes.per.reducer=1000000;


-- 一.数据来源： 1.前一天的访问记录 ，前5天的访问记录
--                
-- 二.基本推荐数据的生成: 浏览记录 ---> 看了又看^2

-- 三.结果的修正

-- 补增      1.根据用户商圈和商圈热门推荐出的结果
--           2.历史推荐结果

-- 过滤      1.以一定概率过 滤掉不喜欢的 / 并过滤掉不喜欢的相似结果
--           2.已经浏览过的数据
--            3.每个子分类的结果不能超过5个

-- 增强      1.卡、团、券 增强
--           2.根据点评推荐的结果           


-----------------------------------------------------------------------------------------------
-- 1.生成浏览记录数据
-- 最近5day记录
create table if not exists mobile_shopcv_count(deviceid string, userid int ,shopid int, count int);
insert overwrite table mobile_shopcv_count
select mt.deviceid, mt.user_id, mt.shop_id, (mt.count/ln(3+datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd') ,mt.dt))) 
from (
select deviceid , user_id , shop_id ,count(deviceid) as count ,max(hp_stat_time) as dt
from bi.dpdw_traffic_base
where hp_log_type <= 104 
and hp_log_type >= 100
and hp_stat_time >= 'TIMELAST'
and NOT deviceid REGEXP '^[0]+$'
and deviceid is not null and shop_id is not null
group by deviceid, user_id, shop_id 
having count > 2
distribute by deviceid,dt,count
sort by deviceid , dt desc, count desc 
)mt
where row_number(mt.deviceid) <= 30
;

-- 前1天用户访问记录, 既需要更新的id
create table if not exists mobile_shopcv_count_last(deviceid string , userid int, shopid int);
insert overwrite table mobile_shopcv_count_last
select distinct deviceid ,user_id ,shop_id
from bi.dpdw_traffic_base
where hp_log_type <= 104
and hp_log_type >= 100
and hp_stat_time >= 'TIMENOW'
and NOT deviceid REGEXP '^[0]+$'
and deviceid is not null and shop_id is not null
;


insert overwrite table mobile_shopcv_count
select t.deviceid, t.userid, t.shopid, t.count from 
mobile_shopcv_count t
inner join
(select distinct deviceid from mobile_shopcv_count_last) tt
on t.deviceid = tt.deviceid
;

-- 2. jion  看了又看 -- add cityid ,star power
create table if not exists mobile_shoprank_cv_new (deviceid string, cityid int, shopid int, rank float);
insert overwrite table mobile_shoprank_cv_new
select * from
(
select t1.deviceid as deviceid, t2.cityid as cityid, t2.sid2 as shopid, (rand()*0.5+0.5)*sum(log10(1 + t1.count) * t2.rank) as rank
from
(select * from mobile_shopcv_count where count > 1 ) t1
inner join
(select t22.sid1 as sid1, t22.sid2 as sid2, t21.city_id as cityid , t22.rank as rank from 
(select shop_id, city_id  from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10)) t21
inner join
(select * from shopcv_shopcvrank  where sid2 is not null )t22
on t22.sid2 == t21.shop_id
)t2
on t1.shopid = t2.sid1
where rand() > 0.5
group by t1.deviceid, t2.cityid, t2.sid2
distribute by deviceid, cityid
sort by deviceid ,cityid, rank desc
)m
where row_number(deviceid, cityid) <= 500
;

insert overwrite table mobile_shoprank_cv_new
select * from
(
select t1.deviceid as deviceid, t2.cityid as cityid, t2.sid2 as shopid, (rand()*0.5+0.5)*sum(t1.rank * t2.rank) as rank
from
mobile_shoprank_cv_new t1
inner join
(select t22.sid1 as sid1, t22.sid2 as sid2, t21.city_id as cityid , t22.rank as rank from 
(select shop_id, city_id  from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10)) t21
inner join
(select * from shopcv_shopcvrank  where sid2 is not null )t22
on t22.sid2 == t21.shop_id
)t2
on t1.shopid = t2.sid1
where rand() > 0.5
group by t1.deviceid, t2.cityid, t2.sid2
distribute by deviceid, cityid
sort by deviceid ,cityid, rank desc
)m
where row_number(deviceid, cityid) <= 300
;

--- 平滑热门商户
insert overwrite table mobile_shoprank_cv_new
select tt.deviceid, tt.cityid ,tt.shopid, tt.rank/(t.avr+0.000001) as rank
from
(select shopid, avg(rank) as avr from mobile_shoprank_cv_new group by shopid ) t
inner join
mobile_shoprank_cv_new tt
on tt.shopid = t.shopid 
distribute by deviceid
sort by deviceid, rank desc
;

-- 根据偏斜表过滤
insert overwrite table mwt_mobile_bias_rec
select * from
(
select t.deviceid as deviceid, t.cityid as cityid, tt.sid2 , t.updatetime as dt from
(select * from mwt_mobile_bias_wwwcron  where score < 0)t
inner join 
shopcv_shopcvrank tt
on t.shopid = tt.sid1
where rand() > 0.5
distribute by deviceid, cityid , dt
sort by deviceid, cityid, dt desc
)mt 
where row_number(deviceid) <= 20
;
--从推荐结果中过滤掉不喜欢的
insert into table mwt_mobile_bias_rec
select deviceid, cityid, shopid, updatetime
from mwt_mobile_bias_wwwcron
where score < 10
;

insert overwrite table mobile_shoprank_cv_new
select t.deviceid,t.cityid,t.shopid,t.rank from
mobile_shoprank_cv_new t
left outer join
(select * from mwt_mobile_bias_rec)tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid and  t.shopid = tt.shopid
where tt.shopid is null 
;

--同时从累积表中过滤掉不喜欢的
insert overwrite table mobile_shoprank_cv_base_wwwcron
select t.deviceid,t.cityid,t.shopid,t.rank from
mobile_shoprank_cv_base_wwwcron t
left outer join
mwt_mobile_bias_wwwcron tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid and  t.shopid = tt.shopid
where tt.shopid is null 
;
---------------------------------------------------------------------------------------

-- 合并团购--优惠券--会员卡--预约预订
insert into table mobile_shoprank_cv_new
select t2.deviceid as deviceid, t2.cityid, t2.shopid as shopid, t2.rank *ln(mt.score)
from
mobile_shop_rec_rank mt
inner join
mobile_shoprank_cv_new t2
on mt.shop_id = t2.shopid
;


-- 合并根据 点评和相似用户的推荐 
insert overwrite table mwt_mobile_rec_by_review
select t.deviceid as deviceid, tt.cityid as cityid, tt.shopid as shopid, tt.score as score from 
(select distinct userid, deviceid from mobile_shopcv_count_last)t
inner join
mwt_rec_by_user_review tt
on t.userid = tt.userid 
distribute by deviceid
sort by deviceid ,cityid , score desc
;

insert into table mobile_shoprank_cv_new
select t2.deviceid as deviceid, t2.cityid, t2.shopid as shopid, 5*(t1.score+1)*t2.rank from
mobile_shoprank_cv_new t2
inner join 
mwt_mobile_rec_by_review t1
on t1.deviceid = t2.deviceid and t1.cityid = t2.cityid and t1.shopid = t2.shopid
;

-- 合并结果去重 
insert overwrite table mobile_shoprank_cv_new
select deviceid , cityid, shopid , sum(rank)
from mobile_shoprank_cv_new
group by deviceid, cityid, shopid
;

-- 没看过的
create table if not exists mobile_shoprank_cv_new2 like mobile_shoprank_cv_new;
insert overwrite table mobile_shoprank_cv_new2
select mt.deviceid as deviceid,mt.cityid , mt.shopid as shopid, mt.rank as rank
from
mobile_shoprank_cv_new mt
left outer join
mobile_shopcv_count t2
on mt.deviceid = t2.deviceid  and mt.shopid = t2.shopid
where t2.shopid is null
distribute by deviceid
sort by deviceid, rank desc 
;


-- 看过的
insert into table mobile_shoprank_cv_new2
select mt.deviceid as deviceid, mt.cityid, mt.shopid as shopid, -1 * mt.rank
from
mobile_shoprank_cv_new mt
left outer join
mobile_shopcv_count t2
on mt.deviceid = t2.deviceid  and mt.shopid = t2.shopid
where t2.shopid is not null
;


insert overwrite table mobile_shoprank_cv_new2
select * from 
(
select tt.deviceid as deviceid, tt.cityid as cityid,  tt.shopid as shopid, tt.rank as rank
from
mobile_shoprank_cv_new2 tt
distribute by deviceid,cityid
sort by cityid, deviceid, rank desc
) mt
where row_number(deviceid,cityid)<= 200
;

---------------------------------------------------------------------------------------------------------------------------------------
--- 商圈热店
create table if not exists mwt_mobile_region_new(cityid int, regionid int ,shopid int , count int);
insert overwrite table mwt_mobile_region_new
select t.city_id as cityid, t.region_id as regionid, t.shop_id as shopid, count(tt.deviceid) as count
from
(select shop_id, city_id ,region_id from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10)) t
inner join
(select distinct deviceid, shop_id mobile_shopcv_count_last
where deviceid is not null and shop_id is not null) tt
on t.shop_id = tt.shop_id
group by t.city_id, t.region_id, t.shop_id
distribute by cityid ,regionid
sort by cityid,regionid,count desc
;

--平滑
create table if not exists mwt_mobile_region_new_tf(cityid int, regionid int ,shopid int , score float);
insert overwrite table mwt_mobile_region_new_tf
select t1.cityid, t1.regionid, t1.shopid, ln(t1.count)*t1.count/t2.c*(0.6+rand()*0.4) 
from 
(select * from mwt_mobile_region_new where count > 10) t1
inner join
(select cityid, regionid, sum(count) as c from mwt_mobile_region_new group by  cityid,regionid) t2
on t1.cityid = t2.cityid and t1.regionid = t2.regionid
;

-- 按商圈生成50
create table if not exists mwt_mobile_region_update(cityid int, regionid int ,shopid int , score float);
insert overwrite table mwt_mobile_region_update
select * from (
select a.cityid as cityid, a.regionid  as regionid, a.shopid as shopid, if(b.shopid is null, a.score, a.score - b.score) as score from 
mwt_mobile_region_new_tf a
left outer join
mwt_mobile_region_old_tf b
on a.cityid = b.cityid
and a.regionid = b.regionid
and a.shopid = b.shopid
distribute by cityid,regionid
sort by cityid,regionid,score desc
) mt 
where row_number(cityid,regionid) <= 50
;

-- 商圈推荐
create table if not exists mwt_device_region_rec(deviceid string,cityid int,shopid int, score float);
insert overwrite table mwt_device_region_rec
select * from 
(
select t12.deviceid as deviceid, t12.cityid as cityid, t3.shopid as shopid, (t3.score * log2(2 + t12.score)) as score from
(
select t1.deviceid as deviceid, t2.cityid as cityid, t1.regionid as regionid, t1.score as score from
(select distinct deviceid, cityid from  mobile_shoprank_cv_new2)t2  -- today deviceid
inner join 
mwt_device_region t1 -- deviceid  region
on t2.deviceid = t1.deviceid and t2.cityid = t1.cityid) t12  -- deviceid  shopid  
inner join
(select * from mwt_mobile_region_update where score > 0) t3   -- today shopid region
on t12.cityid = t3.cityid and t3.regionid = t12.regionid
distribute by deviceid,cityid
sort by deviceid,cityid,score desc
)mt
where row_number(deviceid,cityid) <= 10
;

-- 更新old
insert overwrite table mwt_mobile_region_old_tf
select * from mwt_mobile_region_new_tf
;

---------------------------------
--- 更新累积数据
insert overwrite table mobile_shoprank_cv_base_wwwcron
select deviceid,cityid,shopid,0.7*rank 
from mobile_shoprank_cv_base_wwwcron
where rank > 0.05;


create table if not exists mobile_shoprank_cv_new_update like mobile_shoprank_cv_new2;
insert overwrite table mobile_shoprank_cv_new_update 
select t.deviceid, t.cityid, tt.shopid, tt.rank from 
(select distinct deviceid, cityid from mobile_shoprank_cv_new2) t
left outer join
mobile_shoprank_cv_base_wwwcron tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid
where tt.deviceid is not null and rand() > 0.5
;

insert into table mobile_shoprank_cv_new_update
select * from mobile_shoprank_cv_new2;


create table if not exists mwt_mobile_shopcv_new_avg(deviceid string, cityid int, rank float);
insert overwrite table mwt_mobile_shopcv_new_avg
select deviceid , cityid , avg(rank) from mobile_shoprank_cv_new2
group by deviceid, cityid
;

insert into table mobile_shoprank_cv_new_update
select t.deviceid,t.cityid,t.shopid, tt.rank * (0.6 + 0.4 * rand())
from mwt_device_region_rec t
inner join  mwt_mobile_shopcv_new_avg tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid
;


insert overwrite table mobile_shoprank_cv_new_update
select deviceid, cityid, shopid, round(min(rank),3) as rank
from mobile_shoprank_cv_new_update
group by deviceid , cityid ,shopid
;
 ---  更新数据

create table if not exists mobile_shoprank_cv_new_update_cat1(deviceid string, cityid int, shopid int, rank float, cat1id int);
insert overwrite table mobile_shoprank_cv_new_update_cat1
select * from
(
select tt.deviceid as deviceid, tt.cityid as cityid, tt.shopid as shopid , tt.rank as rank , t.cat1_id as cat1_id from 
(select shop_id , cat1_id,shop_name from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10)) t
inner join 
(select * from mobile_shoprank_cv_new_update)tt
on t.shop_id = tt.shopid
distribute by deviceid,cityid,cat1_id
sort by deviceid ,cityid ,cat1_id
)
mt where row_number(deviceid,cityid,cat1_id) <= 5
;

insert overwrite table mobile_shoprank_cv_new_update
select * from 
(
select deviceid, cityid, shopid, rank
from mobile_shoprank_cv_new_update_cat1
distribute by deviceid ,cityid
sort by deviceid , cityid , rank desc
)mt
where row_number(deviceid,cityid)<=50
;

insert overwrite table mobile_shoprank_cv_base_wwwcron
select t.deviceid, t.cityid, t.shopid, t.rank from 
mobile_shoprank_cv_base_wwwcron t
left outer join
mobile_shoprank_cv_new_update tt
on t.deviceid = tt.deviceid and t.cityid = tt.cityid
where tt.deviceid is null
;

insert into table mobile_shoprank_cv_base_wwwcron
select * from mobile_shoprank_cv_new_update
;