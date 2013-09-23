-- wantao.ma

-- 1. 新建数据表
-- 库名：airec ，表名分别为 DP_NaviUserTag 和 DP_NaviUserTagBias；

-- MySql 建表 DP_NaviUserTagBias COMMENT ‘用户标签信息偏移表’
CREATE TABLE `DP_NaviUserTagBias` (
  `Id` int(12) NOT NULL AUTO_INCREMENT COMMENT '行ID 无意义',
  `UserID` int(12) NOT NULL COMMENT '用户ID',
  `CityID` int(12) NOT NULL COMMENT '城市ID',
  `TagType` varchar(20) DEFAULT NULL COMMENT '标签类型',
  `Tag` varchar(20) DEFAULT NULL COMMENT '标签名称',
  `Score` float DEFAULT NULL COMMENT '标签对应分值',
  `UpdateTime` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`Id`),
  KEY `IX_UserID_CityID_Tagtype` (`UserID`,`CityID`,`TagType`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


--  MySql 建表 DP_NaviUserTag  COMMENT ‘用户标签信息累积表’  
CREATE TABLE `DP_NaviUserTag` (
  `Id` int(12) NOT NULL AUTO_INCREMENT COMMENT '行ID 无意义',
  `UserID` int(12) NOT NULL COMMENT '用户ID',
  `CityID` int(12) NOT NULL COMMENT '城市ID',
  `TagType` varchar(20) DEFAULT NULL COMMENT '标签类型',
  `Tag` varchar(20) DEFAULT NULL COMMENT '标签名称',
  `Score` float DEFAULT NULL COMMENT '标签对应分值',
  `UpdateTime` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`Id`),
  KEY `IX_UserID_CityID_Tagtype` (`UserID`,`CityID`,`TagType`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 2.数据同步
-- 请将HIVE下 10.1.1.164 这台机器上的 tmp库下的DP_NaviUserTag表（约1亿条数据）同步到 MySQL 的airec库的DP_NaviUserTag表；
-- HIVE上的表中没有UpdateTime这个字段，所以请在同步数据时将UpdateTime这个字段的值设为now()；

-------------------------------------------------------------------
-- 设计思路
-- 用户访问数据分别来自主站和移动端
-- 步骤1： 从表hippolog 中统计用户近半年来访问过的商户, guid 对应 userid , 得到 shopcv_user_halfyear，包含各类商户;
-- 2：位置 根据商户的位置信息 ，得到用户的位置 标签 ，标签类型为 商圈 或者  行政区，累积标签出现的次数 ，作为tag对应的分数;
-- 3：餐厅特色和餐厅氛围  提取所有的美食 类商户 ，得到商户的 shop_tag字段信息（从中分离出 餐厅特色 和 餐厅氛围 两项标签类型），avg_price字段
-- 3.1：这两个标签 的内容 包括名字和分数都在同一个字段中，需要通过 python脚本进行解析；
-- 3.2：分数不能很好的表示商户对应某一个标签的分数 ，用TFIDF 得到每个标签 的真实 分数；
-- 3.3：根据 shopcv_user_halfyear 和 shop_tag 得到user_tag；
-- 4：价格排序或价格范围 商户信息中目前没有价格范围，暂用平均价格  
-- 5：优惠信息 暂无
-- 6: 合并

-- 1
--包括所有商户
create table if not exists mwt_shopcv_navitag(guid string , shopid int );
INSERT OVERWRITE TABLE mwt_shopcv_navitag
SELECT distinct guid,regexp_extract(LOWER(path),'^/shop/([0-9]+)',1) shopid
FROM default.hippolog
WHERE dt>='2012-09-01' 
and LOWER(path) regexp '^/shop/.+'
and not LOWER(path) regexp '^/shop/[0-9]+/photos'
and page_id = 12
DISTRIBUTE BY shopid 
sort by guid;

--用户半年  共 79579641 条 ,   distinct * 76884549  distinct userid 5475467
create table if not exists mwt_shopcv_navitag_userid(userID int, shopid int);
insert overwrite table mwt_shopcv_navitag_userid
select mainuserid.UserID , mwt_shopcv_navitag.shopid from mainuserid inner join mwt_shopcv_navitag
on mainuserid.guid = mwt_shopcv_navitag.guid
;

-- dp_navilocregion 21675873 条记录
create table mwt_navilocregion(userid int , cityid int, tagetype string, tag string ,score float);
INSERT OVERWRITE table mwt_navilocregion
select * from(
select UserID ,CityID , "商圈", Tag, Score
from(
select t1.userid as UserID, t2.city_id as CityID ,t2.region_name as Tag, log2(1+count(t1.UserID)) as Score
from(
select userid ,shopid from mwt_shopcv_navitag_userid ) t1 
inner join (
select shop_id, region_id ,region_name ,city_id  from  bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" 
) t2 
on t2.shop_id = t1.shopid and t2.region_id >0
group by t1.userid, t2.city_id, t2.region_id ,t2.region_name
) st 
distribute by UserID,CityID
sort by UserID,CityID, Score desc ) mt
where row_number(UserID,CityID)<= 10;

hive -i /usr/local/hadoop/hive-udf/hive-init.sql -e "select * from(select * from www.mwt_navilocregion distribute by UserID,CityID sort by UserID,CityID, Score desc ) mt where row_number(UserID,CityID)<= 3" > navi.txt
--  dp_navilocdestrict  16326289  条记录
create table dp_navilocdestrict(userid int , cityid int, tagetype string, tag string ,score float);
INSERT OVERWRITE table dp_navilocdestrict
select * from(
select UserID ,CityID , "行政区", Tag, Score
from(
select t1.userid as UserID, t2.city_id as CityID ,t2.district_name as Tag, count(t1.UserID) as Score
from(
select userid ,shopid from shopcv_user_halfyear ) t1 
inner join (
select distinct shop_id, district_id ,district_name ,city_id  from bi.dpmid_dp_shop
) t2 
on t2.shop_id = t1.shopid and t2.district_id >0
group by t1.userid, t2.city_id, t2.district_id ,t2.district_name
) st 
distribute by UserID
sort by UserID, Score desc ) mt
where row_number(UserID)<= 10;  


-- 3

--生成大表 保存 餐厅特色 餐厅氛围 平均价格  48403824条

create table dp_shopnavitag(cityid int ,shopid int,tags string,avgprice int);
insert overwrite table dp_shopnavitag
select distinct city_id, shop_id , shop_tags ,avg_price from bi.dpmid_dp_shop where cat0_id = 10 and shop_tags <>"";

--3.1  {情侣约会,朋友聚餐,家庭聚会,商务宴请,休息小憩,随便吃吃}餐厅氛围  {可以刷卡,无线上网,免费停车,可送外卖,24小时营业,无烟区,有午市套餐,有下午茶...}餐厅特色
--脚本开头一定要加 #!/bin/env python  
create table dp_shop_tags(cityid int ,shopid int , tagtype string, tags string, score float);
insert overwrite table dp_shop_tags
select * from(
from(select distinct cityid, shopid, tags from dp_shopnavitag where tags != "" and tags is not null) t  
reduce t.cityid ,t.shopid, t.tags 
using 'reducer_navitag.py' as cityid ,shopid , tagtype, tags , count) rt;

-- distinct shop number 104573
create table dp_shop_character(cityid int, shopid int , tag string, score float);
insert overwrite table dp_shop_character
select cityid ,shopid ,tags, score from dp_shop_tags where TagType = "餐厅特色";

-- distinct shop number 183268
create table dp_shop_atmosphere(cityid int,shopid int , tag string, score float);
insert overwrite table dp_shop_atmosphere
select cityid ,shopid ,tags, score from dp_shop_tags where TagType = "餐厅氛围";

-- 3.2 
--餐厅氛围
--IDF
insert overwrite table dp_shop_atmosphere
select tt.cityid as cityid ,tt.shopid as shopid,  tt.tag, tt.score/(t.total + 0.0000001) as tf
from (
select shopid, sum(score) as total 
from dp_shop_atmosphere 
group by shopid) t 
inner join dp_shop_atmosphere tt
on t.shopid = tt.shopid;

-- TFIDF
insert overwrite table dp_shop_atmosphere
select tt.cityid ,tt.shopid, t.tag, tt.score * t.idf as score
from (
select tag , ln(183268/count(shopid)) as idf 
from dp_shop_atmosphere 
group by tag ) t 
inner join dp_shop_atmosphere tt
on t.tag = tt.tag
sort by cityid, shopid, score desc;

-- 餐厅特色
insert overwrite table dp_shop_character
select tt.cityid as cityid ,tt.shopid as shopid,  tt.tag, tt.score/(t.total + 0.0000001) as tf
from (
select shopid, sum(score) as total 
from dp_shop_character 
group by shopid) t 
inner join dp_shop_character tt
on t.shopid = tt.shopid;

-- TFIDF
insert overwrite table dp_shop_character
select tt.cityid, tt.shopid, t.tag, tt.score * t.idf  score
from (
select tag , ln(104573/count(shopid)) as idf 
from dp_shop_character 
group by tag ) t 
inner join dp_shop_character  tt
on t.tag = tt.tag
sort by cityid, shopid, score desc;

 
-- dp_navicharacter 31552347记录
create table dp_navicharacter(userid int , cityid int, tagtype string, tag string, score float);
insert overwrite table dp_navicharacter
select us.userid as userid, t.cityid as cityid, '餐厅特色', t.tag as tag, sum(t.score) as score
from
dp_shop_character t
inner join shopcv_user_halfyear us
on t.shopid = us.shopid
group by us.userid, t.cityid ,t.tag
distribute by userid
sort by userid, cityid, score;

-- dp_naviatmosphere 26734734 条记录
create table dp_naviatmosphere(userid int , cityid int, tagtype string, tag string, score float);
insert overwrite table dp_naviatmosphere_mobile
select us.userid as userid, t.cityid as cityid, '餐厅氛围', t.tag as tag, sum(t.score) as score
from
dp_shop_atmosphere t
inner join shopcv_user_halfyear us
on t.shopid = us.shopid
group by us.userid, t.cityid ,t.tag
distribute by userid
sort by userid, cityid, score;

--4
create table dp_naviprice(userid int , cityid int, tagetype string, tag string, score float);
insert overwrite table dp_naviprice
select * from (
select tt.userid as userid , t.cityid as cityid, "价格区间", t.tag , count(tt.userid) as score
from
(select cityid, shopid, if (avgprice > 200, '200以上', if(avgprice > 120, '121-200',if(avgprice > 80, '81-120', if (avgprice > 50,'51-80' , if(avgprice > 20,'21-50','20以下'))))) as tag
from dp_shopnavitag where avgprice is not NULL) t
inner join
shopcv_user_halfyear tt 
on t.shopid = tt.shopid
group by t.cityid, tt.userid, t.tag
distribute by userid
sort by userid, cityid ,score desc)mt
where row_number(userID)<= 20;
-- 5
-- 6
-- 合并  dp_naviprice , dp_navicharacter , dp_naviatmosphere , dp_navilocregion, dp_navilocdestrict,  1亿条

create table dp_naviusertag(userid int , cityid int , tagtype string, tag string, score float);
insert overwrite table dp_naviusertag
select * from dp_navilocregion;

insert into table dp_naviusertag
select * from dp_navilocdestrict;

insert into table dp_naviusertag
select userid,cityid,tagtype,tag,score * 10 from dp_navicharacter;

insert into table dp_naviusertag
select userid,cityid,tagtype,tag,score * 10 from dp_naviatmosphere; 

insert into table dp_naviusertag
select * from dp_naviprice ;

--107902446 条记录
-----------------------------------------
--     移动端数据处理类似


create table shopcv_user_halfyear_mobile (userid int, shopid int);
insert overwrite table shopcv_user_halfyear_mobile
select distinct user_id, shop_id from bi.dpdw_traffic_base
where hp_stat_time > '2013-05-01'
and hp_log_type < 104 
and shop_id is not null
and user_id > 0;

insert into table shopcv_user_halfyear_mobile
select distinct user_id, shop_id from bi.dpdw_traffic_base
where hp_stat_time > '2013-04-01'
and hp_stat_time <= '2013-05-01'
and hp_log_type < 104 
and shop_id is not null
and user_id > 0;

insert into table shopcv_user_halfyear_mobile
select distinct user_id, shop_id from bi.dpdw_traffic_base
where hp_stat_time > '2013-03-01'
and hp_stat_time <= '2013-04-01'
and hp_log_type < 104 
and shop_id is not null
and user_id > 0;

insert into table shopcv_user_halfyear_mobile
select distinct user_id, shop_id from bi.dpdw_traffic_base
where hp_stat_time > '2013-02-01'
and hp_stat_time <= '2013-03-01'
and hp_log_type < 104 
and shop_id is not null
and user_id > 0;

insert into table shopcv_user_halfyear_mobile
select distinct user_id, shop_id from bi.dpdw_traffic_base
where hp_stat_time > '2013-01-01'
and hp_stat_time <= '2013-02-01'
and hp_log_type < 104 
and shop_id is not null
and user_id > 0;

-- 281659511
create table dp_navilocregion_mobile(userid int , cityid int, tagetype string, tag string ,score float);
INSERT OVERWRITE table dp_navilocregion_mobile
select * from(
select t1.userid as UserID, t2.city_id as CityID ,"商圈" , t2.region_name as Tag, count(t1.UserID) as Score
from 
shopcv_user_halfyear_mobile t1 
inner join 
(select distinct shop_id, region_id ,region_name ,city_id  from bi.dpmid_dp_shop) t2 
on t2.shop_id = t1.shopid and t2.region_id > 0
group by t1.userid, t2.city_id, t2.region_id ,t2.region_name
distribute by UserID
sort by UserID, Score desc ) mt
where row_number(UserID)<= 10;

create table dp_navilocdestrict_mobile(userid int , cityid int, tagetype string, tag string ,score float);
INSERT OVERWRITE table dp_navilocdestrict_mobile
select * from(
select UserID ,CityID , "行政区", Tag, Score
from(
select t1.userid as UserID, t2.city_id as CityID ,t2.district_name as Tag, count(t1.UserID) as Score
from shopcv_user_halfyear_mobile t1 
inner join (
select distinct shop_id, district_id ,district_name ,city_id  from bi.dpmid_dp_shop
) t2 
on t2.shop_id = t1.shopid and t2.district_id >0
group by t1.userid, t2.city_id, t2.district_id ,t2.district_name
) st 
distribute by UserID
sort by UserID, Score desc ) mt
where row_number(UserID)<= 10;

create table dp_naviprice_mobile(userid int , cityid int, tagetype string, tag string, score float);
insert overwrite table dp_naviprice_mobile
select * from (
select tt.userid as userid , t.cityid as cityid, "价格区间", t.tag , count(tt.userid) as score
from
(select cityid, shopid, if (avgprice > 200, '200以上', if(avgprice > 120, '121-200',if(avgprice > 80, '81-120', if (avgprice > 50,'51-80' , if(avgprice > 20,'21-50','20以下'))))) as tag
from dp_shopnavitag where avgprice is not NULL) t
inner join
shopcv_user_halfyear_mobile tt 
on t.shopid = tt.shopid
group by t.cityid, tt.userid, t.tag
distribute by userid
sort by userid, cityid ,score desc)mt
where row_number(userID)<= 20;

--  63907276
create table dp_navicharacter_mobile(userid int , cityid int, tagtype string, tag string, score float);
insert overwrite table dp_navicharacter_mobile
select us.userid as userid, t.cityid as cityid, '餐厅特色', t.tag as tag, sum(t.score) as score
from
dp_shop_character t
inner join shopcv_user_halfyear_mobile us
on t.shopid = us.shopid
group by us.userid, t.cityid ,t.tag
distribute by userid
sort by userid, cityid, score desc; 

--  48199083
create table dp_naviatmosphere_mobile(userid int , cityid int, tagtype string, tag string, score float);
insert overwrite table dp_naviatmosphere_mobile
select us.userid as userid, t.cityid as cityid, '餐厅氛围', t.tag as tag, sum(t.score) as score
from
dp_shop_atmosphere t
inner join shopcv_user_halfyear_mobile us
on t.shopid = us.shopid
group by us.userid, t.cityid ,t.tag
distribute by userid
sort by userid, cityid, score desc;

create table dp_naviusertag_mobile(userid int , cityid int , tagtype string, tag string, score float);
insert overwrite table dp_naviusertag_mobile
select * from dp_navilocregion_mobile;

insert into table dp_naviusertag_mobile
select * from dp_navilocdestrict_mobile;

insert into table dp_naviusertag_mobile
select userid,cityid,tagtype,tag,score * 10 from dp_naviatmosphere_mobile;

insert into table dp_naviusertag_mobile
select userid,cityid,tagtype,tag,score * 10 from dp_navicharacter_mobile; 

insert into table dp_naviusertag_mobile
select * from dp_naviprice_mobile;

-- 202881340