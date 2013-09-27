--dianping jilu
create table if not exists dp_review(reviewid int, cityid int, shopid int, reviewbody string);
insert overwrite table dp_review
select distinct t.reviewid as reviewid , t.cityid as cityid, t.shopid as shopid, tt.reviewbody as reviewbody
from
(select reviewid,cityid,shopid from bi.dpods_dp_review) t
inner join
(select reviewid, reviewbody from bi.dpods_dp_kv_reviewbody where hp_statdate > '2013-07-01' ) tt
on tt.reviewid = t.reviewid
;

--shop_dish

create table if not exists mwt_shop_info(cityid int ,shopid int , shop_tags string, dish_tags string);
insert overwrite table mwt_shop_info
select city_id ,shop_id, shop_tags, dish_tags
from  bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10) and cat0_id = 10
and (shop_tags != "" or dish_tags !="")
;

insert overwrite table dp_review
select tt.reviewid, tt.cityid, tt.shopid, tt.reviewbody from
(select  shopid from mwt_shop_info where dish_tags <> "") t
inner join dp_review tt
on t.shopid = tt.shopid
;

create table if not exists mwt_shop_dish(cityid int , shopid int , tagtype string ,tags string, rank float);
insert overwrite table mwt_shop_dish
select * from(
from (select cityid, shopid, dish_tags from mwt_shop_info where dish_tags <> "") t
reduce t.cityid, t.shopid, t.dish_tags
using 'reducer_tags2.py' as cityid, shopid ,tagtype, tags , rank ) rt;

insert overwrite table mwt_shop_dish
select * from mwt_shop_dish
where tags <> "没有"
;

hive -e "select * from www.dp_review" > dp_review.txt
hive -e "select shopid , tags from www.mwt_shop_dish"> shop_dish.txt
hive -e "select concat(tags,' 1 n') from www.mwt_shop_dish group by tags having count(tags) > 1"> dish_dict_reduced.txt