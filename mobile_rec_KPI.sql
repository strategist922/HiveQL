use www;

select '覆盖率' , t.c/tt.cc , from_unixtime(unix_timestamp(),'yyyy-MM-dd') from 
(select count(distinct shopid) as c from mobile_shoprank_cv_new_update_old) t
inner join
(select count(distinct shop_id) as cc from bi.dpdim_dp_shop where hp_valid_end_dt = "3000-12-31" and  star >= 30 and power in (5,10)) tt
on 1 = 1
;


create table if not exists mwt_mobile_rec_popularity(shopid int, p float);
insert overwrite table mwt_mobile_rec_popularity
select t.shopid, (t.c/tt.t) as p from 
(select shopid, count(deviceid) as c from mobile_shoprank_cv_new_update_old group by shopid) t
inner join
(select count(distinct deviceid) as t from mobile_shoprank_cv_new_update_old) tt
on 1 = 1
distribute by p
sort by p desc
;

select '信息熵' , sum(-p*ln(p)) , from_unixtime(unix_timestamp(),'yyyy-MM-dd') from mwt_mobile_rec_popularity



select '召回率 ',t.c/tt.cc , from_unixtime(unix_timestamp(),'yyyy-MM-dd') from 
(select count(*) as c from 
mobile_shopcv_count_last a
inner join mobile_shoprank_cv_new_update_old b
on a.deviceid = b.deviceid and a.shopid = b.shopid 
) t
inner join 
(select count(*) as cc from mobile_shoprank_cv_new_update_old) tt
on 1= 1 
;


insert overwrite table mobile_shoprank_cv_new_update_old
select * from mobile_shoprank_cv_new_update
;
