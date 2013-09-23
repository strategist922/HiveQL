
use www;
set mapred.reduce.tasks=256;
set hive.exec.reducers.bytes.per.reducer=1000000;

--- rec_by_dianping --- 
create table mwt_review_data(userid int, cityid int, shopid int, score float);
insert overwrite table mwt_review_data
select userid ,cityid, shopid, star from bi.dpods_dp_review where hp_valid_end_dt = "3000-12-31"
and star >=10 
;


--归一化
insert overwrite table mwt_review_data
select tt.userid, tt.cityid , tt.shopid, tt.score/t.c as score
from
mwt_review_data tt
inner join 
(select count(*) as c , userid , cityid from  mwt_review_data group by userid , cityid) t
on tt.userid = t.userid and t.cityid = tt.cityid
;

--去均值
insert overwrite table mwt_review_data
select tt.userid , tt.cityid , tt.shopid, ln(tt.score/t.av+ 0.001)+0.001 from 
mwt_review_data tt
inner join
(select userid , cityid , avg(score) as av from mwt_review_data group by userid ,cityid) t
on t.userid = tt.userid and t.cityid = tt.cityid
;


insert overwrite table mwt_user_review_rank
select * from (
select cityid ,sid1, sid2, sum(score1*score2)*ln(count(sid1)) as score from 
(
select a.cityid as cityid, a.userid as sid1, b.userid as sid2 , a.shopid as shopid, a.score as score1, b.score as score2 from 
mwt_review_data a
inner join 
mwt_review_data b 
on a.cityid = b.cityid and a.shopid = b.shopid
where a.userid <> b.userid
) t
group by cityid, sid1, sid2
distribute by cityid,sid1
sort by cityid, sid1, score desc
)mt where row_number(cityid,sid1) <= 25
;

--- 时间后面再加
create table if not exists mwt_rec_by_user_review(userid int, cityid int, shopid int, score float);
insert overwrite table mwt_rec_by_user_review
select * from ( 
select st.userid as userid, st.cityid as cityid, st.shopid as shopid ,st.score as score from 
(
select t.sid1 as userid, tt.cityid as cityid, tt.shopid as shopid , sum(t.score * tt.score) as score from 
mwt_user_review_rank t
inner join 
(select * from mwt_review_data where score > 0.1)tt
on t.sid2 = tt.userid and t.cityid = tt.cityid
where rand()>0.3
group by t.sid1, tt.cityid , tt.shopid
) st
left outer join
mwt_review_data ss
on st.userid = ss.userid and ss.shopid = st.shopid
where ss.shopid is null 
distribute by userid , cityid
sort by userid, cityid,  score desc
)mt 
where row_number(userid,cityid) <= 500
;

-- create table if not exists mwt_mobile_rec_by_review(deviceid string, cityid int, shopid int, score float);
-- insert overwrite table mwt_mobile_rec_by_review
-- select t.deviceid as deviceid, tt.cityid as cityid, tt.shopid as shopid, tt.score as score from 
-- mobile_shopcv_count_last t
-- inner join
-- mwt_rec_by_user_review tt
-- on t.userid = tt.userid 
-- distribute by deviceid
-- sort by deviceid ,cityid , score desc
-- ;

