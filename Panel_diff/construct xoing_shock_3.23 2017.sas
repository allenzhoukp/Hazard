data temp1;
set warrant.xiong_shock_3_revised2;
keep gta_wrprice date securitycode gta_tshare gta_turnover n_1 lag1_market_ret lag2_market_ret lag3_market_ret lag_adjfundamental market_ret vol n_week pre_new_2 pre_new_2_scale;
run;

proc sql;
create table temp2 as select distinct a.*, b.diff as pre_amount1 from temp1 a left join warrant.use_date1 b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, date;
run;

proc sql;
create table temp2 as select distinct a.*, b.diff as pre_amount2 from temp2 a left join warrant.use_date2 b
on a.securitycode=b.securitycode and a.date=b.date
order by securitycode, date;
run;

data warrant.xiong_shock_4_revised2;
set temp2;
pre_amount=pre_amount1+pre_amount2;
pre_amount_scale=pre_amount/gta_tshare;
run;