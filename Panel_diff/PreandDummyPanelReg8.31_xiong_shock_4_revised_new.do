set more off

set matsize 10000

gen adj_gtaturnover=gta_turnover/100

gen adj_vol= vol/100

gen adj_gtatshare= gta_tshare/1000000000

gen adj_pre_amount_scale= pre_amount_scale*10000

gen after=0
replace after=1 if date>=mdy(5,30,2007)

reg gta_wrprice adj_pre_amount_scale adj_gtaturnover adj_gtatshare after i.n_1, cluster(date)
est store m1
reg gta_wrprice adj_pre_amount_scale adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m2
reg gta_wrprice adj_pre_amount_scale adj_gtaturnover adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m3

reg gta_wrprice pre_new_2_scale adj_gtaturnover adj_gtatshare after i.n_1, cluster(date)
est store m4
reg gta_wrprice pre_new_2_scale adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m5
reg gta_wrprice pre_new_2_scale adj_gtaturnover adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m6

reg gta_wrprice adj_pre_amount_scale pre_new_2_scale adj_gtaturnover adj_gtatshare after i.n_1, cluster(date)
est store m7
reg gta_wrprice adj_pre_amount_scale pre_new_2_scale adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m8
reg gta_wrprice adj_pre_amount_scale pre_new_2_scale adj_gtaturnover adj_vol adj_gtatshare after i.n_1, cluster(date)
est store m9

esttab m1 m2 m3 m4 m5 m6 m7 m8 m9 using myout.csv, replace compress nogap nonotes ar2 star(* 0.1 ** 0.05 *** 0.01)