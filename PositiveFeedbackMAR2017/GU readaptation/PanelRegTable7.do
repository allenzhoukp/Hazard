// For Table 7, the input data is xiong_shock.
//    (shock indicates only those affected by May 30 event.)

// Copied from Replication folder. Replace all Social Contagion variables with Selling volume.
// ZHOU Kunpeng, Jul 19 2018

set more off
set matsize 3000

gen adj_gtaturnover = gta_turnover / 100
gen adj_vol = vol / 100
gen adj_gtatshare = gta_tshare / 1000000000  //1e9
gen adj_pre_amount_scale = pre_amount_scale * 10000 //pre_amount_scale is pre_amount / gta_tshare.
gen adj_pre_sell_scale = pre_sell_scale * 10000

gen transactionTax = 0
replace transactionTax = 1 if date >= mdy(5,30,2007)

reg gta_wrprice adj_pre_amount_scale adj_gtaturnover adj_gtatshare transactionTax i.n_1, cluster(date)
est store m1
reg gta_wrprice adj_pre_amount_scale adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m2
reg gta_wrprice adj_pre_amount_scale adj_gtaturnover adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m3

reg gta_wrprice adj_pre_sell_scale adj_gtaturnover adj_gtatshare transactionTax i.n_1, cluster(date)
est store m4
reg gta_wrprice adj_pre_sell_scale adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m5
reg gta_wrprice adj_pre_sell_scale adj_gtaturnover adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m6

reg gta_wrprice adj_pre_amount_scale adj_pre_sell_scale adj_gtaturnover adj_gtatshare transactionTax i.n_1, cluster(date)
est store m7
reg gta_wrprice adj_pre_amount_scale adj_pre_sell_scale adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m8
reg gta_wrprice adj_pre_amount_scale adj_pre_sell_scale adj_gtaturnover adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store m9

esttab m1 m2 m3 m4 m5 m6 m7 m8 m9 using Table7.csv, replace compress nogap nonotes ar2 star(* 0.1 ** 0.05 *** 0.01)
