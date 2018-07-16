// For Table 6, the input data is xiong_3 (all datalines).

// REPLICATION CONFIRMED SUCCESS (IDENTICAL)

set more off
set matsize 3000

// some scaling
gen adj_gtaturnover = gta_turnover / 100
gen adj_vol = vol / 100
gen adj_gtatshare = gta_tshare / 1000000000  //1e9 = 1bn

gen transactionTax = 0
replace transactionTax = 1 if date >= mdy(5,30,2007)

reg gta_wrprice adj_gtaturnover i.n_1, cluster(date)
est store ma1
reg gta_wrprice adj_vol i.n_1, cluster(date)
est store ma2
reg gta_wrprice adj_gtatshare i.n_1, cluster(date)
est store ma3
reg gta_wrprice adj_gtaturnover adj_vol adj_gtatshare i.n_1, cluster(date)
est store ma4
reg gta_wrprice adj_gtaturnover adj_gtatshare i.n_1, cluster(date)
est store ma5
reg gta_wrprice adj_vol adj_gtatshare i.n_1, cluster(date)
est store ma6

reg gta_wrprice adj_gtaturnover transactionTax i.n_1, cluster(date)
est store mb1
reg gta_wrprice adj_vol transactionTax i.n_1, cluster(date)
est store mb2
reg gta_wrprice adj_gtatshare transactionTax i.n_1, cluster(date)
est store mb3
reg gta_wrprice adj_gtaturnover adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store mb4
reg gta_wrprice adj_gtaturnover adj_gtatshare transactionTax i.n_1, cluster(date)
est store mb5
reg gta_wrprice adj_vol adj_gtatshare transactionTax i.n_1, cluster(date)
est store mb6

esttab ma1 ma2 ma3 ma4 ma5 ma6 mb1 mb2 mb3 mb4 mb5 mb6 using Table6.csv, replace compress nogap nonotes ar2 star(* 0.1 ** 0.05 *** 0.01)
