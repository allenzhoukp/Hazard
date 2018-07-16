// For Table 8, the input data is xiong_3 (all datalines).

// REPLICATION CONFIRMED SUCCESS (IDENTICAL)

set more off
set matsize 3000

// some scaling
gen adj_gtaturnover = gta_turnover / 100
gen adj_vol = vol / 100
gen adj_gtatshare = gta_tshare / 1000000000  //1e9 = 1bn

gen transactionTax = 0
replace transactionTax = 1 if date >= mdy(5,30,2007)

// Warning: Stata uses == as equal operator

reg gta_wrprice adj_gtaturnover adj_vol adj_gtatshare i.n_1 if transactionTax == 0, cluster(date)
est store m1
reg gta_wrprice adj_gtaturnover adj_gtatshare i.n_1 if transactionTax == 0, cluster(date)
est store m2
reg gta_wrprice adj_vol adj_gtatshare i.n_1 if transactionTax == 0, cluster(date)
est store m3

reg gta_wrprice adj_gtaturnover adj_vol adj_gtatshare i.n_1 if transactionTax == 1, cluster(date)
est store m4
reg gta_wrprice adj_gtaturnover adj_gtatshare i.n_1 if transactionTax == 1, cluster(date)
est store m5
reg gta_wrprice adj_vol adj_gtatshare i.n_1 if transactionTax == 1, cluster(date)
est store m6

esttab m1 m2 m3 m4 m5 m6 using Table8.csv, replace compress nogap nonotes ar2 star(* 0.1 ** 0.05 *** 0.01)
