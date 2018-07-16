drop if date==mdy(2,29,2008)
drop if date>=mdy(5,30,2007)

set matsize 10000
set more off

xtset id time

drop exp_return_1day exp_return_1day_max exp_return_1day_min

gen exp_return_1day=exp(return_1day)

winsor return_1day, gen( return_1day_w) p(0.01)
winsor return_1day_max, gen( return_1day_max_w) p(0.01)
winsor return_1day_min, gen( return_1day_min_w) p(0.01)
winsor exp_return_1day, gen( exp_return_1day_w) p(0.01)

gen inter1_w = return_1day_w * branch_investor
gen inter1_max_w = return_1day_max_w * branch_investor
gen inter1_min_w = return_1day_min_w * branch_investor
gen inter1_exp_w = exp_return_1day_w * branch_investor

gen market_return_max=0
replace market_return_max=market_return if market_return>0

gen market_return_min=0
replace market_return_min=market_return if market_return<0

xtreg branch_new_investor l.branch_new_investor  l.return_1day_min_w  l.return_1day_max_w  l.branch_investor   l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m7
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.return_1day_min_w l2.return_1day_min_w l.return_1day_max_w l2.return_1day_max_w l.branch_investor l2.branch_investor  l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max   l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m8
xtreg branch_new_investor l.branch_new_investor  l.exp_return_1day_w  l.branch_investor   l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m9
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.exp_return_1day_w l2.exp_return_1day_w l.branch_investor l2.branch_investor  l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max   l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m10
xtreg branch_new_investor l.branch_new_investor  l.return_1day_w  l.exp_return_1day_w  l.branch_investor   l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m11
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.return_1day_w l2.return_1day_w l.exp_return_1day_w l2.exp_return_1day_w l.branch_investor l2.branch_investor  l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max   l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m12


esttab m7 m8 m9 m10 m11 m12 using myout1.csv, replace compress nogap nonotes sca(r2_w) star(* 0.1 ** 0.05 *** 0.01)



