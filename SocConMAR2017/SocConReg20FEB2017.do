//exp_return_1day_w  winsorize

xtreg branch_new_investor l.branch_new_investor l.return_1day_w l.exp_return_1day_w   l.branch_investor    l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m4
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.return_1day_w l2.return_1day_w l.exp_return_1day_w l2.exp_return_1day_w  l.branch_investor l2.branch_investor   l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max  l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m5
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l3.branch_new_investor l.return_1day_w l2.return_1day_w l3.return_1day_w l.exp_return_1day_w l2.exp_return_1day_w l3.exp_return_1day_w l.branch_investor l2.branch_investor l3.branch_investor l.brokerage_new_investor_adj l2.brokerage_new_investor_adj l3.brokerage_new_investor_adj l.market_return_min l2.market_return_min l3.market_return_min l.market_return_max l2.market_return_max l3.market_return_max l.turnover l2.turnover l3.turnover i.date i.n_day, fe vce(cluster id)
est store m6

esttab m4 m5 m6 using myout1.csv, replace compress nogap nonotes sca(r2_w) star(* 0.1 ** 0.05 *** 0.01)



//exp_return_1day_w  winsorize

xtreg branch_new_investor l.branch_new_investor l.return_1day_min_w  l.return_1day_max_w l.exp_return_1day_w   l.branch_investor    l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m4
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.return_1day_min_w l2.return_1day_min_w l.return_1day_max_w l2.return_1day_max_w l.exp_return_1day_w l2.exp_return_1day_w  l.branch_investor l2.branch_investor   l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max  l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m5
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l3.branch_new_investor l.return_1day_min_w l2.return_1day_min_w l3.return_1day_min_w l.return_1day_max_w l2.return_1day_max_w l3.return_1day_max_w l.exp_return_1day_w l2.exp_return_1day_w l3.exp_return_1day_w l.branch_investor l2.branch_investor l3.branch_investor l.brokerage_new_investor_adj l2.brokerage_new_investor_adj l3.brokerage_new_investor_adj l.market_return_min l2.market_return_min l3.market_return_min l.market_return_max l2.market_return_max l3.market_return_max l.turnover l2.turnover l3.turnover i.date i.n_day, fe vce(cluster id)
est store m6

esttab m4 m5 m6 using myout1.csv, replace compress nogap nonotes sca(r2_w) star(* 0.1 ** 0.05 *** 0.01)



//exp_return_1day_w  winsorize

xtreg branch_new_investor l.branch_new_investor l.return_1day_min_w  l.return_1day_max_w l.exp_return_1day_w   l.branch_investor    l.brokerage_new_investor_adj  l.market_return_min l.market_return_max  l.turnover i.date i.n_day, fe vce(cluster id)
est store m4
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l.return_1day_min_w l2.return_1day_min_w l.return_1day_max_w l2.return_1day_max_w l.exp_return_1day_w l2.exp_return_1day_w  l.branch_investor l2.branch_investor   l.brokerage_new_investor_adj l2.brokerage_new_investor_adj  l.market_return_min l2.market_return_min l.market_return_max l2.market_return_max  l.turnover l2.turnover i.date i.n_day, fe vce(cluster id)
est store m5
xtreg branch_new_investor l.branch_new_investor l2.branch_new_investor l3.branch_new_investor l.return_1day_min_w l2.return_1day_min_w l3.return_1day_min_w l.return_1day_max_w l2.return_1day_max_w l3.return_1day_max_w l.exp_return_1day_w l2.exp_return_1day_w l3.exp_return_1day_w l.branch_investor l2.branch_investor l3.branch_investor l.brokerage_new_investor_adj l2.brokerage_new_investor_adj l3.brokerage_new_investor_adj l.market_return_min l2.market_return_min l3.market_return_min l.market_return_max l2.market_return_max l3.market_return_max l.turnover l2.turnover l3.turnover i.date i.n_day, fe vce(cluster id)
est store m6

esttab m4 m5 m6 using myout1.csv, replace compress nogap nonotes sca(r2_w) star(* 0.1 ** 0.05 *** 0.01)