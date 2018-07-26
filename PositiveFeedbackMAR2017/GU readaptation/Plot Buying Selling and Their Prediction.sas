data warrant_date;
	set warrant.warrant_date;
	if amount > 0 then buy = amount;
	else buy = 0;
	if amount < 0 then sell = -amount;
	else sell = 0;
run;


proc sql;
	create table buysell as
		select distinct securitycode, date, sum(buy) as buying, sum(sell) as selling
			from warrant_date
			group by securitycode, date
			order by securitycode, date;

	create table buysell_2 as
		select distinct a.securitycode, a.date, b.buying, b.selling
			from warrant.warranttrans a left join buysell b
				on a.securitycode = b.securitycode and a.date = b.date
			order by securitycode, date;

	create table buysell_3 as
		select distinct a.*, b.pre_amount as selling2PredVolume
			from buysell_2 a left join wrregen.prediction_model2_9 b
				on a.securitycode = b.securitycode and a.date = b.date
			order by securitycode, date;
quit;

data buysell_3;
	set buysell_3;
	format a best12.;
	a = securitycode;
	drop securitycode;
	rename a = securitycode;
run;

proc sql;
	create table buysell_4 as
		select distinct a.*, b.amount as cycle1PredVolume
			from buysell_3 a left join wrregen.prediction_expand1_1_adj2 b
				on a.securitycode = b.securitycode and a.date = b.date
			order by securitycode, date;

	create table buysell_5 as
		select distinct a.*, b.amount as cycle2PredVolume
			from buysell_4 a left join wrregen.prediction_expand1_2_adj2 b
				on a.securitycode = b.securitycode and a.date = b.date
			order by securitycode, date;

quit;

data buysell_6;
set buysell_5;
buypred = cycle1PredVolume + cycle2PredVolume;
sellpred = selling2PredVolume;
run;

symbol i=join;
proc gplot data=buysell_6;
	by securitycode;
	plot buying*date selling*date buypred*date sellpred*date / overlay legend;
run;
quit;

data buysell_7;
	set buysell_6;
	buyratio = buypred / buying;
	sellratio = sellpred / selling;
run;

symbol i=join;
proc gplot data=buysell_7;
	by securitycode;
	plot buyratio*date sellratio*date / overlay legend;
run;
quit;
