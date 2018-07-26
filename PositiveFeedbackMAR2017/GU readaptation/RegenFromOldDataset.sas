/* Adjustment @ 2018-7-15: expand1_1_adj
   Replace dummy to if return does NOT exist, then 1.
   Add new variables: I(return > 0).
   Similarly to expand1_2_adj. */

libname wrregen "G:\Ö¤È¯Êý¾Ý\warrant_regen_from_gucode_20180715\";

data wrregen.expand1_1_adj2a;
    set warrant.warrant_expand1_1_adj;
    if hold_num = 1 then hold_dummy = 0;
    else hold_dummy = 1;
    if last_num = 1 then last_dummy = 0;
    else last_dummy = 1;
    if other_num = 1 then other_dummy = 0;
    else other_dummy = 1;

    if hold_return=. then hold_return=0;
    if last_return=. then last_return=0;
    if other_return=. then other_return=0;

    if hold_return > 0 then isHoldReturnPositive = 1;
    else isHoldReturnPositive = 0;
    if last_return > 0 then isLastReturnPositive = 1;
    else isLastReturnPositive = 0;
    if other_return > 0 then isOtherReturnPositive = 1;
    else isOtherReturnPositive = 0;

    drop hold_num last_num other_num;
run;

proc sort data=warrant.warranttrans out=wrregen.warranttrans;
    by securitycode date;
run;

data wrregen.warranttrans_1a;
    set wrregen.warranttrans;
    by securitycode date;
    if first.securitycode then
        n = 0;
    n + 1;
run;

/* mktrpx as return from xth previous trading day */
data wrregen.warranttrans_1b;
    set wrregen.warranttrans_1a;
    mktrp4 = wrprice / lag4(wrprice) - 1;
    mktrp15 = wrprice / lag15(wrprice) - 1;
    if n<=4 then mktrp4=.;
    if n<=15 then mktrp15=.;
run;

/* mktrp1 as mktrp4 of the-day-before-yesterday (in trading day)
   mktrp2 as mktrp15 of lag6
   (so, it is return of 1day / 2day, 2day / 6day, 6day / 21day,
    which is daily, weekly, and monthly.) */
data wrregen.warranttrans_1c;
    set wrregen.warranttrans_1b;
    mktrp1=lag2(mktrp4);
    mktrp2=lag6(mktrp15);
    if n<=2 then mktrp1=.;
    if n<=6 then mktrp2=.;
run;

data wrregen.warranttrans_1;
    set wrregen.warranttrans_1c;
run;

/* NOTE changeratio is turnover.
   Generate average turnover in [-2, -5], [-6, -21] lags (in trading day - n rather than date). */
proc sql;
    create table wrregen.warranttrans_2a as
        select distinct a.*, mean(b.changeratio) as turnoverLagWeek
            from wrregen.warranttrans_1 a left join wrregen.warranttrans_1 b
                on a.securityCode = b.securitycode
                and b.n <= (a.n - 2) and b.n >= (a.n - 5)
            group by a.securitycode, a.date
            order by a.securitycode, a.date;
    create table wrregen.warranttrans_2b as
        select distinct a.*, mean(b.changeratio) as turnoverLagMonth
            from wrregen.warranttrans_2a a left join wrregen.warranttrans_1 b
                on a.securityCode = b.securityCode
                and b.n <= (a.n - 6) and b.n >= (a.n - 21)
            group by a.securitycode, a.date
            order by a.securitycode, a.date;
quit;

data wrregen.warranttrans_2;
    set wrregen.warranttrans_2b;
    if n<=2 then turnoverLagWeek = .;
    if n<=6 then turnoverLagMonth = .;
    keep securitycode date mktrp1 mktrp2 turnoverLagWeek turnoverLagMonth;
run;


/* Merge the generated variables into expand1_1_adj2. */
proc sql;
create table wrregen.expand1_1_adj2b as
    select distinct x.*, y.*
        from wrregen.expand1_1_adj2a x left join wrregen.warranttrans_2 y
            on x.securitycode=y.securitycode and x.date=y.date
        order by fundacctnum,securitycode,date;
quit;
/* ...and output that. */
data wrregen.expand1_1_adj2;
    set wrregen.expand1_1_adj2b;
run;



/* Similarly for 1_2. */
data wrregen.expand1_2_adj2a;
    set warrant.warrant_expand1_2_adj;
    if hold_num = 1 then hold_dummy = 0;
    else hold_dummy = 1;
    if last_num = 1 then last_dummy = 0;
    else last_dummy = 1;
    if other_num = 1 then other_dummy = 0;
    else other_dummy = 1;

    if hold_return=. then hold_return=0;
    if last_return=. then last_return=0;
    if other_return=. then other_return=0;

    if hold_return > 0 then isHoldReturnPositive = 1;
    else isHoldReturnPositive = 0;
    if last_return > 0 then isLastReturnPositive = 1;
    else isLastReturnPositive = 0;
    if other_return > 0 then isOtherReturnPositive = 1;
    else isOtherReturnPositive = 0;

    drop hold_num last_num other_num;
run;

/* There's no need to recalc warranttrans again. */


/* Merge the generated variables into expand1_2_adj2. */
proc sql;
create table wrregen.expand1_2_adj2b as
    select distinct x.*, y.*
        from wrregen.expand1_2_adj2a x left join wrregen.warranttrans_2 y
            on x.securitycode=y.securitycode and x.date=y.date
        order by fundacctnum,securitycode,date;
quit;
/* ...and output that. */
data wrregen.expand1_2_adj2;
    set wrregen.expand1_2_adj2b;
run;



/* Similarly for new. */
data wrregen.expand_new_adj2a;
    set warrant.warrant_expand_new;
    if hold_num = 1 then hold_dummy = 0;
    else hold_dummy = 1;
    if last_num = 1 then last_dummy = 0;
    else last_dummy = 1;
    if other_num = 1 then other_dummy = 0;
    else other_dummy = 1;

    if hold_return=. then hold_return=0;
    if last_return=. then last_return=0;
    if other_return=. then other_return=0;

    if hold_return > 0 then isHoldReturnPositive = 1;
    else isHoldReturnPositive = 0;
    if last_return > 0 then isLastReturnPositive = 1;
    else isLastReturnPositive = 0;
    if other_return > 0 then isOtherReturnPositive = 1;
    else isOtherReturnPositive = 0;

    drop hold_num last_num other_num;
run;

/* There's no need to recalc warranttrans again. */


/* Merge the generated variables into expand_new_adj2. */
proc sql;
create table wrregen.expand_new_adj2b as
    select distinct x.*, y.*
        from wrregen.expand_new_adj2a x left join wrregen.warranttrans_2 y
            on x.securitycode=y.securitycode and x.date=y.date
        order by fundacctnum,securitycode,date;
quit;
/* ...and output that. */
data wrregen.expand_new_adj2;
    set wrregen.expand_new_adj2b;
run;
