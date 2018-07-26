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
