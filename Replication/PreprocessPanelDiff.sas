/* NOTE the original dataset should be warrant.xiong.
However, we still don't know how n_day, n_week_2 are constructed,
so we directly construct it from warrant.xiong_3.

Further, warrant.xiong is not the original dataset; it is still obtained from warrant.warranttranns (likely).
It is not a problem for now, since those datasets are unlikely to change.  */

/* TODO Incompleted */

libname wrreplic "G:\证券数据\warrant_replication";

proc sql;
    create table xiong_all as
        select distinct a.*, b.diff as cycle1Diff, b.pre_amount as cycle1PredVolume
            from warrant.xiong_3 a left join warrant.use_date1 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;

    create table xiong_all as
        select distinct a.*, b.diff as cycle2Diff, b.pre_amount as cycle2PredVolume
            from xiong_all a left join warrant.use_date2 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;

data wrreplic.xiong_all;
    set xiong_all;
    pre_amount = cycle1Diff + cycle2Diff;
    pre_volume = cycle1PredVolume + cycle2PredVolume;
    pre_amount_scale = pre_amount / gta_tshare;
    pre_volume_scale = pre_volume / gta_tshare;
run;

data wrreplic.xiong_shock;
    set wrreplic.xiong_all;
    if securitycode = 038003 or
       securitycode = 038004 or
       securitycode = 038006 or
       securitycode = 038008 or
       securitycode = 580997;
run;


proc export data=wrreplic.xiong_all /* exporting for panel regression in stata for panel regression */
            file="G:\证券数据\warrant_replication\xiong_all.dta"
            DBMS=STATA REPLACE;
run;
proc export data=wrreplic.xiong_shock /* exporting for panel regression in stata for panel regression */
            file="G:\证券数据\warrant_replication\xiong_shock.dta"
            DBMS=STATA REPLACE;
run;
