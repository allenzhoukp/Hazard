/* ZHOU Kunpeng, 19 Jul 2018
   Copied from Replication folder.*/

/* NOTE the original dataset should be warrant.xiong.
However, we still don't know how n_day, n_week_2 are constructed,
so we directly construct it from warrant.xiong_3.

Further, warrant.xiong is not the original dataset; it is still obtained from warrant.warranttranns (likely).
It is not a problem for now, since those datasets are unlikely to change.  */

/* NOTE Using wrregen dataset (including 1 cycle, 2 cycle, and selling). New investors not included for now. */


libname wrregen "G:\证券数据\warrant_regen_from_gucode_20180715\";

proc sql;
    create table wrregen.xiong_all_1 as
        select distinct a.*, b.diff as cycle1Diff, b.amount as cycle1PredVolume
            from warrant.xiong_3 a left join wrregen.prediction_expand1_1_adj2 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;
proc sql;
    create table wrregen.xiong_all_2 as
        select distinct a.*, b.diff as cycle2Diff, b.amount as cycle2PredVolume
            from wrregen.xiong_all_1 a left join wrregen.prediction_expand1_2_adj2 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;
proc sql;
    create table wrregen.xiong_all_3 as
        select distinct a.*, b.diff as newinvsDiff, b.amount as newinvsPredVolume
            from wrregen.xiong_all_2 a left join wrregen.prediction_expand_new_adj2 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;

data wrregen.xiong_all_3;
	set wrregen.xiong_all_3;
	a = put(securitycode, z6.);
	drop securitycode;
	rename a = securitycode;
run;

proc sql;
    create table wrregen.xiong_all_4 as
        select distinct a.*, b.diff as selling2Diff, b.pre_amount as selling2PredVolume
            from wrregen.xiong_all_3 a left join wrregen.prediction_model2_9 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;
proc sql;
    create table wrregen.xiong_all_5 as
        select distinct a.*, b.diff as selling3Diff, b.pred_vol as selling3PredVolume
            from wrregen.xiong_all_4 a left join wrregen.warrant_intra_pred_allnew_3_9 b /* NOTE this can be changed to other dataset. */
                on a.securitycode = b.securitycode and
                   a.date = b.date
            order by SecurityCode, Date;
quit;


data wrregen.xiong_all;
    set wrregen.xiong_all_5;
    pre_amount = cycle1Diff + cycle2Diff + newinvsDiff;
    pre_volume = cycle1PredVolume + cycle2PredVolume + newinvsPredVolume;
    pre_sell = selling2Diff + selling3Diff;
    pre_sellvol = selling2PredVolume + selling3PredVolume;
    pre_amount_scale = pre_amount / gta_tshare;
    pre_volume_scale = pre_volume / gta_tshare;
    pre_sell_scale = pre_sell / gta_tshare;
    pre_sellvol_scale = pre_sellvol / gta_tshare;
run;

data wrregen.xiong_shock;
    set wrregen.xiong_all;
    if securitycode = 038003 or
       securitycode = 038004 or
       securitycode = 038006 or
       securitycode = 038008 or
       securitycode = 580997;
run;


proc export data=wrregen.xiong_all /* exporting for panel regression in stata for panel regression */
            file="G:\证券数据\warrant_regen_from_gucode_20180715\xiong_all.dta"
            DBMS=STATA REPLACE;
run;
proc export data=wrregen.xiong_shock /* exporting for panel regression in stata for panel regression */
            file="G:\证券数据\warrant_regen_from_gucode_20180715\xiong_shock.dta"
            DBMS=STATA REPLACE;
run;
