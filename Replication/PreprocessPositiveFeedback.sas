/* Preprocessing
   The original dataset, warrant.warrant_date, is NOT exactly the first preprocess outcome warrant.warrant.
   The difference is still unknown.
   Guess: only initial given transactions are deleted, or maybe the first entire cycle is deleted.

   Steps:
   1. Preprocess warrant.warrant_date.
      Get pseudocycles and calculate variables related to pseudocycles.
   2. Preprocess warrant.warranttrans (warrant's daily information.)
      Calculate variables related to trading days.
   3. Combine into the final dataset.
   */

/* =========================================================================== */


/* Create pseudocycles.
   As in Clean_New by GU Shi, two 'cycles' (from 0 position to 0 position) combined into one pseudocycle if
   previous cycle's start == previous cycle's end == this cycle's start == this cycle's end.
   This can be shortened as: previous cycle's start = this cycle's end.

   However, we still need cycles, since AT the beginning of each 0 positions we don't know whether to start a new pseudocycle.
   (We cannot go back to previous observations in the data step... What a pity.)
   */

/* sort according to chrono order, within each investor, each security.
   descending amount is to avoid multiple transaction within a second. (That's how position is generated.) */

libname wrreplic "G:\Ö¤È¯Êý¾Ý\warrant_replication";

proc sort data = warrant.warrant_date out = wrreplic.warrant_transactions_1;
    by Fundacctnum Securitycode date time descending amount;
run;

/* Get cycles */
data wrreplic.warrant_transactions_2;
    set wrreplic.warrant_transactions_1;

    /* by default,
       set current value = value in previous transaction record */
    retain cycleID 0;

    /* use "prev" to refer to previous transaction record */
    prevPosition = lag(position);
    prevdate = lag(date);
    prevAcctnum = lag(Fundacctnum);
    prevSecurityCode = lag(Securitycode);

    /* Condition: new investor, new security code, or
         we've go across a 0-position transaction
         and the start of last pseudocycle (prevPseudocycleStart) == the day of 0-position (prevDate). */
    if Fundacctnum ~= prevAcctnum or Securitycode ~= prevSecurityCode or
       prevPosition = 0 then
        cycleID = cycleID + 1;

    /* Drop temporary variables */
    drop prevPosition prevdate prevAcctnum prevSecurityCode;
run;

/* Summary for cycles.
   NOTE I discover that I got more cycles than GU.
        By comparison I find he merge some different securitycodes (even fundacctnums) into one cycle.
        I think it is obviously a mistake. */

proc sql;
    create table wrreplic.warrant_cycles_1 as
        select cycleID, min(date) as start_date, max(date) as end_date
            from wrreplic.warrant_transactions_2
            group by Fundacctnum, Securitycode, cycleID;
quit;

/* Create PseudoID */
data wrreplic.warrant_cycles_2;
    set wrreplic.warrant_cycles_1;
    retain pseudoID 0;
    prevStartDate = lag(start_date);
    if end_date ~= prevStartDate then
        pseudoID = pseudoID + 1;
    drop prevStartDate;
run;

/* Link pseudoID */
proc sql;
    create table wrreplic.warrant_transactions_3 as
        select a.*, b.pseudoID
            from wrreplic.warrant_transactions_2 a, wrreplic.warrant_cycles_2 b
            where a.cycleID = b.cycleID;
quit;


/* Calculate return as: sum(sells) pq / sum(buys) pq - 1
   Also, mark isStart dummy for the first transaction of every pseudocycle. */
data wrreplic.warrant_transactions_4;
    set wrreplic.warrant_transactions_3;
    by Fundacctnum Securitycode pseudoID;

    /* Cumulative variables: retain */
    retain sumsells;
    retain sumbuys;

    if first.pseudoID then do;
        sumsells = 0;
        sumbuys = 0;
        isStart = 1;
    end;
    else
        isStart = 0;

    if amount > 0 then sumbuys = sumbuys + amount * price;
    else sumsells = sumsells - amount * price;

    if last.pseudoID then
        returnRate = sumsells / sumbuys - 1;

    drop sumsells sumbuys;
run;

/* Special: warrant_pseudoStart
   records all first transactions of a pseudocycle. */
data wrreplic.warrant_pseudoStart;
    set wrreplic.warrant_transactions_4;
    by Fundacctnum SecurityCode pseudoID;
    if first.pseudoID;
run;

/* Merge into pseudocycle! */
proc sql;
    create table wrreplic.warrant_pseudocycles_1 as
        select Branch, Fundacctnum, Securitycode, pseudoID,
                min(date) as start_date,
                max(date) as end_date,
                max(returnRate) as returnRate /* All except last record in a pseudocycle has returnRate=NA. So just take the max. */
            from wrreplic.warrant_transactions_4
            group by Branch, Fundacctnum, Securitycode, pseudoID;
quit;


/* Create variables we want */
data wrreplic.warrant_pseudocycles_2;
    set wrreplic.warrant_pseudocycles_1;
    by Branch Fundacctnum Securitycode;

    /* this counter is to avoid non-NA values for first several pseudocycles.
       for return lags, if it actually does not exist, set those values to NA.
       (to know what we should include in the different columns of regressions)*/
    retain pseudocycleCounter 0;
    if first.Securitycode then
        pseudocycleCounter = 0;
    pseudocycleCounter = pseudocycleCounter + 1;

    returnLag1 = lag(returnRate);
    if pseudocycleCounter <= 1 then
        returnLag1 = .;

    if returnRate > 0 then
        isReturnPositive = 1;
    else isReturnPositive = 0;

    if returnLag1 > 0 then
        isReturnLag1Positive = 1;
    else isReturnLag1Positive = 0;

    drop pseudocycleCounter;
run;


/* =========================================================================== */
/* Now, we need to obtain WarrantReturn, Turnover, AdjustedFundamental,
   Maturity, and transaction dates.
   These are from warrant.warranttrans (why this name?).
   Generate the variables we want, and use sql select to get warrant-date-investor HUGE data. */

/* NOTE IMPORTANT The original transaction data does not consists Feb 29, 2008.
   To make it simple, We need to delete it from daily info data.
   Also, to avoid name conflicts, rename begin_date and end_date. */

data wrreplic.warrant_dailyInfo_1;
    set warrant.warranttrans;
    if Date = '29Feb2008'd then
        delete;
    rename begin_date = initialDate;
    rename end_date = maturityDate;
run;

/* Maturity (or n_1) is measured most likely in trading day difference.
   Need to count all trading days after given date.
   - Requires sorting
   - Copied from Clean_New */
proc sort data = wrreplic.warrant_dailyInfo_1 out = wrreplic.warrant_dailyInfo_2;
    by Securitycode date;
run;
proc sql;
    create table wrreplic.warrant_dailyInfo_3 as
        select distinct a.*, count(distinct b.Date) as maturity
            from wrreplic.warrant_dailyInfo_2 a left join wrreplic.warrant_dailyInfo_2 b
                on a.Securitycode = b.securityCode and
                   a.Date < b.Date
            group by a.Securitycode, a.Date;
quit;

/* Generate yesterday's and the-day-before-yesterday's warrant return, turnover, AdjustedFundamental.
   - Requires sorting */
proc sort data = wrreplic.warrant_dailyInfo_3 out = wrreplic.warrant_dailyInfo_4;
    by Securitycode date;
run;
data wrreplic.warrant_dailyInfo_5;
    set wrreplic.warrant_dailyInfo_4 (keep = Date stockcode stockprice securitycode wrprice exprice
                                  ChangePercent ChangeRatio TrdVol tshare
                                  initialDate maturityDate maturity);
    by Securitycode;

    /* This is NOT maturity (n_1). Where is the definition of that? */
    /* maturity = maturityDate - date; */
    /* NOTE Not confirmed. Use the values already generated. */
    /* returnRate = wrprice / lag(wrprice) - 1;
    turnover = TrdVol / tshare; */
    dailyReturn = changepercent / 100;
    turnover = changeratio;
    adjustedFundamental = (1 - exprice / stockprice) / maturity;

    /* this counter is to avoid non-NA values for first several observations*/
    retain counter 0;
    if first.Securitycode then
        counter = 0;
    counter = counter + 1;

    lag1Return = lag(dailyReturn);
    lag2Return = lag(lag1Return);
    lag1Turnover = lag(turnover);
    lag2Turnover = lag(lag1Turnover);
    lag1AdjFundamental = lag(adjustedFundamental);

    if counter <= 1 then do;
        lag1Return = .;
        lag1Turnover = .;
        lag1AdjFundamental = .;
    end;
    if counter <= 2 then do;
        lag2Return = .;
        lag2Turnover = .;
    end;

    drop counter;
run;


/* =========================================================================== */

/* Combination process */
/* This is quite complicated. First, the final dataset is like a
   Investor x Security x Date Cartesian product.
    This means HUGE number of rows, est. 40m.
   Second, the dates consists of
    (end of a pseudocycle, first day of next pseudocycle] or
    (end of a pseudocycle, end of time horizon],
    which means:
        1. Drop all days within a pseudocycle except for the first day;
        2. The lag-in-pseudocycle variables are in EVERY observation.
           To be exact, if pseudocycle 2 ends at s and 3 starts at t,
           The lag-in-pseudocycle are those of 2 for all days in (s, t].

   Method:
    1. Grab a new variable "nextPseudoStart" in warrant_pseudocycles
       (if not exist, set it to 99999.)
    2. Get the Cartesian product
    3. Left join warrant_pseudocycles on special condition.
*/

/* 1. Grab nextPseudoStart.
   we need to sort descending in date first. */
proc sort data = wrreplic.warrant_pseudocycles_2 out = wrreplic.warrant_pseudocycles_3;
    by FundAcctnum Securitycode descending start_date descending end_date;
run;

/* Then nextPseudoStart is just simply lag(start_date).
   Special treatment for the first ones (i.e. last in chrono order). */
data wrreplic.warrant_pseudocycles_4;
    set wrreplic.warrant_pseudocycles_3;
    by Fundacctnum Securitycode;
    nextPseudoStart = lag(start_date);
    if first.Securitycode then
        nextPseudoStart = 99999;
run;


/* 2. Combination to get Cartesian product.
   This, is how you EXPLODE the number of observations, Chernov! */

proc sql;
    /* Basic idea is to select distinct a.Date, b.Fundacctnum, b.Securitycode.
       To include variables in warrant_dailyInfo, we use a.* instead,
       which should not generate more rows
       since combination of a.Date, a.Securitycode is unique. */
    create table wrreplic.warrant_combined_1 as
        select distinct a.*, b.Fundacctnum
            from wrreplic.warrant_dailyInfo_5 a,
                (select distinct Fundacctnum, Securitycode from wrreplic.warrant_transactions_4) b
            where a.Securitycode = b.Securitycode;
quit;


/* 3. Left join warrant_pseudocycles. */
proc sql;
    /* Here, all dates after start_date (and before next pseudocycle)
       are joined into current pseudocycle.
       So for the days After end_date,
           the present values are in fact "lag1" values. Similarly for lag2, 3, and so on.*/

    /* <=: The start_day is arranged into previous pseudocycle.
       This is because that day is the last day of decision processes
           and the decision is made according to previous pseudocycle. */

    create table wrreplic.warrant_combined_2 as
        select distinct a.*, b.*
            from wrreplic.warrant_combined_1 a left join wrreplic.warrant_pseudocycles_4 b
                on a.Fundacctnum = b.Fundacctnum and a.Securitycode = b.Securitycode
                   and a.date > b.start_date and a.date <= b.nextPseudoStart;

    /* Delete all rows within a pseudocycle but not the START of it. */
    delete from wrreplic.warrant_combined_2
        where date > start_date and date <= end_date;

quit;

/* Rename current to lag1, lag1 to lag2, ...*/
data wrreplic.warrant_combined_3;
    set wrreplic.warrant_combined_2;
    rename returnRate = returnLag1Pseudo;
    rename isReturnPositive = isReturnLag1PseudoPositive;
    rename returnLag1 = returnLag2Pseudo;
    rename isReturnLag1Positive = isReturnLag2PseudoPositive;
    rename pseudoID = prevPseudoID;
run;

/* Sort according to Fundacctnum-Securitycode-Date. */
proc sort data = wrreplic.warrant_combined_3 out = wrreplic.warrant_combined_4;
    by Fundacctnum Securitycode Date;
run;

/* Grab (begin, end) accordingly.
   begin counts from the first day after a pseudocycle as 0,1,...
   and end = begin + 1. */
data wrreplic.warrant_combined_5;
    set wrreplic.warrant_combined_4;
    by Fundacctnum Securitycode Date;
    retain begin;

    lagPrevPseudoID = lag(prevPseudoID);

    if first.Securitycode then
        begin = 0;
    else if prevPseudoID ~= lagPrevPseudoID then
        begin = 0;
    else
        begin = begin + 1;

    end = begin + 1;

    drop lagPrevPseudoID;
run;

/* isStart is more complicated.
   We've added a new column (isStart) in warrant_transactions.
        It is then passed to warrant_pseudoStart. Just left join it.
   (In fact, just set isStart = 1 if Date = nextPseudoStart is OK.
    However, the first pseudocycle is omitted since there is no header in warrant_pseudocycles.
    So I decide to make it more robust.) */

proc sql;
    create table wrreplic.warrant_combined_6 as
        select distinct a.*, b.isStart
            from wrreplic.warrant_combined_5 a left join wrreplic.warrant_pseudoStart b
                on a.Fundacctnum = b.Fundacctnum and a.Securitycode = b.SecurityCode
                and a.Date = b.Date;
quit;

/* =========================================================================== */

/* Generate dataset. */

data wrreplic.warrant_combinedOneCycle;
    set wrreplic.warrant_combined_6;
    if returnLag1Pseudo ~= . and returnLag2Pseudo = .;
run;

data wrreplic.warrant_combinedTwoCycle;
    set wrreplic.warrant_combined_6;
    if returnLag2Pseudo ~= .;
run;
