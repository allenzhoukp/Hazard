/* Table 3, Column 1 */

/* Explanation:
class indicates which variables are in category type.
model indicates the main regression,
    (begin, end) indicates start and end timestamp in which the subject is in hazard,
    isStart(1) is the censoring dummy.
    ties=efron determines EFRON function.
baseline indicates the output dataset, and the variable names of the output. */

proc phreg data = wrregen.expand1_1_adj2;
class securitycode date n_1;
model (begin, end) * start(0) =
    lag1_return D1
    lag1_market_ret mktrp1 mktrp2
    lag1_turnover turnoverLagWeek turnoverLagMonth
    lag1_adjfundamental
    hold_return hold_dummy isHoldReturnPositive
    last_return last_dummy isLastReturnPositive
    other_return other_dummy isOtherReturnPositive
    securitycode date n_1
        / ties=efron rl;
baseline out = wrregen.baseline_date1 survival = survival cumhaz = cumhaz xbeta = xbeta;
run;

/* Table 3, Column 2 */
/* Similarly. */

proc phreg data = wrregen.expand1_2_adj2;
class securitycode date n_1;
model (begin, end) * start(0) =
    lag1_return D1
    lag2_return D2
    lag1_market_ret mktrp1 mktrp2
    lag1_turnover turnoverLagWeek turnoverLagMonth
    lag1_adjfundamental
    hold_return hold_dummy isHoldReturnPositive
    last_return last_dummy isLastReturnPositive
    other_return other_dummy isOtherReturnPositive
    securitycode date n_1
        / ties=efron rl;
baseline out = wrregen.baseline_date1 survival = survival cumhaz = cumhaz xbeta = xbeta;
run;
