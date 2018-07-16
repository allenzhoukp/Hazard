/* Regression.
   NOTE 2018-7-10
   The most important problem is why regressing like that.
   Multiple lines can represent one subject, since covariates change over time.
   (begin, end) indicates the time order.
   When an event happens, isStart as the censoring variable becomes one of the values in the parenthesis,
   and the data is censored here.
   Since covariates change every day, every one single day requires one observatiion, and
   the subject itself is an investor x warrant Cartesian product,
   the input dataset naturally becomes investor x warrant x date Cartesian product.

   See SAS Help: PHREG procedure -> Detals -> Counting Process Style of Input for the example that SAS provides.

   TODO Why the censoring value takes 0, not 1? It is obvious that start=1 when
        an investment is made and the following process is censored.

   NOTE This regression is THE MOST TIME CONSUMING part, est. 1 week on an SSD. */

/* Table 3, Column 1 */

/* Explanation:
class indicates which variables are in category type.
model indicates the main regression,
    (begin, end) indicates start and end timestamp in which the subject is in hazard,
    isStart(1) is the censoring dummy.
    ties=efron determines EFRON function.
baseline indicates the output dataset, and the variable names of the output. */

proc phreg data = warrant.warrant_combinedOneCycle;
class securitycode date maturity;
model (begin, end) * isStart(0) =
    returnLag1Pseudo isReturnLag1PseudoPositive
    lag1Return lag1Turnover lag1AdjFundamental
    securitycode date maturity
        / ties=efron rl;
baseline out = warrant.baselineOneCycle survival = survival cumhaz = cumhaz xbeta = xbeta;
run;

/* Table 3, Column 2 */
/* Similarly. */

proc phreg data = warrant.warrant_combinedTwoCycle;
class securitycode date maturity;
model (begin, end) * isStart(0) =
    returnLag1Pseudo returnLag2Pseudo
    isReturnLag1PseudoPositive isReturnLag2PseudoPositive
    lag1Return lag2Return lag1Turnover lag2Turnover lag1AdjFundamental
    securitycode date maturity
        / ties=efron rl;
baseline out = warrant.baseline0woCycle survival = survival cumhaz = cumhaz xbeta = xbeta;
run;
