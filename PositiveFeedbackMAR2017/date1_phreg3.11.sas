/* Main Regression: Table 3, Column 1.
   D1 is I(lag1_return > 0).
   class: determines which variables are class variables.
   Argument: ties=efron (determines the approximate likelihood function Efron(1977). )
             rl Produce confidence intervals for hazard ratios.
   Output creates baseline as baseline function estimates. See help files.

   TODO
   Why use (BEGIN, END) as the time that the investor is subject to risk?
   BEGIN=1 is the SECOND date of a psudocycle. Why is that?
   Seriously I don't know why we should regress like that. This procedure doesn't seem to be different as in stata.

   TODO
   where does n_1 come from? */

proc phreg data=warrant.warrant_expand1_1;
class securitycode date n_1;
model (begin,end)*start(0)=lag1_return  D1   lag1_market_ret lag1_turnover lag1_adjfundamental  securitycode date n_1 / ties=efron rl;
baseline out=baseline survival=survival cumhaz=cumhaz xbeta=xbeta;
run;
