options nodate nosource nonotes;

data raw;
set minzu.R1 minzu.R2 minzu.R3 minzu.R4 minzu.R5 minzu.R6 minzu.R7 minzu.R8 minzu.R9 minzu.R10 minzu.R11 minzu.R12 minzu.R13 minzu.R14 minzu.R15 minzu.R16 minzu.R17 minzu.R18 minzu.R19 minzu.R20 minzu.R21 minzu.R22 minzu.R23 minzu.R24 minzu.R25 minzu.R26 minzu.R27 minzu.R28 minzu.R29 minzu.R30 minzu.R31 minzu.R32 minzu.R33 minzu.R34 minzu.R35 minzu.R36 minzu.R37 minzu.R38 minzu.R39 minzu.R40 minzu.R41 minzu.R42 minzu.R43 minzu.R44 minzu.R45 minzu.R46 minzu.R47 minzu.R48 minzu.R49 minzu.R50 minzu.R51 minzu.R52 minzu.R53 minzu.R54 minzu.R55 minzu.R56 minzu.R57 minzu.R58 minzu.R59 minzu.R60 minzu.R61 minzu.R62 minzu.R63 minzu.R64 minzu.R65 minzu.R66 minzu.R67 minzu.R68 minzu.R69 minzu.R70 minzu.R71 minzu.R72 minzu.R73 minzu.R74 minzu.R75 minzu.R76 minzu.R77 minzu.R78 minzu.R79 minzu.R80 minzu.R81 minzu.R82 minzu.R83 minzu.R84 minzu.R85 minzu.R86 minzu.R87 minzu.R88 minzu.R89 minzu.R90 minzu.R91 minzu.R92 minzu.R93 minzu.R94 minzu.R95 minzu.R96 minzu.R97 minzu.R98 minzu.R99 minzu.R100 minzu.R101 minzu.R102 minzu.R103 minzu.R104 minzu.R105 minzu.R106 minzu.R107 minzu.R108 minzu.R109 minzu.R110 minzu.R111 minzu.R112 minzu.R113 minzu.R114 minzu.R115 minzu.R116 minzu.R117 minzu.R118 minzu.R119 minzu.R120 minzu.R121 minzu.R122 minzu.R123 minzu.R124 minzu.R125 minzu.R126 minzu.R127 minzu.R128 minzu.R129 minzu.R130 minzu.R131 minzu.R132 minzu.R133 minzu.R134 minzu.R135 minzu.R136 minzu.R137 minzu.R138 minzu.R139 minzu.R140 minzu.R141 minzu.R142 minzu.R143 minzu.R144 minzu.R145 minzu.R146 minzu.R147 minzu.R148 minzu.R149 minzu.R150 minzu.R151 minzu.R152 minzu.R153 minzu.R154 minzu.R155 minzu.R156 minzu.R157 minzu.R158 minzu.R159 minzu.R160 minzu.R161 minzu.R162 minzu.R163 minzu.R164 minzu.R165 minzu.R166 minzu.R167 minzu.R168 minzu.R169 minzu.R170 minzu.R171 minzu.R172 minzu.R173 minzu.R174 minzu.R175 minzu.R176 minzu.R177 minzu.R178 minzu.R179 minzu.R180 minzu.R181 minzu.R182 minzu.R183 minzu.R184 minzu.R185 minzu.R186 minzu.R187 minzu.R188 minzu.R189 minzu.R190 minzu.R191 minzu.R192 minzu.R193 minzu.R194 minzu.R195 minzu.R196 minzu.R197 minzu.R198 minzu.R199 minzu.R200 minzu.R201 minzu.R202 minzu.R203 minzu.R204 minzu.R205 minzu.R206 minzu.R207 minzu.R208 minzu.R209 minzu.R210 minzu.R211 minzu.R212 minzu.R213 minzu.R214 minzu.R215 minzu.R216 minzu.R217 minzu.R218 minzu.R219 minzu.R220 minzu.R221 minzu.R222 minzu.R223 minzu.R224 minzu.R225 minzu.R226 minzu.R227 minzu.R228 minzu.R229 minzu.R230 minzu.R231 minzu.R232 minzu.R233 minzu.R234 minzu.R235 minzu.R236 minzu.R237 minzu.R238;
if substr(code,1,4) in ('0380','5809') and length(fundacc)>=6 and length(stockacc)>=8;
run;

/***********************************************************
Identify
***********************************************************/

Data network802 network803 network804 network805 network806 network807 network808 network809 network810 network811 network812 network813 network815 network816 network817 network818 network819 network820 network821 network822 network823 network824 network825 network826 network827 network828 network829 network831 network832 network833 network834 network835 network836 network837 network838 network850 network851 network852 network853 network855 network856 network858 network859 network862 network868 network878 network879;
Set raw;
if network="802" then output network802;
if network="803" then output network803;
if network="804" then output network804;
if network="805" then output network805;
if network="806" then output network806;
if network="807" then output network807;
if network="808" then output network808;
if network="809" then output network809;
if network="810" then output network810;
if network="811" then output network811;
if network="812" then output network812;
if network="813" then output network813;
if network="815" then output network815;
if network="816" then output network816;
if network="817" then output network817;
if network="818" then output network818;
if network="819" then output network819;
if network="820" then output network820;
if network="821" then output network821;
if network="822" then output network822;
if network="823" then output network823;
if network="824" then output network824;
if network="825" then output network825;
if network="826" then output network826;
if network="827" then output network827;
if network="828" then output network828;
if network="829" then output network829;
if network="831" then output network831;
if network="832" then output network832;
if network="833" then output network833;
if network="834" then output network834;
if network="835" then output network835;
if network="836" then output network836;
if network="837" then output network837;
if network="838" then output network838;
if network="850" then output network850;
if network="851" then output network851;
if network="852" then output network852;
if network="853" then output network853;
if network="855" then output network855;
if network="856" then output network856;
if network="858" then output network858;
if network="859" then output network859;
if network="862" then output network862;
if network="868" then output network868;
if network="878" then output network878;
if network="879" then output network879;
run;

%macro identify(series);

*将缺位的数据整理好;

data network&series;set network&series;
if length(stockacc)=8 then stockacc='00'||stockacc;
name=trim(name);fund=substr(fundacc,1,7);
proc sort;by name fundacc;
data temp;set network&series(keep=name fundacc fund);
by name fundacc;if first.fundacc;

data temp;set temp;if name=lag(name) and fund=lag(fund) and fundacc^=lag(fundacc);
data temp;set temp(rename=(fundacc=f1));by name;if last.name;
data network&series;merge network&series temp;by name fund;
data network&series(drop=f1 fund);set network&series;if f1^='' then fundacc=f1;


*排序得到不重复的(fundacc,stockacc)组合;
proc sort;
by fundacc stockacc;
data temp_acc;
set network&series(keep=fundacc stockacc);
by fundacc stockacc;
if last.stockacc;

*用宏变量obs记录观测数，首先将每一个组合都认为是不同的人(y);
data t;
set temp_acc nobs=nobs;
if _n_=1 then y=0;
y+1;
call symput('obs',nobs);
run;

*使用宏mingzhu识别投资者，使得同一个人有同一个y;
%mingzhu;

data network&series(drop=y);merge network&series t;by fundacc stockacc;id=y+1000000*&Series;
proc sort;by fundacc code date time;run;

data t;set t;Network=&Series;id=y+1000000*&Series;
data Correspondence;set Correspondence t;run;

%mend identify;

%macro mingzhu;

*从第一个观测运行到最后一个观测，宏变量obs记录了总观测数;
%let i=1;
%do %while(&i<=&obs);

*将第i个观测的fundacc和stockacc记录入宏变量f和s;
data _null_;
set t(where=(y=&i));
call symput('f',fundacc);
call symput('s',stockacc);
run;

*找出所有与第i个观测具有相同fundacc或者stockacc的观察;
data temp_t;
set t;
where fundacc=symget('f') or stockacc=symget('s');
run;

*将复合这个条件的所有观测的y改成他们之中最小的y(即min(y));
proc sql noprint;
select min(y) into :my from temp_t;
update t set y=&my where y in(select y from temp_t);
quit;

*将i加1，进入下一次循环;
%let i=%eval(&i+1);

%end;

%mend mingzhu;

data Correspondence;delete;run;

%identify(802);
%identify(803);
%identify(804);
%identify(805);
%identify(806);
%identify(807);
%identify(808);
%identify(809);
%identify(810);
%identify(811);
%identify(812);
%identify(813);
%identify(815);
%identify(816);
%identify(817);
%identify(818);
%identify(819);
%identify(820);
%identify(821);
%identify(822);
%identify(823);
%identify(824);
%identify(825);
%identify(826);
%identify(827);
%identify(828);
%identify(829);
%identify(831);
%identify(832);
%identify(833);
%identify(834);
%identify(835);
%identify(836);
%identify(837);
%identify(838);
%identify(850);
%identify(851);
%identify(852);
%identify(853);
%identify(855);
%identify(856);
%identify(858);
%identify(859);
%identify(862);
%identify(868);
%identify(878);
%identify(879);


*数据整理;

data warrant(drop=stocktype bs fundacc stockacc tax stroke transferfee entrustfee otherfee name stdcommission code id date); set network802 network803 network804 network805 network806 network807 network808 network809 network810 network811 network812 network813 network815 network816 network817 network818 network819 network820 network821 network822 network823 network824 network825 network826 network827 network828 network829 network831 network832 network833 network834 network835 network836 network837 network838 network850 network851 network852 network853 network855 network856 network858 network859 network862 network868 network878 network879;
Date1=mdy(substr(date,5,2),substr(date,7,2),substr(date,1,4));
Securitycode=code;Fundacctnum=id;
format date1 yymmdd10.;
run;

/***********************************************************
data a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11; set warrant;
if business='红股入帐' then output a1;
if business='权证入帐' then output a2;
if business='权证上市' then output a3;
if business='权证上市转入' then output a4;
if business='认沽行权' then output a5;
if business='托管转出' then output a6;
if business='托管转入' then output a7;
if business='行权权证' then output a8;
if business='余额入账' then output a9;
if business='转托转出' then output a10;
if business='转托转入' then output a11;
run;

a1---a4表示股权分置改革所得到的权证；a6 a7 a10 a11是转托管的情况，为方便计算，记为当天的15:00:00，视为按照收盘价卖出
a5 a8为行权的情况，共两个观测值；a9也是类似转托管的情况，视为按照收盘价买入或者卖出，记为当天的15:00:00
***********************************************************/

data warrant; set warrant;
rename date1=Date;
if securitycode not in ('038007','038009','038010','038011','038012','038013');
if business in ('红股入帐','权证入帐','权证上市','权证上市转入') then time='90000';
if business in ('托管转出','托管转入','转托转出','转托转入','余额入账') then time='150000';
run;

proc sql; create table warrant as
select a.*, b.clpr from warrant a left join warrant.wrntqttn b on a.securitycode=b.wrntcd and a.date=b.date;
quit;

data warrant(drop=network clpr); set warrant;
if business in ('托管转出','托管转入','转托转出','转托转入','余额入账') then price=clpr;
if length(TIME)=1 then var3=HMS(substr(TIME,1,1),0,0);
	else if length(TIME)=2 then var3=HMS(substr(TIME,1,2),0,0);
	else if length(TIME)=5 then var3=HMS(substr(TIME,1,1),substr(TIME,2,2),substr(TIME,4,2));
	else var3=HMS(substr(TIME,1,2),substr(TIME,3,2),substr(TIME,5,2));format var3 time.;
Branch=floor(fundacctnum/1000000);
run;

data warrant; set warrant;
if business='证券买入' then Type=1;
   else if business='证券卖出' then Type=2;
   else if business in ('红股入帐','权证入帐','权证上市','权证上市转入') then Type=3;
   else if business in ('托管转出','托管转入','转托转出','转托转入','余额入账') then Type=4;
   else Type=5;
drop time business;
run;
data warrant; set warrant;
rename var3=Time;
run;

proc sort data=warrant;
by fundacctnum securitycode date time;
run;

data warrant; set warrant;
if fundacctnum=lag(fundacctnum) and securitycode=lag(securitycode) and date=lag(date) and time=lag(time) and amount=lag(amount) and position=lag(position) then delete;
run;

proc sql; create table warrant as 
select branch, fundacctnum, securitycode, date, time, type, amount, position, price, value, commission, entrust, invtype from warrant; quit;

data warrant; set warrant;
if amount<0 and lag(position)=0 then delete;
run;
data warrant; set warrant; by fundacctnum securitycode date time;
if first.securitycode then Position1=0;
Position1+amount;
run;

data temp; set warrant;
if position1<0;
run;
data temp; set temp; var=securitycode||fundacctnum; run;
data warrant; set warrant; var=securitycode||fundacctnum; run;
proc sql; create table temp1 as
select * from warrant where warrant.var in (select distinct var from temp); quit;
proc sql; create table temp2 as
select * from warrant where warrant.var not in (select distinct var from temp); quit;

data temp1; set temp1;
by fundacctnum securitycode date time;
Amount1=position-lag(position);
if first.securitycode then Amount1=Position;
drop position1 var;
if amount1^=0;
run;
data temp1; set temp1; drop amount; run;
data temp1; set temp1; rename amount1=Amount; run;
data temp2; set temp2; drop position var; run;
data temp2; set temp2; rename position1=Position; run;
data t1; set temp1; if position<0; run;
data temp1; set temp1; var=securitycode||fundacctnum; run;
data t1; set t1; var=securitycode||fundacctnum; run;
proc sql; create table temp1 as select temp1.* from temp1 where temp1.var not in (select var from t1);quit;
data temp1; set temp1; drop var; run;

data temp1; set temp1;
Value=abs(price*amount);
if Type=1 or type=2 then Type=0;
Position1=position-0;
run;
data temp2; set temp2;
if Type=1 or type=2 then Type=0;
Amount1=Amount-0;
run;
proc sql; create table warrant1 as 
select branch, fundacctnum, securitycode, date, time, type, amount, position1 as position, price, value, commission, entrust, invtype from temp1; quit;
proc sql; create table warrant2 as 
select branch, fundacctnum, securitycode, date, time, type, amount1 as amount, position, price, value, commission, entrust, invtype from temp2; quit;
data warrant; set warrant1 warrant2; run;

proc sort data=warrant; by fundacctnum securitycode date time; quit;


























