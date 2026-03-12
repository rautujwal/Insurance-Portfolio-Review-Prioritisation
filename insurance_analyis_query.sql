/* I am going to  analyse the pre-cleaned insurance dataset (cleaned and preprocessed using python)
   to identify policies and segments generating disproportionate claim workload,
   so portfolio review effort can be prioritised effectively. */


/* Database setup and data imports: create the project database, define the insurance_data table, and load the csv file into mysql.
   this establishes the working dataset used for the full portfolio review analysis. */
create database project;
use project;
create table if not exists insurance_data(
id int primary key,
Age int,
Gender varchar(50),
Annual_Income decimal,
Marital_Status varchar(66),
Number_of_Dependents int,
Education_Level varchar(60),
Occupation varchar(60),
Health_Score decimal,
Location varchar(70),
Policy_Type varchar(70),
Previous_Claims int,
Vehicle_Age float,
Credit_Score decimal,
Insurance_Duration decimal,
Policy_Start_Date datetime,
Customer_Feedback varchar(70),
Smoking_Status varchar(70),
Exercise_Frequency varchar(70),
Property_Type varchar(70),
Premium_Amount decimal
);
set global local_infile=1;
set autocommit=0;
set unique_checks=0;
set foreign_key_checks=0;
load data local infile 'C:\\Users\\rautu\\OneDrive\\Desktop\\Insurance_cleaned.csv'
into table insurance_data
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(@id,@Age,@Gender,@Annual_Income,@Marital_Status,@Number_of_Dependents,@Education_Level,@Occupation,@Health_Score,@Location,@Policy_Type,@Previous_Claims,@Vehicle_Age,
@Credit_Score,@Insurance_Duration,@Policy_Start_Date,@Customer_Feedback,@Smoking_Status,@Exercise_Frequency,@Property_Type,@Premium_Amount)
set
id=nullif(@id,''),
Age = nullif(@Age, ''),
Gender = nullif(@Gender, ''),
Annual_Income = nullif(@Annual_Income, ''),
Marital_Status = nullif(@Marital_Status, ''),
Number_of_Dependents = nullif(@Number_of_Dependents, ''),
Education_Level = nullif(@Education_Level, ''),
Occupation = nullif(@Occupation, ''),
Health_Score = nullif(@Health_Score, ''),
Location = nullif(@Location, ''),
Policy_Type = nullif(@Policy_Type, ''),
Previous_Claims = nullif(@Previous_Claims, ''),
Vehicle_Age = nullif(@Vehicle_Age, ''),
Credit_Score = nullif(@Credit_Score, ''),
Insurance_Duration = nullif(@Insurance_Duration, ''),
Policy_Start_Date = nullif(@Policy_Start_Date, ''),
Customer_Feedback = nullif(@Customer_Feedback, ''),
Smoking_Status = nullif(@Smoking_Status, ''),
Exercise_Frequency = nullif(@Exercise_Frequency, ''),
Property_Type = nullif(@Property_Type, ''),
Premium_Amount = nullif(@Premium_Amount, '');
set foreign_key_checks=1;
set unique_checks=1;
set autocommit=1;
set global local_infile=0;
select count(*) as total_record from insurance_data;

/* data completeness check: inspect non-null counts across all columns in insurance_data.
   verifying that the imported dataset is complete before starting the analytical workflow. */
SET SESSION group_concat_max_len = 1000000;
set @query=null;
select group_concat(
concat('select ','''',column_name,'''',' as column_name, ',
'count(`',column_name,'`) as total_non_null_count ',
'from insurance_data '
)
separator' union all '
)
into @query
from information_schema.columns
where table_name='insurance_data' and table_schema='project';
select @query;
prepare stmt from @query;
execute stmt;
deallocate prepare stmt;
-- shows that the data are complete for each column.

-- Analytical Workflow Steps:

/* step 1: I am going to create the canonical policy-level analytical base, 
   to define one row per policy, derive claim_band and tenure_band, and create validation flags
   for invalid claims, premium, duration, and duplicate policy ids. */

with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant

from insurance_data
)
SELECT *
FROM policy_base
where is_id_redundant=0 
and is_valid_premium_amount=1
and is_valid_previous_claims=1
and is_valid_duration=1;

/* step 2: Now, I am going to compute portfolio baseline kpis from the cleaned policy base 
   such as total policies, total premium, total claims, average claims per policy,
   claims per $1k premium, and claim-band proportions to define normal portfolio behaviour. */

with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant

from insurance_data
)
select count(*) as total_policies,
sum(Premium_Amount) as total_premium,
sum(Previous_Claims) as total_claims,
avg(previous_claims) as avg_claims_per_policy,
sum(Previous_Claims)/sum(Premium_Amount)*1000 as claims_per_1k_premium,
sum(case when Previous_Claims=0 then 1 else 0 end)/count(*)*100 as pct_policies_0_claims,
sum(case when claim_band='3+' then 1 else 0 end)/count(*)*100 as pct_policies_3plus_claims
from policy_base
where is_id_redundant=0 
and is_valid_premium_amount=1
and is_valid_previous_claims=1
and is_valid_duration=1;
--  establishes a stable portfolio benchmark so all later segment comparisons are evaluated relative to the overall portfolio.

/* step 3: This step includes benchmarking policy_type × location segments against the portfolio baseline,
   comparing segment claims per policy, total claims, total premium, premium share, claim share,
   and lift vs portfolio claims per policy to test for segment-level workload imbalance. */

with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant
from insurance_data
),
filtered_data as (
SELECT *
FROM policy_base
where is_id_redundant=0 
and is_valid_premium_amount=1
and is_valid_previous_claims=1
and is_valid_duration=1
),
portfolio_baseline as (
select
avg(Previous_Claims) as portfolio_claims_per_policy,
sum(Previous_Claims) as portfolio_claims,
sum(Premium_Amount) as portfolio_premium
from filtered_data
)
select
Policy_Type, Location,
sum(Previous_Claims) /count(*) AS claims_per_policy,
sum(Previous_Claims) as total_claims,
sum(Premium_Amount) as total_premium,
sum(Premium_Amount)/max(portfolio_premium)*100 as premium_share_pct,
sum(Previous_Claims)/max(portfolio_claims)*100 as claim_share_pct,
(sum(Previous_Claims)/count(*))/max(portfolio_claims_per_policy) as lift_vs_portfolio_claims_per_policy
from filtered_data
cross join portfolio_baseline
group by Policy_Type, Location
order by lift_vs_portfolio_claims_per_policy desc
-- Finding shows that segment-level behaviour appeared structurally balanced, with claim share closely tracking premium share across segments.


/* step 4: Now  i am going to rank policy_type × location segments by claim share and compute cumulative contribution,
  to assess whether a small subset of segments drives most portfolio claim workload (pareto effect)
   or whether claim contribution remains broadly distributed across segments. */
with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant
from insurance_data
),
portfolio_baseline as (
select
avg(Previous_Claims) as portfolio_claims_per_policy,
sum(Previous_Claims) as portfolio_claims,
sum(Premium_Amount) as portfolio_premium
from policy_base
where is_id_redundant=0 
and is_valid_premium_amount=1
and is_valid_previous_claims=1
and is_valid_duration=1
)
select Policy_Type,
Location,
count(*) as total_claim,
sum(Previous_Claims)/max(portfolio_claims)*100 as claim_share_pct,
rank() over (order by (sum(Previous_Claims)/max(portfolio_claims)*100) desc)  as ranking,
sum(sum(Previous_Claims)/max(portfolio_claims)*100) over (order by (sum(Previous_Claims)/max(portfolio_claims)*100) desc) as cumulative_sum
from policy_base
cross join portfolio_baseline
group by Policy_Type, Location
order by cumulative_sum
-- findings from this step suggests that claim intensity remains nearly constant across tenure groups, indicating lifecycle stage is not a primary driver of claim workload.

/* step 6: Now i am ranking policies by previous_claims and divide them into deciles using ntile(10) to
   measure how claims are distributed across policy buckets to test whether
   claim workload is concentrated within a subset of policies. */
with policy_base as (
  select
    id as policy_id,
    premium_amount,
    previous_claims,
    insurance_duration,
    policy_type,
    property_type,
    location,
    case
      when previous_claims is null or previous_claims < 0 then 'invalid_claim'
      when previous_claims = 0 then '0'
      when previous_claims = 1 then '1'
      when previous_claims = 2 then '2'
      when previous_claims >= 3 then '3+'
      else 'invalid_claim'
    end as claim_band,
    case
      when insurance_duration between 0 and 1 then '0-1'
      when insurance_duration in (2,3) then '2-3'
      when insurance_duration in (4,5) then '4-5'
      when insurance_duration > 5 then '6+'
      when insurance_duration is null or insurance_duration < 0 then 'invalid_duration'
      else 'invalid_duration'
    end as tenure_band,
    case when insurance_duration is null or insurance_duration < 0 then 0 else 1 end as is_valid_duration,
    case when previous_claims is null or previous_claims < 0 then 0 else 1 end as is_valid_previous_claims,
    case when premium_amount is null or premium_amount <= 0 then 0 else 1 end as is_valid_premium_amount,
    case when count(*) over (partition by id) > 1 then 1 else 0 end as is_id_redundant
  from insurance_data
),
cleaned_data as (
  select *
  from policy_base
  where is_id_redundant = 0
    and is_valid_premium_amount = 1
    and is_valid_previous_claims = 1
    and is_valid_duration = 1
),
totals as(
select sum(previous_claims) as portfolio_claims
from cleaned_data
),
bucket as(
select policy_id,
previous_claims,
ntile(10) over(order by previous_claims desc) as claim_bucket
from cleaned_data
)
select
b.claim_bucket,
count(*) as policies_in_policies,
sum(b.previous_claims) as claims_in_bucket,
sum(b.previous_claims)/max(t.portfolio_claims) *100 as claim_share_pct_bucket
from bucket b
cross join totals t
group by b.claim_bucket
order by b.claim_bucket
-- this step suggests strong concentration observed at policy level, roughly 55% of total claims generated by the top ~30% of policies.

/* step 7: comparing claim behaviour across tenure cohorts.
   I am going to compute policy count, total claims, claims per policy, claim share, and premium share
   by tenure_band to assess whether claim workload concentration is associated with policy tenure. */
with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant
from insurance_data
),
cleaned_data as(
select * 
from policy_base
where is_id_redundant=0
and is_valid_premium_amount=1
and is_valid_previous_claims = 1
and is_valid_duration = 1
),
portfolio_data as(
select sum(Previous_Claims) as total_claims,
sum(Premium_Amount) as total_premium,
count(*) as total_records
from cleaned_data
)
select 
c.tenure_band as tenure_bands,
count(*) as policy_count,
sum(c.Previous_Claims) as total_claim,
avg(c.Previous_Claims) as claims_per_policy_band,
sum(c.Previous_Claims)/max(p.total_claims)*100 as claim_share_percent,
sum(c.Premium_Amount)/max(p.total_premium)*100 as premium_share_percent
from cleaned_data c
cross join portfolio_data p
group by c.tenure_band;
-- claim intensity remains nearly constant across tenure groups, indicating lifecycle stage is not a primary driver of claim workload.

/* step 8: assess stability of policy_type × location segment averages.
   I will measure policy count, average claims, standard deviation, and coefficient of variation
   to confirm whether earlier segment-level findings are reliable and structurally stable. */
with policy_base as(
select id as policy_id, Premium_Amount, Previous_Claims, Insurance_Duration,
Policy_Type, Property_Type, Location,
case
 when Previous_Claims is null or Previous_Claims<0 then 'invalid_claim'
 when Previous_Claims=0 then '0'
 when Previous_Claims=1 then '1'
 when Previous_Claims=2 then '2'
 when Previous_Claims>=3 then '3+'else "invalid_claim" end as claim_band,
case 
when Insurance_Duration between 0 and 1 then '0-1'
when Insurance_Duration in (2,3) then '2-3'
when Insurance_Duration in (4,5) then '4-5' 
when Insurance_Duration>5 then '6+'  
when Insurance_Duration IS NULL OR Insurance_Duration < 0 THEN 'invalid_duration'
else 'invalid_duration'
end as tenure_band,
 
case when Insurance_Duration is null or Insurance_Duration<0 then 0 else 1 end as is_valid_duration,
case when Previous_Claims is null or Previous_Claims<0 then 0 else 1 end as is_valid_previous_claims,
case when Premium_Amount is null or Premium_Amount<=0 then 0 else 1 end as is_valid_premium_amount,
case when count(*) over (partition by id)>1 then 1 else 0 end as is_id_redundant
from insurance_data
),
cleaned_data as(
select * 
from policy_base
where is_id_redundant=0
and is_valid_premium_amount=1
and is_valid_previous_claims = 1
and is_valid_duration = 1
)
select
Policy_Type,Location,
count(*) as total_policy_count,
avg(Previous_Claims) as average_claims,
stddev(Previous_Claims) as standard_deviation_claims,
stddev(Previous_Claims)/avg(Previous_Claims) as coefficient_of_variation
from cleaned_data
group by Policy_Type, Location
-- segment metrics show similar variability levels and balanced policy counts, confirming that earlier segment-level comparisons are structurally stable.

/* conclusion:
   The above analysis suggests segment and tenure analyses showed balanced behaviour,while policy-level decile analysis revealed meaningful concentration of claims.
   Therefore, review prioritisation should focus primarily at the policy level. */