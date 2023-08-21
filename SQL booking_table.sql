select * from booking_table
select * from user_table

select u.segment ,count(distinct u.user_id) as no_of_user,
count(distinct case when b.line_of_business='flight' and b.booking_date between '2022-04-01'and '2022-04-30' then b.user_id end) as user_who_booking_april2022
from user_table u left join booking_table b 
on u.user_id=b.user_id
group by segment ;


select * from(
select * ,
rank()over(partition by user_id order by booking_date) as rnk
from booking_table) a
where rnk = 1 and line_of_business='hotel' ;


select user_id ,min(booking_date),max(booking_date), 
datediff(day, min(booking_date),max(booking_date)) as no_of_days
from booking_table
group by USER_ID ;


select segment ,
sum (case when line_of_business='flight' then 1 else 0 end) flight_booking,
sum (case when line_of_business='hotel' then 1 else 0 end) hotel_booking
 from booking_table b
inner join user_table u on b.user_id=u.user_id
group by segment


select* from drivers

select id , count(1) as total_rides,
sum(case when end_loc=next_start_location then 1 else 0 end) as profit
from (
select *
, lead (start_loc,1) over(partition by id order by start_time asc) as next_start_location 
from drivers ) a
group by id

with ride as (
select *, row_number() over(partition by id order by start_loc asc) as rnk
from drivers)

select r1.id , count(1) total_ride ,count(r2.id) as profit 
from ride r1
left join ride r2 
on r1.id=r2.id and r1.end_loc=r2.start_loc and r1.rnk+1=r2.rnk
group by r1.id



with cte as( 
select *
,row_number() over(partition by section order by number desc) as rn 
from section_data)
,cte2 as(
select *
,sum(number)over(partition by section) as total 
,max(number)over(partition by section) as section_max
from cte where rn<=2)
select* from(
select *
,dense_rank()over(order by total desc , section_max desc) as rnk
from cte2) a where rnk<=2

create table Ameriprise_LLC

with qualified as(
select teamid , count(1) as no_of_eligible
from Ameriprise_LLC
where criteria1='y' and criteria2='y'
group by teamid
having count(1)>=2)

select al.*, ql.*
,case when criteria1='y' and criteria2='y' and ql.no_of_eligible is not null then 'y' else 'n' end as qualified_team
from Ameriprise_LLC al
left join qualified ql on al.teamid=ql.teamid ;

select *,
 sum(case when criteria1='y' and criteria2='y' then 1 else 0 end) over(partition by teamid) as qualified_team ,
	case when criteria1='y' and criteria2='y' and 
sum(case when criteria1='y' and criteria2='y' then 1 else 0 end) over(partition by teamid) >=2 then 'y' else 'n' end as quaalified
from ameriprise_llc ;



select * from call_start_logs
select * from call_end_logs

select A.phone_number,A.rnk,A.start_time,B.end_time ,DATEDIFF(MINUTE,start_time,end_time) as duration
from
(select * , ROW_NUMBER() over(partition by phone_number order by start_time) as rnk from call_start_logs) A
inner join
(select * , ROW_NUMBER() over(partition by phone_number order by end_time) as rnk from call_end_logs) B
on A.phone_number=B.phone_number and A.rnk=B.rnk

create table business_city (
business_date date,
city_id int
);
delete from business_city;
insert into business_city
values(cast('2020-01-02' as date),3),(cast('2020-07-01' as date),7),(cast('2021-01-01' as date),3),(cast('2021-02-03' as date),19)
,(cast('2022-12-01' as date),3),(cast('2022-12-15' as date),3),(cast('2022-02-28' as date),12);

select * from business_city

with cte as (
select DATEPART(year,business_date) as business_year ,city_id
from business_city)
select  c1.business_year,count(distinct case when c2.city_id is null then c1.city_id end) as new_city
from cte c1
left join cte c2
on c1.business_year>c2.business_year and c1.city_id=c2.city_id
group by c1.business_year


select * from employees
order by dep_id

with cte as(
select dep_id , min(salary) as min_salary ,max(salary) as max_salary 
from employees
group by dep_id )
select e.dep_id 
,max(case when salary=max_salary then emp_name else  null end) as max_salary_emp
,max(case when salary=min_salary then emp_name else null end) as min_salary_emp
from employees e
inner join cte 
on e.dep_id=cte.dep_id
group by e.dep_id


with cte as(
select *
,row_number()over (partition by dep_id order by salary desc ) as rnk_desc
,row_number()over (partition by dep_id order by salary asc ) as rnk_asc
from employees)

select dep_id
,min(case when rnk_desc=1 then emp_name end) as max_salary_emp
,min(case when rnk_asc=1 then emp_name end) as min_salary_emp
from cte
group by dep_id


with cte as(
select * 
,row_number()over(partition by section order by number desc) as rn
from section)

,cte2 as(
select *
,sum(number)over(partition by section) as total
,max(number)over(partition by section) as max_sec
from cte where rn<=2)
select * from(
select *
, DENSE_RANK()over (order by total desc , max_sec desc)as rnk
from cte2 ) a where rnk<=2

create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);
INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');


select * from icc_world_cup

select team_name ,count(1) as no_f_match_played ,sum(winning) as no_of_match_won , count(1)-sum(winning) as no_of_losses
from(
select Team_1 as team_name , case when Team_1=winner then 1 else 0 end as winning
from icc_world_cup
union all
select Team_2 as team_name , case when Team_2=winner then 1 else 0 end as winnig
from icc_world_cup) A
group by team_name
order by no_of_match_won desc

ALTER DATABASE flight MODIFY NAME = flightbooking;