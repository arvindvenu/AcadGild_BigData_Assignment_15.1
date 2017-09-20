CREATE DATABASE IF NOT EXISTS assignment_15_1;

USE assignment_15_1;

-- create the emp_details table
create table IF NOT EXISTS emp_details
(
	emp_name string,
	skill string,
	exp int,
	location string
)
row format delimited
fields terminated by ',';

-- load the input into the emp_details table
load data inpath '/user/arvind/hive/acadgild/assignments/assignment_15.1/input/input.txt'
into table emp_details;


-- create the emp_details_partitioned table
create table IF NOT EXISTS emp_details_partitioned
(
	emp_name string,
	exp int,
	location string
)
partitioned by (skill string);

-- set hive.exec.dynamic.partition.mode as nonstrict to support dynamic partition
SET hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- insert data from emp_details to emp_details_partitioned
insert overwrite table emp_details_partitioned
partition(skill)
select emp_name,exp,location,skill from emp_details;

-- set the number of reduce tasks to 2
SET mapred.reduce.tasks=2;

-- Use CLUSTER BY to control the output that goes to a reducer
-- If GROUP BY and CLUSTER BY is done on the same column, good parallelism can be achieved
-- because your aggregations in a single group will run only in separate reducers
-- store the output in a local directory
INSERT OVERWRITE DIRECTORY '/user/arvind/hive/acadgild/assignments/assignment_15.1/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select skill, COUNT(1) AS count from emp_details_partitioned GROUP BY skill CLUSTER BY skill;
