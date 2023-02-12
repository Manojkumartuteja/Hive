#Hive project:-

 

 
#Step 1:-
#Create DDL hql script

vi proj_hive_manoj.hql
create database hive_proj_manoj_sports;
Use hive_proj_manoj_sports; 
Create table IF NOT EXISTS athlete_events_final (
Id int,
Name string,
Sex string,
Age int,
Height int,
Team string,
NOC string,
Games string,
Year int,
Season string,
Sport string,
Event string,
Medal string)
COMMENT 'athlete_events_table'
ROW FORMAT SERDE
 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
("separater char" = ",",
"quote chare" = "\"")TBLPROPERTIES("skip.header.line.count"="1") 

 

# Run the hql script
Hive -f proj_hive_manoj.hql

 



#Step 2
#Create DML scripts.

vi Load_athlete_hive_manoj.hql
Use hive_proj_manoj_sports; 
load data inpath ‘/user/manojkumartuteja6614/Hive_projects/athlete_events.csv’ into table athlete_events_final;
:wq!
 

# Run the hql script
Hive -f Load_athlete_hive_manoj.hql

 
#Step3 
#Create anotherDDL hql script

vi noc_loc_hive_manoj.hql
Use hive_proj_manoj_sports; 
Create table IF NOT EXISTS noc_regions (
NOC string,
Region string,
Notes string)
COMMENT ‘noc regions table’
Row format delimited fields terminated by “,”
TBLPROPERTIES ( “skip.header.line.count” = “1”);
:wq!

 
# Run the hql script
Hive -f noc_loc_hive_manoj.hql
 


Step 4
Create another DML scripts.

vi Load_noc_hive_manoj.hql
Use hive_proj_manoj_sports; 
load data inpath ‘/user/manojkumartuteja6614/Hive_projects/noc_locations.csv’ into table noc_regions;
:wq!

 
# Run the hql script
Hive -f Load_noc_hive_manoj.hql
 
 
#Use case 1:-
#As per age, display the medals gold.

Select 
Insert overwrite directory ‘/user/manojkumartuteja6614/medals_gold_op/’ select count(medal) as Medals, Age from athlete_events_final where Medal = ‘Gold’ group by Age order by Medals desc;
Hive_prpjects
 
 





 

