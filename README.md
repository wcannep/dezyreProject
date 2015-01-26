# dezyreProject
Final Project for Hadoop Class

Certificate in Big Data and Hadoop Class Project

This project is to look at US efforts to reduce Heathcare-Associated Infections (HAI) in Year 2013.  
4 measures are included:
hospital-acquired central line-associated bloodstream infections (CLABSI) 
Methicillin-resistant Staphylococcus aureus (MRSA)
catheter-associated urinary tract infections (CAUTI)
Clostridium difficile (C.diff.) 

Data source: Medicare.gov, Official Hospital Compare Data

These are the steps taken to process the data


Step 1: Download data source as csv files:
	HAI_Hospital.csv
	HAI_State.csv
	HAI_State_Prior.csv

Step 2: mkdir directory medicare.  Copy files into HDFS

Step 3: Use Hive to create database medicare
	Run LoadStagingTables.hsql
	This is to create and load staging tables: 
	hai_hospital_stg
	hai_state_stg
	Hai_state_prior_yr_stg

Step 4: Run Create_Load_tables.hsql
This sript is to:
•	Create tables hai_hosptial and populate from hai_hospital_stg table
filtering on measureid like '%SIR' (only include standardized infection ratio) and score <> 'Not Available', partitioned by measureid
•	Create table hai_hospital_cnt_by_state and populate from hai_hospital_stg table group by state to get the number of participating hospitals
•	Create table hai_state and populate from hai_state_stg filtering on measureid like '%SIR' (only include standardized infection ratio) and score <> 'Not Available'
•	Create table hai_state_prior_yr and populate from hai_state_prior_yr_stg filtering on score <> 'Not Available'

Step 5: Run Hai_Analysis
This script is to create table hai_analysis and populate by joining hai_state, hai_hospital_cnt_by_state and hai_state_prior_yr to create one dataset for data visualization.

Step 6: Use sqoop to export hai_analysis to mysql:
	sqoop export -m 1 --connect jdbc:mysql://192.165.155.1/medicare --username root --password root --table HAI_Analysis --export-dir /user/cloudera/medicare/hai_analysis --input-fields-terminated-by '\t' 





 

