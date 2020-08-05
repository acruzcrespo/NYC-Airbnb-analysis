-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ###Step 1: Download Data
-- MAGIC 
-- MAGIC The dataset was selected from Kaggle. 

-- COMMAND ----------

-- MAGIC %sh pwd

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC rm -rf /dbfs/data
-- MAGIC mkdir /dbfs/data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.library.installPyPI("kaggle") 
-- MAGIC dbutils.library.restartPython()

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC 
-- MAGIC cd ~
-- MAGIC rm -rf .kaggle
-- MAGIC mkdir .kaggle
-- MAGIC 
-- MAGIC cd .kaggle
-- MAGIC touch kaggle.json
-- MAGIC echo '{"username":"ashleyccrespo","key":"31d75a6e210bab29f65396347a546231"}' > kaggle.json
-- MAGIC chmod 600 kaggle.json
-- MAGIC 
-- MAGIC cat kaggle.json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #once you installed kaggle using python, you can initialize and authenticated the kaggle api instance in order to download (in this case) the public dataset
-- MAGIC from kaggle.api.kaggle_api_extended import KaggleApi
-- MAGIC api = KaggleApi()
-- MAGIC api.authenticate()
-- MAGIC 
-- MAGIC # Download all files of a dataset
-- MAGIC # Signature: dataset_download_files(dataset, path=None, force=False, quiet=True, unzip=False)
-- MAGIC api.dataset_download_files('dgomonov/new-york-city-airbnb-open-data',path='/dbfs/data',unzip='True')

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #read, load and display the csv file in order to make it accessible to work with the data in SQL
-- MAGIC #previous steps were for creating the space (here in the notebook) to work with a public dataset. install kaggle library in python #to install the kaggle api to attach user account and being able to download a public dataset into the notebook. Once the dataset #was download and unzip, the dataset was move to dbfs and formatted to in python to be able to access the data and analyze it. 
-- MAGIC 
-- MAGIC airbnbData = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/data/AB_NYC_2019.csv')
-- MAGIC # airbnbData.createOrReplaceTempView('airbnb_data')
-- MAGIC display(airbnbData)
-- MAGIC 
-- MAGIC airbnbData.write.saveAsTable("airbnb_data")

-- COMMAND ----------

select * from airbnb_data limit 10

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Before start analyzing the data, lets first clean the data. I add missing information, i.e. neighbourhood that didn't were in any neighbourhood group. 

-- COMMAND ----------

-- select neighbourhood_group as District
-- from airbnb_data
-- group by neighbourhood_group

select name, neighbourhood_group
from airbnb_data




-- COMMAND ----------

create or replace temporary view boroughs as
  select 'Harlem' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Bedford-Stuyvesant' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Hell\'s Kitchen' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Upper East Side' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Elmhurst' as neighbourhood_group, 'Queens' as borough union all
  select 'Williamsburg' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'East Harlem' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Mott Haven' as neighbourhood_group, 'Bronx' as borough union all
  select 'Canarsie' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Midtown' as neighbourhood_group, 'Manhattan' as borough union all
  select 'East Village' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Washington Heights' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Bushwick' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Ditmars Steinway' as neighbourhood_group, 'Queens' as borough union all
  select 'Woodhaven' as neighbourhood_group, 'Queens' as borough union all
  select 'Midwood' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Flushing' as neighbourhood_group, 'Queens' as borough union all
  select 'Crown Heights' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Long Island City' as neighbourhood_group, 'Queens' as borough union all
  select 'Upper West Side' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Morningside Heights' as neighbourhood_group, 'Manhattan' as borough union all
  select 'NoHo' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Prospect-Lefferts Gardens' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Longwood' as neighbourhood_group, 'Bronx' as borough union all
  select 'Astoria' as neighbourhood_group, 'Queens' as borough union all
  select 'Greenwich Village' as neighbourhood_group, 'Manhattan' as borough union all
  select 'East Elmhurst' as neighbourhood_group, 'Queens' as borough union all
  select 'Cypress Hills' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Pelham Gardens' as neighbourhood_group, 'Bronx' as borough union all
  select 'Chelsea' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Jackson Heights' as neighbourhood_group, 'Queens' as borough union all
  select 'Flatbush' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Douglaston' as neighbourhood_group, 'Queens' as borough union all
  select 'Nadia' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Little Neck' as neighbourhood_group, 'Queens' as borough union all
  select 'Clinton Hill' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Briarwood' as neighbourhood_group, 'Queens' as borough union all
  select 'Randall Manor' as neighbourhood_group, 'Staten Island' as borough union all
  select 'Edgemere' as neighbourhood_group, 'Queens' as borough union all
  select 'Carmen' as neighbourhood_group, 'Staten Island' as borough union all
  select 'Bath Beach' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Evelyn' as neighbourhood_group, 'Staten Island' as borough union all
  select 'Fort Greene' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Gramercy' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Throgs Neck' as neighbourhood_group, 'Bronx' as borough union all
  select 'South Ozone Park' as neighbourhood_group, 'Queens' as borough union all
  select 'D' as neighbourhood_group, 'null' as borough union all
  select 'Queens Village' as neighbourhood_group, 'Queens' as borough union all
  select 'Prospect Heights' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'West Village' as neighbourhood_group, 'Manhattan' as borough union all
  select 'East Flatbush' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Cambria Heights' as neighbourhood_group, 'Queens' as borough union all
  select 'Eltingville' as neighbourhood_group, 'Staten Island' as borough union all
  select 'Woodside' as neighbourhood_group, 'Queens' as borough union all
  select 'Stuyvesant Town' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Springfield Gardens' as neighbourhood_group, 'Queens' as borough union all
  select 'Maspeth' as neighbourhood_group, 'Queens' as borough union all
  select 'Inwood' as neighbourhood_group, 'Manhattan' as borough union all
  select 'William Hakan' as neighbourhood_group, 'Staten Island' as borough union all
  select 'Borough Park' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'East New York' as neighbourhood_group, 'Brooklyn' as borough union all
  select '197400421' as neighbourhood_group, 'null' as borough union all
  select 'Murray Hill' as neighbourhood_group, 'Manhattan' as borough union all
  select '194716858' as neighbourhood_group, 'null' as borough union all
  select 'Red Hook' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Greenpoint' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Brooklyn Heights' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'Arverne' as neighbourhood_group, 'Queens' as borough union all
  select 'Concourse Village' as neighbourhood_group, 'Bronx' as borough union all
  select 'Seth' as neighbourhood_group, 'Brooklyn' as borough union all
  select 'SoHo' as neighbourhood_group, 'Manhattan' as borough union all
  select 'Krista' as neighbourhood_group, 'null' as borough

-- COMMAND ----------

create or replace temporary view airbnb_data_with_boroughs as
  select a.*,case when a.neighbourhood_group not in ('Manhattan','Queens','Bronx','Brooklyn','Staten Island') then b.borough else a.neighbourhood_group end as borough
  from airbnb_data a
  left outer join boroughs b
  on a.neighbourhood_group = b.neighbourhood_group

-- COMMAND ----------

select *
from airbnb_data_with_boroughs

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select id, host_name, last_review
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where last_review is null

-- COMMAND ----------

select COALESCE(last_review, 0) 
from airbnb_data_with_boroughs

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### Step 2: Analyze the Data
-- MAGIC 
-- MAGIC What is the borough with more listings?
-- MAGIC What is the borough with fewer listings?
-- MAGIC What is the borough with more hosts?
-- MAGIC What is the borough with fewer hosts?
-- MAGIC What is the room_type most listed?
-- MAGIC What is the borough with the highest price listed?
-- MAGIC What is the avg price listed per room_type?
-- MAGIC What is the room_type with the highest price listed?
-- MAGIC What are the top 5 common price per room_type?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select neighbourhood_group, count(id) as listing_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC group by neighbourhood_group
-- MAGIC order by listing_count desc 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select neighbourhood_group, count(distinct host_id) as host_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC group by neighbourhood_group
-- MAGIC order by host_count desc 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select borough, count(id) as listing_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC group by borough
-- MAGIC order by listing_count desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select borough, count(distinct host_id) as host_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where borough in ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island')
-- MAGIC group by borough
-- MAGIC order by host_count desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select borough, count(distinct host_id) as host_count, round(count(*)/count(distinct host_id), 2) as listings_per_host
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where borough in ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island')
-- MAGIC group by borough
-- MAGIC order by host_count desc

-- COMMAND ----------

select host_id, count(distinct borough), count(*)
from airbnb_data_with_boroughs
group by 1
order by 2 desc
limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select room_type, count(room_type) as room_type_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where room_type in ('Entire home/apt', 'Private room', 'Shared room')
-- MAGIC group by room_type
-- MAGIC order by room_type_count desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select borough, count(price) as price_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC group by borough
-- MAGIC order by price_count desc
-- MAGIC limit 5 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select neighbourhood_group, count(price) as price_count
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC group by neighbourhood_group
-- MAGIC order by price_count desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select room_type,  avg(price) as avg_price
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where room_type in ('Entire home/apt', 'Private room', 'Shared room')
-- MAGIC group by room_type
-- MAGIC order by avg_price desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select room_type, borough, avg(price) as avg_price
-- MAGIC from airbnb_data_with_boroughs
-- MAGIC where room_type in 
-- MAGIC ('Entire home/apt', 'Private room', 'Shared room')
-- MAGIC group by room_type, borough
-- MAGIC order by 1 asc, 2 asc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC with mode as (
-- MAGIC     select room_type, price, count(*) as count, 
-- MAGIC     row_number() over (partition by room_type order by count(*) desc) as rn
-- MAGIC     from airbnb_data_with_boroughs
-- MAGIC     where room_type in ('Entire home/apt', 'Private room', 'Shared room')
-- MAGIC     group by 1,2
-- MAGIC     order by 1 asc, 3 desc
-- MAGIC ), top5 as (
-- MAGIC   select room_type, collect_list(price) as top_prices
-- MAGIC   from mode
-- MAGIC   where rn >= 1 and rn <= 5
-- MAGIC   group by 1
-- MAGIC )
-- MAGIC select l.room_type,  max(l.price) as max_price, min(l.price) as min_price, 
-- MAGIC avg(l.price) as avg_price, m.price as mode_price, t.top_prices
-- MAGIC from airbnb_data_with_boroughs l
-- MAGIC inner join mode m
-- MAGIC on l.room_type = m.room_type
-- MAGIC inner join top5 t
-- MAGIC on l.room_type = t.room_type
-- MAGIC where l.room_type in ('Entire home/apt', 'Private room', 'Shared room')
-- MAGIC and rn=1
-- MAGIC group by l.room_type, m.price, t.top_prices
-- MAGIC order by max_price desc

-- COMMAND ----------

-- MAGIC  %sql show tables in default

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC with host as (
-- MAGIC     select host_id, count(id) as listing_count 
-- MAGIC     from airbnb_data_with_boroughs
-- MAGIC     group by 1
-- MAGIC     order by 2 desc
-- MAGIC )
-- MAGIC select count(*)
-- MAGIC from host
-- MAGIC where listing_count >= 3  

-- COMMAND ----------

