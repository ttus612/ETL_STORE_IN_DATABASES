import os 
import datetime
import findspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main_task(path, file_name):

    print("-----------------------------------")
    print("Reading data from file")
    print("-----------------------------------")
	
    print("-----------------------------------")
    print("Path: " + path)
    print("File name:"+ file_name)
    print("-----------------------------------")

    print("-----------------------------------")
    ds = spark.read.json(path)
    print("Read data success")
    print("-----------------------------------")
    ds.select('_source.*').show()

    ds = ds.withColumn('Date', lit(file_name))

    ds = ds.withColumn('Category',
                when(
                     (col('_source.AppName') == 'KPLUS') | 
                     (col('_source.AppName') == 'RELAX'),
                     "Giải trí"
                    )
                .when(
                     (col('_source.AppName') == 'CHILD'),
                     "Trẻ em"
                    )
                .when(
                     (col('_source.AppName') == 'CHANNEL') |
                     (col('_source.AppName') == 'VOD'),
                     "Truyền hình"
                    )
                .when(
                     (col('_source.AppName') == 'FIMS'), 
                     "Phim ảnh"
                )
                .when(
                     (col('_source.AppName') == 'SPORT'),
                     "Thể thao"
                )
                .otherwise("Khác")
            )
    ds.show()
    return ds
	
def mostWatchedCategory(ds):
    ds = ds.withColumn('Most_Watch', greatest('TVDuration', 'MovieDuration', 'ChildDuration', 'SportDuration', 'RelaxDuration'))
    ds = ds.withColumn('Most_Watch',              
                 when(
                    (col('Most_Watch') == col('TVDuration')),
                     "TVDuration"
                    )
                .when(
                     (col('Most_Watch') == col('MovieDuration')),
                     "MovieDuration"
                    )
                .when(
                     (col('Most_Watch') == col('ChildDuration')),
                     "ChildDuration"
                    )
                .when(
                     (col('Most_Watch') == col('SportDuration')),
                     "SportDuration"
                    )
                .when(
                     (col('Most_Watch') == col('RelaxDuration')),
                     "RelaxDuration"
                    )
                .otherwise("Khác")
                )
    return ds

def type_contract(ds):
    low_engagement_threshold = 10000
    high_engagement_threshold = 50000
   
    ds = ds.withColumn('Type_Contract'
                       ,when(
                            (col('TVDuration') + 
                             col('MovieDuration') + 
                             col('ChildDuration') + 
                             col('SportDuration') + 
                             col('RelaxDuration')) < low_engagement_threshold,
                             "Low_Engagement")
                        .when((
                             col('TVDuration') + 
                             col('MovieDuration') + 
                             col('ChildDuration') + 
                             col('SportDuration') + 
                             col('RelaxDuration')) > high_engagement_threshold,
                             "High_Engagement")
                        .otherwise("Normal_Engagement"))
    return ds

def customer_taste(ds):
    ds = ds.withColumn('Customer_Taste',
        concat_ws("-",
            when(
                (col('TVDuration') != 0), "TVDuration"),
            when(
                (col('MovieDuration') != 0), "MovieDuration"),
            when(
                (col('ChildDuration') != 0),"ChildDuration"),
            when(
                (col('SportDuration') != 0),"SportDuration"),
            when(
                (col('RelaxDuration') != 0),"RelaxDuration")
            )
        )
    return ds

def active_date(ds, pivot_data, sum_date):
    ds = ds.groupBy('Contract', 'Date').pivot('Category')\
        .agg(sum('TotalDuration'))\
        .select(
            'Contract',
            'Date',
            'Giải trí', 'Phim ảnh', 'Trẻ em', 'Thể thao', 'Truyền hình')\
        .withColumnRenamed('Giải trí', 'TVDuration')\
        .withColumnRenamed('Phim ảnh', 'MovieDuration')\
        .withColumnRenamed('Trẻ em', 'ChildDuration')\
        .withColumnRenamed('Thể thao', 'SportDuration')\
        .withColumnRenamed('Truyền hình', 'RelaxDuration')\
        .na.fill(0)
    
    ds_count_day = ds.groupBy('Contract').agg(
        ((count('Date')/ sum_date)*100).alias('ActiveNess')
    ).select('Contract', 'ActiveNess')
      
    final_df = pivot_data.join(ds_count_day, on='Contract', how='inner')

    return final_df

def group_Contract(ds):
    ds = ds.withColumn('Group',
        when(
            (col('ActiveNess') < 10), "Group1:Nhóm người dùng ít sử dụng")
        .when(
            (col('ActiveNess') >= 10) & (col('ActiveNess') < 30), "Group2:Nhóm người dùng trung bình")
        .when(
            (col('ActiveNess') >= 30) & (col('ActiveNess') < 60), "Group3:Nhóm người dùng nhiều sử dụng")
        .when(  
            (col('ActiveNess') >= 60), "Group4:Nhóm người dùng rất nhiều sử dụng")
        .otherwise("Khác")
    )
  
    return ds

def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value
	
def date_range(start_date, end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list
	
def genarate_date_range(from_date, to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)	
	date_list = date_range(from_date, to_date)
	return date_list

def sum_date(start_date, end_date):
    start_date = convert_to_datevalue(start_date)
    end_date = convert_to_datevalue(end_date)
    if(start_date == 1 and end_date == 1):
        return sum_date.day + 1
    
    sum_date = (end_date - start_date)
    return sum_date.days + 1
	
if __name__ == "__main__":
    findspark.init()

    spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()

    path = 'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content'

    # Dùng để lấy tất cả tên của fil trong thư mục
    # list_files = os.listdir(path)

    # Chọn ngày mình muốn xử lý
    start_date = '20220401'
    end_date = '20220401'
    list_files = genarate_date_range(start_date, end_date)
    
    sum_date = sum_date(start_date, end_date)

    print("-----------------------------------")
    print("Start processing data")
    print("-----------------------------------")

    final_result = None
    for file_name in list_files:
        print(f"==> Processing file {file_name} <==")
        path_new = path + '\\' + file_name + '.json'
        processed_data = main_task(path_new, file_name)
        if final_result is None:
            final_result = processed_data
        else:
            final_result = final_result.unionAll(processed_data)

    print("-----------------------------------")
    print("Show data & structure after read all files")
    print("-----------------------------------")
    final_result.show()

    print("-----------------------------------")
    print("Group by data")
    print("-----------------------------------")
    grouped_data = final_result.groupBy('_source.Contract', 'Category', 'Date').agg(
        sum('_source.TotalDuration').alias('TotalDuration')
    )

    grouped_data.show()

    print("-----------------------------------")
    print("Pivot data ")
    print("-----------------------------------")
    pivot_data = grouped_data.groupBy('Contract').pivot('Category')\
        .agg(sum('TotalDuration'))\
        .select(
            'Contract',
            'Giải trí', 'Phim ảnh', 'Trẻ em', 'Thể thao', 'Truyền hình')\
        .withColumnRenamed('Giải trí', 'TVDuration')\
        .withColumnRenamed('Phim ảnh', 'MovieDuration')\
        .withColumnRenamed('Trẻ em', 'ChildDuration')\
        .withColumnRenamed('Thể thao', 'SportDuration')\
        .withColumnRenamed('Truyền hình', 'RelaxDuration')\
        .na.fill(0)

    pivot_data.show()

    print("-----------------------------------")
    print("Processing data active date")
    print("-----------------------------------")
    ds = active_date(grouped_data, pivot_data, sum_date)

    print("-----------------------------------")
    print("Processing data most watched")
    print("-----------------------------------")
    ds = mostWatchedCategory(ds)

    print("-----------------------------------")
    print("Processing data customer taste")
    print("-----------------------------------")
    ds = customer_taste(ds)

    print("-----------------------------------")
    print("Processing data type contract")
    print("-----------------------------------")
    ds = type_contract(ds)
    
    print("-----------------------------------")
    print("Processing data group")
    print("-----------------------------------")
    ds_final = ds.select('Contract', 'ActiveNess', 'Most_Watch', 'Type_Contract', 'Customer_Taste') 

    ds_final = group_Contract(ds_final)

    print("-----------------------------------")
    print("Show table after ETL")
    print("-----------------------------------")
    ds_final.show()

    print("-----------------------------------")
    print("Save data to database")
    print("-----------------------------------")
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + '...'
    driver = "com.mysql.cj.jdbc.Driver"
    user = '...'
    password = '...'
    ds_final.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','CONTRACTS').option('user',user).option('password',password).mode('append').save()
   
    print("-----------------------------------")
    print("Read data from database")
    print("-----------------------------------")
    sql = '(select * from contracts) A'
    df = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()
    df.show()
    # save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class5\\Storage\\Methoad2"
    # ds.repartition(1).write.csv(save_path, header=True)
    # pivot_data.coalesce(1).write.option("header","true").format("csv").save(save_path)

    print("-----------------------------------")
    print("End processing data")
    print("-----------------------------------")
		