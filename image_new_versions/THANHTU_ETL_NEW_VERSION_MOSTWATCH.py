import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.context import SparkContext
import pandas as pd
from pyspark.sql.window import Window
import datetime


spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()

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
        
def read_data(path):
    print("------------ => 2.1. Read data--------------")
    df = spark.read.json(path)
    return df

def merge_data_from_list(path, listDays):
    data_result = None
    for filename in listDays:
        path_new = path + '\\' + filename + '.json'
        df = read_data(path_new)
        print("Data count:", df.count())
        df = df.withColumn('Date', lit(filename))
 
        print("------------ => 2.2. Merge data into common data--------------")
        if data_result is None:
            data_result = df
        else:
            data_result = data_result.unionAll(df)

    return data_result

def process_category(df):
    data = df.withColumn('Category',
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
    
    return data

def process_groupby_data(df):
    data = df.groupBy('_source.Contract', 'Category', 'Date').agg(
        sum('_source.TotalDuration').alias('TotalDuration')
    )   

    return data

def process_pivot_data(df):
    data =  df.groupBy('Contract').pivot('Category')\
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
    
    return data

def write_file_csv(df):
    save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class3_Class4\\Storage\\DataFrom30Day_3"
    df.repartition(1).write.csv(save_path, header=True)
    print("Data have been written file csv")

def save_data_into_db(df):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_db'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = 'sapassword'
    df.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','etl_log_content_most_watch').option('user',user).option('password',password).mode('append').save()
    print("Data has been saved into DB")

def sum_date(start_date, end_date):
    start_date = convert_to_datevalue(start_date)
    end_date = convert_to_datevalue(end_date)
    if(start_date == 1 and end_date == 1):
        return sum_date.day + 1
    
    sum_date = (end_date - start_date)
    return sum_date.days + 1

def active_date(ds, pivot_data, sum_date):
    data = ds.groupBy('Contract', 'Date').pivot('Category')\
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
    
    data = data.groupBy('Contract').agg(
        ((count('Date')/ sum_date)*100).alias('ActiveNess')
    ).select('Contract', 'ActiveNess')
      
    data = pivot_data.join(data, on='Contract', how='inner')

    return data

def mostWatchedCategory(df):
    data = df.withColumn('Most_Watch', greatest('TVDuration', 'MovieDuration', 'ChildDuration', 'SportDuration', 'RelaxDuration'))
    data = data.withColumn('Most_Watch',              
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
    return data

def customer_taste(df):
    data = df.withColumn('Customer_Taste',
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
    return data


def type_contract(df):
    low_engagement_threshold = 10000
    high_engagement_threshold = 50000
   
    data = df.withColumn('Type_Contract'
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
    return data


def group_Contract(df):
    data = df.withColumn('Group',
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
    return data

def filter_column(df):
    data = df.select('ActiveNess', 'Most_Watch', 'Customer_Taste', 'Type_Contract', 'Group')
    return data


def main_task(path, stat_date, end_date):
    print("------------1. Get list days--------------")
    listDays = genarate_date_range(stat_date, end_date)
    print("List days:", listDays)
    print("------------2. Read and merge data form list--------------")
    data = merge_data_from_list(path, listDays)
    print("Data has been merged:",data.count())
    print("------------3. Process category--------------")
    data = process_category(data)
    print("------------4. Processing groupby data--------------")
    data = process_groupby_data(data)
    date_temp = data
    print("Group data count",data.count())
    print("------------5. Processing pivot data--------------")
    data = process_pivot_data(data)
    print("------------6. Processing active date--------------")
    data = active_date(date_temp, data, sum_date)
    print("------------7. Processing most watched--------------")
    data = mostWatchedCategory(data)
    print("------------8. Processing customer taste--------------")
    data = customer_taste(data)
    print("------------9. Processing type contract--------------")
    data = type_contract(data)
    print("------------10. Processing data group--------------")
    data = group_Contract(data)
    data.show()
    print("------------10. Filter Column--------------")
    data = filter_column(data)
    print("------------11. Save data--------------")
    write_file_csv(data)
    save_data_into_db(data)
    print("------------Finish--------------")
    
    
   
path = 'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content'
start_date = input("Enter your start_date(YYYYmmdd): ")
end_date = input("Enter your end_date(YYYYmmdd): ")
print("Start_date:", start_date)
print("End_date:", end_date)
sum_date = sum_date(start_date, end_date)
main_task(path, start_date, end_date)