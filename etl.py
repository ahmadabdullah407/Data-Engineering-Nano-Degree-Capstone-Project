import re
import configparser
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import MapType
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('capstone.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

# data processing functions
def create_spark_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages",\
                "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def visa_int_to_var(visa):
    """assigns values to integers in visa volumn in i94 immigration dataset from i94_SAS_Labels_Description.
    
    Arguments: 
                visa : Integer
    Returns:   
                String
    """
    if visa == 1:
        return 'Business'
    elif visa == 2:
        return 'Pleasure'
    elif visa == 3:
        return 'Student'

    
def parseco(line):
    """Uses Regex to parse country code and name string in i94_SAS_Labels_Description.
    
    Arguments:
            line : String
    Returns:
            Dictionary
    """
    pattern = "^\s*(\d+)\s*=\s*'(.*)'"
    match = re.search(pattern,line)

    return {
        'code' :int(match.group(1)),
        'country':match.group(2)
        }

def parsecity(line):
    """Uses Regex to parse city code and name string in i94_SAS_Labels_Description.
    
    Arguments:
            line : String
    Returns:
            Dictionary
    """
    pattern = "^\s*'(\w{3})'\s*=\s*'([\w\s\./-]*),{0,1}"
    match = re.search(pattern,line)

    return {
        'city_code' :match.group(1),
        'city_name':match.group(2)
        }


def parsestate(line):
    """Uses Regex to parse state code and name string in i94_SAS_Labels_Description.
    
    Arguments:
            line : String
    Returns:
            Dictionary
    """
    pattern = "^\s*'(\w{2})'\s*=\s*'(.*)'"
    match = re.search(pattern,line)

    return {
        'state_code' :match.group(1),
        'state_name':match.group(2)
        }

def parseregion(line):
    """Uses Regex to parse country and state from region string in airport-codes data.
    
    Arguments:
            line : String
    Returns:
            String
    """
    pattern = "\w{2}-(\w{2})"
    match = re.search(pattern,line)

    return match.group(1)


def load_and_clean_data(spark, input_data):
    """Loads and cleans immigration data,its description files, cities demography data and airport codes data.
    dimm_immi_personal and dimm_flight_detail tables.
        
        Arguments:
            spark (object): SparkSession object
            input_data (object): Source S3 endpoint
        Returns:
            None
    """
    # read immigration data file
    logging.info("Loading immigration Data")
    immi_data = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/*.sas7bdat')
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(immi_data)
    
    
    spark.udf.register("SAS_to_datetime",lambda date: pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1'),TimestampType())
    spark.udf.register("visa_transform",lambda visa:visa_int_to_var(visa))
    df_spark.createOrReplaceTempView("df_spark")
    
    # cleaning immigration data
    logging.info("Cleaning immigration Data")
    cleaned_df_immi = spark.sql("""SELECT monotonically_increasing_id() as id,
                            cast(cicid as BIGINT),
                            cast(i94yr as INT) as year,
                            cast(i94mon as INT) as month,
                            cast(i94cit as INT) as citizen_country,
                            cast(i94res as INT) as residence_country,
                            cast(i94port as CHAR(3)) as port,
                            SAS_to_datetime(arrdate) as arrival_date,
                            SAS_to_datetime(depdate) as departure_date,
                            cast(i94addr as CHAR(2)) as state_code,
                            cast(i94bir as INT) as birth_year,
                            visa_transform(i94visa) as visa,
                            cast(gender as CHAR(1)) as gender,
                            cast(admnum as BIGINT) as adm_num,
                            fltno as flight_no,
                            airline,
                            visatype
                            FROM df_spark
                            WHERE i94mode = 1 AND depdate IS NOT NULL AND i94addr IS NOT NULL
                            AND fltno IS NOT NULL AND airline IS NOT NULL AND visatype IS NOT NULL
                            AND gender IS NOT NULL AND i94cit IS NOT NULL AND i94res IS NOT NULL
                            """)
    
    cleaned_df_immi.createOrReplaceTempView("df_immi")
        
    # loading text from label_description
    label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    df=spark.read.text(label_file)
    
    
    # Extracting country codes and names from label description
    spark.udf.register('parseco',parseco,MapType(Str(),Str()))
    df_countries=spark.createDataFrame(df.collect()[10:298])
    df_countries.createOrReplaceTempView("df_co")
    df_countries=spark.sql("""SELECT parseco(value) as parsed
                            FROM df_co""")
    df_countries.createOrReplaceTempView("df_co")
    df_countries = spark.sql("""SELECT parsed['code'] as country_code,
                                    parsed['country'] as country_name
                                FROM df_co""")
    df_countries.createOrReplaceTempView("df_countries")
    
    
    # Extracting city codes and names from label description
    spark.udf.register('parsecity',parsecity,MapType(Str(),Str()))
    df_city=spark.createDataFrame(df.collect()[303:893])
    df_city.createOrReplaceTempView("df_city")
    df_city=spark.sql("""SELECT parsecity(value) as parsed
                FROM df_city""")
    df_city.createOrReplaceTempView("df_city")
    df_city=spark.sql("""SELECT parsed['city_code'] as city_code,parsed['city_name'] as city_name
                        FROM df_city""")
    df_city.createOrReplaceTempView("df_city")
    
    
    #Extracting state codes and names from lobels description
    spark.udf.register('parsestate',parsestate,MapType(Str(),Str()))
    df_state=spark.createDataFrame(df.collect()[982:1036])
    df_state.createOrReplaceTempView("df_state")
    df_state=spark.sql("""SELECT parsestate(value) as parsed
                            FROM df_state""")
    df_state.createOrReplaceTempView("df_state")
    df_state = spark.sql("""SELECT parsed['state_code'] as state_code,
                            parsed['state_name'] as state_name
                            FROM df_state""")
    df_state.createOrReplaceTempView("df_state")
    
    # Load US cities demography data
    demog_data = os.path.join(input_data + 'us-cities-demographics.csv')
    df = spark.read.csv(demog_data,sep=";", inferSchema=True, header=True)
    df.createOrReplaceTempView("df")
    
    # Clean US cities demography data
    df_us_cities=spark.sql("""
                    SELECT upper(a.city) as city,
                    upper(a.state) as state,
                    a.`Median Age` as median_age,
                    a.`Male Population` as male_population,
                    a.`Female Population` as female_population,
                    a.`Total Population` as total_population,
                    a.`Number of Veterans` as veteran_population,
                    a.`Foreign-born` as foreign_population,
                    a.`Average Household Size` as avg_household_size,
                    a.`State Code` as state_code,
                    a.Race as race
                    FROM df a INNER JOIN (SELECT city,MAX(count) as count
                                            FROM df
                                            GROUP BY city) b
                    ON a.city = b.city AND a.count = b.count
                    """)
    df_us_cities.createOrReplaceTempView("df_us_cities")
    
    
    # Loading airport_codes data 
    airport_data = os.path.join(input_data + 'airport-codes_csv.csv')
    df = spark.read.csv(airport_data,sep=",", inferSchema=True, header=True)
    
    # Cleaning Airport Codes data
    spark.udf.register('parseregion',parseregion,Str())
    df.createOrReplaceTempView("df")
    df_airports = spark.sql("""SELECT ident as airport_id,
                                        type,
                                        name,
                                        elevation_ft,
                                        parseregion(iso_region) as region,
                                        municipality,
                                        coordinates
                                        FROM df
                                        WHERE iso_country = 'US'
                            """)
    df_airports.createOrReplaceTempView("df_airports")
    
    
def process_and_write_data(spark, output_data):
    """Models data according to our decided star schema and writes data to output specified s3 path.
    Arguments:
            spark (object): SparkSession object
            output_data (object): target S3 endpoint
    Returns:
            None
    """
    # Generating and writing fact_immigration table
    fact_immigration=spark.sql("""SELECT id,
                                cicid,
                                year,
                                month,
                                port,
                                state_code,
                                visa,
                                arrival_date,
                                departure_date,
                                adm_num
                                FROM df_immi""")

    fact_immigration.write.mode("overwrite")\
                    .parquet(path=output_data + 'fact_immigration')
    
    # Generating and writing dimm_immi_personal table
    dimm_immi_personal = spark.sql("""
                                SELECT DISTINCT i.cicid,
                                        cc.country_name as citizen_country,
                                        rc.country_name as residence_country,
                                        i.birth_year,
                                        i.gender
                                        FROM df_immi i
                                        LEFT JOIN df_countries cc ON i.citizen_country=cc.country_code
                                        LEFT JOIN df_countries rc ON i.residence_country=rc.country_code
                                        ORDER BY cicid
                                        """)
    dimm_immi_personal.write.mode("overwrite")\
                    .parquet(path=output_data + 'dimm_immi_personal')
    
    # Generating and writing flight detail table
    dimm_flight_detail = spark.sql("""
                                SELECT DISTINCT adm_num,
                                        flight_no,
                                        airline,
                                        visatype
                                FROM df_immi
                                """)
    
    dimm_flight_detail.write.mode("overwrite")\
                    .parquet(path=output_data + 'dimm_flight_detail')
    
    # Generating and writing dimm_airports table
    dimm_airports = spark.sql("""SELECT * FROM df_airports""")
    dimm_airports.write.mode("overwrite")\
                    .parquet(path=output_data + 'dimm_airports')
    
    # Generating and writing dimm_city_population.
    dimm_city_population = spark.sql("""
                                    SELECT uc.city,
                                            uc.state,
                                            c.city_code,
                                            uc.state_code,
                                            uc.male_population,
                                            uc.female_population,
                                            uc.total_population,
                                            uc.veteran_population,
                                            uc.foreign_population,
                                            uc.race
                                            FROM df_us_cities uc
                                            LEFT JOIN df_city c ON uc.city=c.city_name
                                            WHERE city_code IS NOT NULL
                                            """)
    dimm_city_population.write.mode("overwrite")\
                    .parquet(path=output_data + 'dimm_city_population')
    
    # generating and writing dimm_city_stats table
    dimm_city_stats = spark.sql("""SELECT uc.city,
                                uc.state,
                                c.city_code,
                                uc.state_code,
                                uc.median_age,
                                uc.avg_household_size
                                FROM  df_us_cities uc
                                LEFT JOIN df_city c ON uc.city=c.city_name
                                WHERE city_code IS NOT NULL""")
    dimm_city_stats.write.mode("overwrite")\
                    .parquet(path=output_data + 'dimm_city_stats')
    
def main():
    spark = create_spark_session()
    input_data = SOURCE_S3_BUCKET
    output_data = DEST_S3_BUCKET
    
    load_and_clean_data(spark, input_data)
    process_and_write_data(spark, output_data)
    
    logging.info("Data processing completed")
    spark.stop()
    
if __name__ == "__main__":
    main()