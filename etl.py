import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count,avg
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import split
import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''
    Creates a spark session and returns it
    '''
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").\
        enableHiveSupport().getOrCreate()
    
    return spark


def split_iso_region(iso_region):
    return iso_region.split("-")[1]


def sas_date_to_datetime(sas_date):
    '''
    Converts given SAS numeric date to datetime
    '''
    if sas_date is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=sas_date)


def generate_time_df(immigration_data):
    '''
    Returns a df with (timestamp,year,month,day,week,weekday) columns
    From given immigrations df with (SAS date column(s))
    '''
    # get date columns
    time_df = immigration_data.select(['arrival_date', 'departure_date'])

    # Start creating unified date time df
    # there will be a lot of duplicates, so drop them
    arrival_df = time_df.select('arrival_date').dropDuplicates()
    departure_df = time_df.select('departure_date').dropDuplicates()
    unified_df = arrival_df.union(departure_df).dropDuplicates()
    
    # Create UDF for sas date conversion
    reg_convert_sas_date = F.udf(lambda date: sas_date_to_datetime(date))

    # Apply it
    unified_df = unified_df.withColumn('arrivalDateAsDATE', reg_convert_sas_date(unified_df.arrival_date))

    # Add other columns
    unified_df = unified_df.withColumn('year', F.year('arrivalDateAsDATE'))
    unified_df = unified_df.withColumn('month', F.month('arrivalDateAsDATE'))
    unified_df = unified_df.withColumn('day', F.dayofmonth('arrivalDateAsDATE'))
    unified_df = unified_df.withColumn('week', F.weekofyear('arrivalDateAsDATE'))
    unified_df = unified_df.withColumn('weekday', F.dayofweek('arrivalDateAsDATE'))

    # Drop date string column since we no longer need it
    unified_df = unified_df.drop('arrivalDateAsDATE')

    # Rename
    unified_df = unified_df.withColumnRenamed('arrival_date','timestamp')
    
    return unified_df


def split_iso_region(iso_region):
    '''
    Returns the state part of iso_region column
    For example: Input 'US-CA' yields 'CA'
    '''
    return iso_region.split("-")[1]
    
    
def process_immigrations_ports_cities_data(spark, output_data):
    time_output_data = output_data + 'dim_time'
    cities_output_data = output_data + 'dim_cities'
    ports_output_data = output_data + 'dim_ports'
    immigrations_output_data = output_data + 'fact_immigrations'

    # Read cities
    city_df = spark.read.options(header="true",inferSchema="true",nullValue = "NULL",delimiter=";").csv('us-cities-demographics.csv')
    # drop race and count columns
    city_df = city_df.drop('Race','Count')
    # Get non-null state code and city
    city_df = city_df.filter(city_df['State Code'].isNotNull() & city_df['City'].isNotNull())

    # Read Ports
    ports_df = spark.read.options(header="true",inferSchema="true",nullValue = "NULL").csv('airport-codes_csv.csv')
    # Lets take iso_country US, non-null iata code and non-closed records
    ports_df = ports_df.filter((ports_df.iata_code.isNotNull()) \
                                & (ports_df.iso_country == 'US') \
                                & (ports_df.type != 'closed') )
    drop_cols = ('iso_country','gps_code','elevation_ft','local_code','coordinates')
    ports_df = ports_df.drop(*drop_cols)


    # Join on 'City.city == ports_df.municipality
    combined_df = city_df.join(ports_df, city_df.City == ports_df.municipality).dropDuplicates()

    # Register split function
    reg_split_iso_region = F.udf(lambda iso_reg: split_iso_region(iso_reg))

    final_ports_df = combined_df.select(['ident','name','municipality','type','iata_code','iso_region']) \
                            .withColumnRenamed('ident','port_id') \
                            .withColumnRenamed('municipality','city') 
    
    # Apply split function to iso_region
    final_ports_df = final_ports_df.withColumn('iso_region',reg_split_iso_region('iso_region').alias('region'))
    
    # Cache it for optimization
    final_ports_df = final_ports_df.cache()

    final_cities_df = combined_df.select(['City','State','Median Age','Male Population','Female Population','Total Population' \
                                      ,'Number of Veterans','Foreign-born','Average Household Size']) \
                            .withColumnRenamed('City','city') \
                            .withColumnRenamed('State','state') \
                            .withColumnRenamed('Median Age','median_age') \
                            .withColumnRenamed('Male Population','male_pop') \
                            .withColumnRenamed('Female Population','female_pop') \
                            .withColumnRenamed('Total Population','total_pop') \
                            .withColumnRenamed('Number of Veterans','veterans') \
                            .withColumnRenamed('Foreign-born','foreign_born') \
                            .withColumnRenamed('Average Household Size','avg_household_size')
    # Cache it for optimization
    final_cities_df = final_ports_df.cache()
    
    # Start working on immigrations data

    # These columns are either meaningless for the scope of the project or almost empty, so we are removing them
    drop_cols = ("i94yr","i94mon","i94res","count","visapost","occup","entdepa","entdepd","entdepu","matflag" \
             ,"biryear","insnum","fltno","dtadfile","dtaddto","airline","admnum")

    # Month name list
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    
    for month in months:
        immigration_data_path = f'../../data/18-83510-I94-Data-2016/i94_{month}16_sub.sas7bdat'
        
        # Read data using month in input file name
        df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)
        # Cache before using it
        df = df.cache()
        
        df = df.dropDuplicates().drop(*drop_cols).na.drop()
        
        # Fix column names and types
        df = df.withColumnRenamed('cicid','immigration_id')
        df = df.withColumn('immigration_id',df['immigration_id'].cast('int'))
        df = df.withColumnRenamed('i94cit','origin')
        df = df.withColumn('origin',df['origin'].cast('int'))
        df = df.withColumnRenamed('i94bir','age')
        df = df.withColumn('age',df['age'].cast('int'))
        df = df.withColumnRenamed('i94mode','arrival_mode')
        df = df.withColumn('arrival_mode',df['arrival_mode'].cast('int'))
        df = df.withColumnRenamed('i94visa','visa')
        df = df.withColumn('visa',df['visa'].cast('int'))
        df = df.withColumnRenamed('i94port','landing_port') \
            .withColumnRenamed('arrdate','arrival_date') \
            .withColumnRenamed('i94mode','arrival_mode') \
            .withColumnRenamed('i94addr','state') \
            .withColumnRenamed('depdate','departure_date')

        # Now generate time df from current immigration data using 'arrival_date' and 'departure_date' columns
        time_df = generate_time_df(df)
        #print(f'Month: {month}, DF row count: {time_df.count()}')
        
        # Join conditions as a list
        conditions = [df.state == final_ports_df.iso_region, df.landing_port == final_ports_df.iata_code]

        # For final fact_immig, join ports on conditions described above
        joined_immig = df.join(final_ports_df, conditions)

        # Final df to write to S3
        fact_immig_df = joined_immig.select('immigration_id','origin','landing_port' \
                                            ,'arrival_date','departure_date','arrival_mode' \
                                            ,'city','age','visa','visatype','gender')
        
        # Write current time and immigrations data to S3
        # Append data to existing folders throughout the loop
        # partition time data by month
        time_df.write.mode('append').partitionBy('day').parquet(time_output_data)
        # partition immigrations data by city and landing_port
        fact_immig_df.write.mode('append').partitionBy('age').parquet(immigrations_output_data)

    # Write cities and ports data to S3
    # partition city data by state
    final_cities_df.write.partitionBy('state').parquet(cities_output_data)
    # partition ports data by iso_region (alias 'state')
    final_ports_df.write.partitionBy('iso_region').parquet(ports_output_data)

    return


def process_temperature_data(spark, output_data):
    output_data = output_data + 'dim_temperatures'
    
    temps_df = spark.read.options(header="true",inferSchema="true",nullValue = "NULL")\
                                            .csv('../../data2/GlobalLandTemperaturesByCity.csv')

    # Filter by united states since others are meaningless
    temps_df = temps_df.filter(temps_df.Country == 'United States')

    # Drop redundant columns
    temps_df = temps_df.drop("dt","Country","Latitude","Longitude")

    final_temperature_df = temps_df.select(col('City'),col('AverageTemperature'),col('AverageTemperatureUncertainty')) \
    .withColumnRenamed('City','city').withColumnRenamed('AverageTemperature','avg_temp') \
    .withColumnRenamed('AverageTemperatureUncertainty','avg_temp_uncertainty')

    # Drop any records with null
    final_temperature_df = final_temperature_df.na.drop()
    
    # average_temp_df
    avg_df = final_temperature_df.select(col('city'),col('avg_temp')).groupBy('city').avg('avg_temp')
    # average_uncertainty_df
    avg_uncer_df = final_temperature_df.select(col('city'),col('avg_temp_uncertainty'))\
                                        .groupBy('city').avg('avg_temp_uncertainty')
    
    # Now join on 'city' and finalize
    final_uni_df = avg_df.join(avg_uncer_df, ['city']) # join column name is same for both frames, so we pass it as a list
    
    # Fix column names and order
    final_uni_df = final_uni_df.select(final_uni_df.city, final_uni_df['avg(avg_temp)'] \
                                       ,final_uni_df['avg(avg_temp_uncertainty)'])
    final_uni_df = final_uni_df.withColumnRenamed('avg(avg_temp)', 'avg_temp') \
                                .withColumnRenamed('avg(avg_temp_uncertainty)','avg_temp_uncertainty')
    
    # Write to S3
    final_uni_df.write.parquet(output_data)
    
    return


def main():
    '''
    Creates spark session and 
    TODO TODO
    Finally, writes the transformed tables to given output S3 bucket in parquet format
    '''
    spark = create_spark_session()
    output_data = "s3a://i94-udacity-capstone-warehouse/"
    
    # This will create dim_temperatures
    process_temperature_data(spark, output_data)
    
    # This will create fact_immig, dim_time, dim_cities and dim_ports
    process_immigrations_ports_cities_data(spark, output_data)
    
    # TODO: Data quality and copy to redshift
    
    
if __name__ == "__main__":
    main()
