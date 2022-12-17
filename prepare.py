from pyspark.sql.functions import input_file_name, split, col, concat, lit, when

# my attempts at trying to increase memory usage for driver and executors
# new_conf = spark.sparkContext.getConf().setAll([('spark.executor.memory', '20g'),('spark.driver.memory','30g'), ('spark.executor.memory','30g')])
# spark = SparkSession.builder.config(conf=new_conf).getOrCreate()

# reading txt file to help with joining between post files and profile files
df1 = spark.read.text('s3a://project-data-sb/post_info.txt').select(split('value', '\t')[1].alias('account_name'), split('value', '\t')[3].alias('json_file'))
data = df1.select(concat(lit('s3a://project-data-sb/post_files/'),col('json_file')).alias("full_path")).collect()

# reading first 10k json files, transforming and writing back to s3
print("reading at range 0")
file_list = data[0:10000]
file_list_mapped = list(map(lambda x: x.full_path, file_list))
df = spark.read.json(file_list_mapped, multiLine=True).withColumn("input_file_name",input_file_name()).select(col("edge_media_preview_like.count").alias("like_count"), col("edge_media_to_comment.count").alias("comment_count"), split("input_file_name", '/')[4].alias('json_file'))
df.write.parquet("s3a://project-data-sb/parquet/data.parquet")

# reading 10k files at a time, i stopped at 100k however because it took about 20 minutes to read/transform 10k files
for index in range(10000,1601074,10000):
	print("reading at range "+str(index))
	file_list = data[index:index+10000]
	file_list_mapped = list(map(lambda x: x.full_path, file_list))
	print("created mapped list")
	df = spark.read.json(file_list_mapped, multiLine=True).withColumn("input_file_name",input_file_name()).select(col("edge_media_preview_like.count").alias("like_count"),col("edge_media_to_comment.count").alias("comment_count"),split("input_file_name",'/')[4].alias('json_file'))
	df.write.mode('append').parquet("s3a://project-data-sb/parquet/data.parquet")

# reading entire post dataset
df = spark.read.parquet("s3a://project-data-sb/parquet/data.parquet")

# reading/transforming entire profile dataset
df3 = spark.read.text('s3://project-data-sb/profile_files/').withColumn("input_file_name",input_file_name()).select(split('value', '\t')[1].alias('follower_num'), split('value', '\t')[2].alias('following_num'), split('value', '\t')[3].alias('post_num'), split('value', '\t')[6].alias('category_num'), split("input_file_name", '/')[4].alias('account_name'))

# performing a couple joins to get all data in one dataframe
first_join = df1.join(df3, df1.account_name == df3.account_name, 'inner')
second_join = df.join(first_join, first_join.json_file == df.json_file, 'inner')

# dropping unnecessary columns
final_df = final_df.drop(df1.account_name, df3.account_name, first_join.json_file, df.json_file)

# transforming category column to be integer instead of string
final_df = second_join.withColumn("category_num_2",
                     when(col("category_num")=='Creators & Celebrities', 1)
                     .when(col("category_num")=='Non-Profits & Religious Organizations', 2)
                     .when(col("category_num")=='Publishers', 3)
                     .when(col("category_num")=='Personal Goods & General Merchandise Stores', 4)
                     .when(col("category_num")=='Business & Utility Services', 5)
                     .when(col("category_num")=='General Interest', 6)
                     .when(col("category_num")=='Transportation & Accomodation Services', 7)
                     .when(col("category_num")=='Content & Apps', 8)
                     .when(col("category_num")=='Lifestyle Services', 9)
                     .when(col("category_num")=='Grocery & Convenience Stores', 10)
                     .when(col("category_num")=='Home Services', 11)
                     .when(col("category_num")=='Food & Personal Goods', 12)
                     .when(col("category_num")=='Local Events', 13)
                     .when(col("category_num")=='Professional Services', 14)
                     .when(col("category_num")=='Auto Dealers', 15)
                     .when(col("category_num")=='Home Goods Stores', 16)
                     .when(col("category_num")=='Entities', 17)
                     .when(col("category_num")=='Restaurants', 18)
                     .when(col("category_num")=='Government Agencies', 19)
                     .when(col("category_num")=='Geography', 20)
                     .when(col("category_num")=='Home & Auto', 21)
                     ).drop("category_num")

# writing final dataframe to s3
final_df.write.parquet("s3a://project-data-sb/parquet/final_data.parquet")