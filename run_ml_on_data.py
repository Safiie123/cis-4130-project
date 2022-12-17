from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import confusion_matrix
from pyspark.sql.functions import col

# reading final dataframe from s3
final_df = spark.read.parquet("s3a://project-data-sb/parquet/final_data.parquet")

# casting string colunms to int
final_df = final_df.withColumn("following_num",final_df.following_num.cast('int')).withColumn("follower_num",final_df.follower_num.cast('int')).withColumn("post_num",final_df.post_num.cast('int'))

# dropping rows with null values
final_df = final_df.na.drop()

# features = ['like_count', 'comment_count', 'follower_num', 'following_num', 'post_num']
# va = VectorAssembler(inputCols = features, outputCol='features')

# # transforming dataframe to be used with rfc
# va_df = va.transform(final_df)
# va_df = va_df.select('features', col('category_num_2').alias('label'))

# (train, test) = va_df.randomSplit([0.8, 0.2])

# # training/testing/predicting
# rfc = RandomForestClassifier(featuresCol="features", labelCol="label")
# rfc = rfc.fit(train)
# prediction = rfc.transform(test)
# evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

# # evaluating model
# accuracy = evaluator.evaluate(prediction)
# predictions = prediction.select("prediction").collect()
# true_vals = prediction.select("label").collect()
# matrix = confusion_matrix(true_vals, predictions)


pd = final_df.toPandas()
myplot = pd.plot.scatter('like_count', 'comment_count', xlim=[0,10000], ylim=[0, 600], s=1)
myplot = pd.plot.scatter('following_num', 'follower_num', xlim=[0,7500], ylim=[1000, 1000000], s=1)
myplot = pd.plot.scatter('category_num_2', 'like_count_normalized_by_followers', xlim=[0,20], ylim=[0, 20], s=1)

myplot.get_figure().savefig("likes_category.png")

import boto3

client = boto3.client('s3', region_name='us-west-2')

client.upload_file('images/image_0.jpg', 'mybucket', 'image_0.jpg')