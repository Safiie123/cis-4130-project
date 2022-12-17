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

	# add like counts normalized by number of followers
	final_df = final_df.withColumn('like_count_normalized_by_followers', final_df.like_count * 100 / final_df.follower_num)

	# convert to pandas dataframe
	pd = final_df.toPandas()

	#plot and save the 4 scatter plot pngs

	myplot = pd.plot.scatter('like_count', 'comment_count', xlim=[0,10000], ylim=[0, 600], s=1)
	myplot.get_figure().savefig("likes_comments.png")

	myplot = pd.plot.scatter('following_num', 'follower_num', xlim=[0,7500], ylim=[1000, 1000000], s=1)
	myplot.get_figure().savefig("following_follower.png")

	myplot = pd.plot.scatter('category_num_2', 'post_num', xlim=[0,20], ylim=[0, 10000], s=1)
	myplot.get_figure().savefig("category_post.png")

	myplot = pd.plot.scatter('category_num_2', 'like_count_normalized_by_followers', xlim=[0,20], ylim=[0, 20], s=1)
	myplot.get_figure().savefig("category_likes.png")



	