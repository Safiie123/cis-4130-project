import boto3
import pandas as pd
df = pd.read_csv('s3://project-data-sb/post_info.txt', header=None, sep='\t')
df2 = df.groupby([1])[1].count()
print(df2)
print(df2.describe())

