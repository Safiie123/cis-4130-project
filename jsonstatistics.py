import zipfile
import boto3
import pandas as pd
import json
from io import BytesIO
bucket="project-data-sb"
zipfile_to_unzip="json_files.zip"
s3_client = boto3.client('s3', use_ssl=False)
s3_resource = boto3.resource('s3')

zip_obj = s3_resource.Object(bucket_name=bucket, key=zipfile_to_unzip)
buffer = BytesIO(zip_obj.get()["Body"].read())
z = zipfile.ZipFile(buffer)

df = pd.DataFrame(columns=['height','width','likes','comments', 'caption_word_count'])

counter = 0
for filename in z.namelist()[1:]: 
	# Added this break here because otherwise it takes too long to run. I will try to run for entire dataset over the weekend
	if counter == 10000:
	    break
	data = json.load(open(filename))
	caption_list = data['edge_media_to_caption']['edges']
	caption_word_count = 0
	if caption_list:
		caption_word_count = len(caption_list[0]['node']['text'].split())
	df = df.append({'height': data['dimensions']['height'], 'width': data['dimensions']['width'], 'likes': data['edge_media_preview_like']['count'],'comments': data['edge_media_to_comment']['count'], 'caption_word_count': caption_word_count}, ignore_index=True)
	counter = counter +1

print(df.astype(float).describe())

