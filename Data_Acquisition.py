curl - SLO https://github.com/prasmussen/gdrive/releases/download/2.1.1
gzip -d gdrive_2.1.1_linux_386.tar.gz and $ tar -xf gdrive_2.1.1_linux_386.tar
 ./gdrive download 1zzKv8Tt3IAkaZ6UfLaUlwM7qk5LMx0Q 
 ./gdrive download 10m2vzWMZcSDmzyTbFYi4zadk8Yq16T0d
 ./gdrive download 1RnhDr_GY7bwiQEI9XEW9ZOsHOwp-1Kq0
 ./gdrive download 1OQbkhgcOAKv1brPE50QhTw6TpwWsgMAo 

 ./gdrive download –stdout 1zzKv8Tt3IAkaZ6UfLaUlwM7qk5LMx0Q- | aws s3 cp - s3://project-data-sb/post_info.txt 
 ./gdrive download --stdout 10m2vzWMZcSDmzyTbFYi4zadk8Yq16T0d | aws s3 cp - s3://project-data-sb/json_files.zip 
 ./gdrive download --stdout 1RnhDr_GY7bwiQEI9XEW9ZOsHOwp-1Kq0 | aws s3 cp - s3://project-data-sb/profiles_influencers.zip 
 ./gdrive download --stdout 1OQbkhgcOAKv1brPE50QhTw6TpwWsgMAo | aws s3 cp - s3://project-data-sb/profiles_brands.zip 