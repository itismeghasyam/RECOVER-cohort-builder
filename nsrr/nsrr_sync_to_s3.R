#############################
# Sync NSRR example to their own bucket
#############################
### Params
# LOCAL_COHORT_LOCATION <- 's3://recover-velsera-integration/main/archive/2024-02-29/'
LOCAL_COHORT_LOCATION <- 'nsrr/main/'
# AWS_DESTINATION_LOCATION <- paste0('s3://recover-velsera-integration/main/archive/',ARCHIVE_VERSION,'/')
# AWS_DESTINATION_LOCATION <- 's3://recover-velsera-integration/main/parquet/'
AWS_DESTINATION_LOCATION <- 's3://nih-nhlbi-bdc-recover-dhdr-quar/main/'



####
# Required libraries and functions
####
library(tidyverse)

s3SyncFromLocal <- function(local_source = './nsrr/', destination_bucket, aws_profile='nsrr_upload'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 sync {local_source} {destination_bucket} --delete')
  if(aws_profile == 'env-var'){
    command_in <- glue::glue('aws s3 sync {local_source} {destination_bucket} --acl bucket-owner-full-control')
  } # need to add acl permisisons as bucket-owner-full-control to upload/put objects in the bucket
  
  print(paste0('RUNNING: ',command_in))
  system(command_in)
}

#############
# Sync curated parquets from local EC2 to S3 bucket (s3://nih-nhlbi-bdc-recover-dhdr-quar/)
#############
s3SyncFromLocal(local_source = LOCAL_COHORT_LOCATION ,
                destination_bucket = AWS_DESTINATION_LOCATION,
                aws_profile = 'nsrr_upload')
