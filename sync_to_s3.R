#############################
# Cohort builder: Sync curated parquets to S3
#############################
### Params
LOCAL_COHORT_LOCATION <- config::get('LOCAL_COHORT_LOCATION')
AWS_DESTINATION_LOCATION <- config::get('AWS_DESTINATION_LOCATION')

####
# Required libraries and functions
####
library(tidyverse)
library(synapser)
synapser::synLogin()

s3SyncFromLocal <- function(local_source = './cohort_builder', destination_bucket, aws_profile='service-catalog'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 sync {local_source} {destination_bucket}')
  if(aws_profile == 'env-var'){
    command_in <- glue::glue('aws s3 sync {local_source} {destination_bucket} --acl bucket-owner-full-control')
  } # need to add acl permisisons as bucket-owner-full-control to upload/put objects in the bucket
  
  print(paste0('RUNNING: ',command_in))
  system(command_in)
}

#############
# Sync curated parquets from local EC2 to S3 bucket (recover-velsera-integration)
#############
# synapser::synLogin(daemon_acc, daemon_acc_password) # login into Synapse
sts_token <- synapser::synGetStsStorageToken(entity = config::get('cohort_builder_sts_location'), # sts enabled folder in Synapse linked to S3 location
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

s3SyncFromLocal(local_source = LOCAL_COHORT_LOCATION ,
                destination_bucket = AWS_DESTINATION_LOCATION,
                aws_profile = 'env-var')
