#############################
# Cohort builder: Sync curated parquets to S3
#############################
### Params
COHORT_BUILDER_LOCATION <- './cohort_builder'
AWS_DESTINATION_BUCKET <- 'recover-velsera-integration'
AWS_DESTINATION_FOLDER <- 'cohort_builder'

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
sts_token <- synapser::synGetStsStorageToken(entity = 'syn53794904', # sts enabled folder in Synapse linked to S3 location
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)


s3SyncFromLocal(local_source = COHORT_BUILDER_LOCATION,
                destination_bucket = paste0('s3://', AWS_DESTINATION_BUCKET,'/',AWS_DESTINATION_FOLDER,'/'),
                aws_profile = 'env-var')