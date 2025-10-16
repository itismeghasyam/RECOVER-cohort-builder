# ################
# # Create Sample set for NSRR transfer
# ################
library(tidyverse)
library(synapser)
library(synapserutils)
library(rjson)
# synapser::synLogin(authToken=SYNAPSE_AUTH_TOKEN)

# set.seed(1)
# base_path <- './cohort_builder/main/archive/2025-02-16/'
# base_path_adults <- './cohort_builder/main/archive/2025-02-16/adults/'
# base_path_pediatric <- './cohort_builder/main/archive/2025-02-16/pediatric/'
# 
# to_path <- './nsrr/example/'
# to_path_adults <- './nsrr/example/adults/'
# to_path_pediatric <- './nsrr/example/pediatric/'
# 
# 
# ### Source set [adults]
# # list of datasets
# datasets_adults <- list.files(base_path_adults)
# 
# # for each dataset pick a five participants
# for(dataset_in in datasets_adults){
#   dataset_path <- paste0(base_path_adults, dataset_in)
#   pids <- list.files(dataset_path)
#   pids_sample <- sample(pids, 5)
#   
#   if(dataset_in == 'dataset_fitbitintradaycombined'){
#     pids_sample <- c('RA12201-00785', pids_sample[1:4])
#   }
#   
#   for(pid in pids_sample){
#     file_path_from <- paste0(dataset_path,'/',pid)
#     file_path_to <- paste0(to_path_adults,dataset_in,'/',pid)
#     xfun::dir_create(file_path_to)
#     print(file_path_from)
#     # List all files in the source directory
#     files <- list.files(file_path_from, full.names = TRUE)
# 
#     # Copy all files to the destination directory
#     file.copy(from = files, to = file_path_to, overwrite = TRUE, recursive = FALSE)
#     # file.copy(from = file_path_from, to = file_path_to, recursive = T)
#   }
# }
# 
# 
# ### Source set [pediatric]
# # list of datasets
# datasets_pediatric <- list.files(base_path_pediatric)
# 
# # for each dataset pick a five participants
# for(dataset_in in datasets_pediatric){
#   dataset_path <- paste0(base_path_pediatric, dataset_in)
#   pids <- list.files(dataset_path)
#   pids_sample <- sample(pids, min(5, length(pids)))
#   
#   
#   for(pid in pids_sample){
#     file_path_from <- paste0(dataset_path,'/',pid)
#     file_path_to <- paste0(to_path_pediatric,dataset_in,'/',pid)
#     xfun::dir_create(file_path_to)
#     print(file_path_from)
#     # List all files in the source directory
#     files <- list.files(file_path_from, full.names = TRUE)
#     
#     # Copy all files to the destination directory
#     file.copy(from = files, to = file_path_to, overwrite = TRUE, recursive = FALSE)
#     # file.copy(from = file_path_from, to = file_path_to, recursive = T)
#   }
#   
#   
# }

### Create STS enabled location in Synapse

# folder_and_storage_location <-
#   synapser::synCreateS3StorageLocation(parent='syn51105296',
#                                        folder_name='nsrr_example',
#                                        folder=NULL,
#                                        bucket_name='recover-nsrr-transfer',
#                                        base_key='example',
#                                        sts_enabled=TRUE)

### Sync to S3
LOCAL_COHORT_LOCATION <- './nsrr/example/'
AWS_DESTINATION_LOCATION <- 's3://recover-nsrr-transfer/example/'

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
sts_token <- synapser::synGetStsStorageToken(entity = 'syn65870872', # sts enabled folder in Synapse linked to S3 location
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

# s3SyncFromLocal(local_source = LOCAL_COHORT_LOCATION ,
#                 destination_bucket = AWS_DESTINATION_LOCATION,
#                 aws_profile = 'env-var')
#############
#### Generate Manifest
#############
LOCATION_TO_UPLOAD <- LOCAL_COHORT_LOCATION

SYNAPSE_PARENT_ID = 'syn65870872' #Synapse location
# Synapse location where the S3 bucket objects are listed

#############
### Synapse credentials and params
#############
# Set the environment variables .Renviron file in your home folder. Refer to README for more details
SYNAPSECLIENT_INSTALL_PATH = Sys.getenv('SYNAPSECLIENT_INSTALL_PATH')
### First run data_sync.R

## The S3 bucket is synced to AWS_DOWNLOAD_LOCATION locally. 
## We will use synapse cmd line client manifest function to replicate the folder structure 
## but NOT upload the files. We will create a datafilehandleid in place later instead of uploading file

old_path <- Sys.getenv("PATH")

if(!grepl(SYNAPSECLIENT_INSTALL_PATH,old_path)){
  Sys.setenv(PATH = paste(old_path, SYNAPSECLIENT_INSTALL_PATH, sep = ":"))
  ## When we install synapser, it installs synapseclient and along with it the synapse cmd line client
  ## We need to add the location of synapseclient to the system path so that it can recognize synapse cmd 
}

### If the above fails
# SYNAPSE_AUTH_TOKEN = Sys.getenv('SYNAPSE_AUTH_TOKEN')
SYSTEM_COMMAND <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_manifest.tsv {LOCATION_TO_UPLOAD}')

## Generate manifest file
system(SYSTEM_COMMAND)


##### Index in Synapse
S3_BUCKET <- 'recover-nsrr-transfer'
AWS_DOWNLOAD_LOCATION <- LOCAL_COHORT_LOCATION
S3_MAIN_KEY <- 'example/'

STR_LEN_AWS_DOWNLOAD_LOCATION = stringr::str_length(AWS_DOWNLOAD_LOCATION)

## All files present locally from manifest
synapse_manifest <- read.csv('current_manifest.tsv', sep = '\t', stringsAsFactors = F) %>% 
  dplyr::filter(path != paste0(AWS_DOWNLOAD_LOCATION,'owner.txt')) %>%  # need not create a dataFileHandleId for owner.txt
  dplyr::rowwise() %>% 
  dplyr::mutate(file_key = stringr::str_sub(string = path, start = STR_LEN_AWS_DOWNLOAD_LOCATION+1)) %>% # location of file from home folder of S3 bucket
  dplyr::mutate(s3_file_key = paste0(S3_MAIN_KEY, file_key)) %>% # the namespace for files in the S3 bucket is S3::bucket/main/
  dplyr::mutate(md5_hash = as.character(tools::md5sum(path))) %>% 
  dplyr::ungroup()

## find those files that are not in the fileview - files that need to be indexed
synapse_manifest_to_upload <- synapse_manifest

#############
# Index in Synapse
#############
## For each file index it in Synapse given a parent synapse folder
if(nrow(synapse_manifest_to_upload) > 0){ # there are some files to upload
  for(file_number in seq(nrow(synapse_manifest_to_upload))){
    # print(file_number)
    # file and related synapse parent id 
    file_= synapse_manifest_to_upload$path[file_number]
    parent_id = synapse_manifest_to_upload$parent[file_number]
    s3_file_key = synapse_manifest_to_upload$s3_file_key[file_number]
    # this would be the location of the file in the S3 bucket, in the local it is at {AWS_DOWNLOAD_LOCATION}/
    
    absolute_file_path <- tools::file_path_as_absolute(file_) # local absolute path
    
    temp_syn_obj <- synapser::synCreateExternalS3FileHandle(
      bucket_name = S3_BUCKET,
      s3_file_key = s3_file_key, #
      file_path = absolute_file_path,
      parent = parent_id
    )
    
    # synapse does not accept ':' (colon) in filenames, so replacing it with '_colon_'
    new_fileName <- stringr::str_replace_all(temp_syn_obj$fileName, ':', '_colon_')
    
    f <- File(dataFileHandleId=temp_syn_obj$id,
              parentId=parent_id,
              name = new_fileName) ## set the new file name
    
    f <- synStore(f)
    
  }
}








