###
# masked external datasets
##
main_start_time <- Sys.time()
########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)
library(tictoc)
source('~/recover-s3-synindex/awscli_utils.R')
source('~/recover-s3-synindex/params.R')

########
# Set up Access and download dataset
########
synapser::synLogin()
ARCHIVE_VERSION <- '2024-06-13'
# To get a list of possible ARCHIVE_VERSION (dates), look at syn52506069 in Synapse.
# It will have a list of possible dates as subfolders
# unlink(paste0('./cohort_builder/masked/archive/', ARCHIVE_VERSION), recursive = TRUE) # remove old partitioning

########
#### Set up access and Get list of valid datasets
#### archived versions of the external parquet dataset (syn52506069)
########
## Set up Token access
sts_token <- synapser::synGetStsStorageToken(entity = 'syn52506069', # sts enabled destination folder
                                             permission = 'read_only',   # request a read only token
                                             output_format = 'json')

s3_external <- arrow::S3FileSystem$create(access_key = sts_token$accessKeyId,
                                          secret_key = sts_token$secretAccessKey,
                                          session_token = sts_token$sessionToken,
                                          region="us-east-1")

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

base_s3_uri_external <- paste0(sts_token$bucket, "/", sts_token$baseKey, '/',ARCHIVE_VERSION)

# # access AWS s3 from env set sts token, sts token was requested from Synapse folder above
local_dest_external <- paste0('cohort_builder/external_parquets/', ARCHIVE_VERSION)
s3SyncToLocal(source_bucket = paste0('s3://', base_s3_uri_external,'/'),
              local_destination = local_dest_external,
              aws_profile = 'env-var')
local_dest_masked <- paste0('cohort_builder/masked/archive/', ARCHIVE_VERSION)

aa <- list.files(local_dest_external, recursive = T) %>% 
  as.data.frame() %>% 
  `colnames<-`('file_path') %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(file_name = str_split(file_path,'/')[[1]][3]) %>%
  dplyr::mutate(datasetType = str_split(file_path,'/')[[1]][1]) %>%
  dplyr::mutate(cohort = str_split(file_path,'/')[[1]][2]) %>%
  dplyr::ungroup()

## copy folder structure of external parquet to masked
datasetLevelFolders <- list.dirs(local_dest_external, recursive = F)
R.utils::copyDirectory(local_dest_external, local_dest_masked, recursive = F)
for(folder_dataset in datasetLevelFolders){
  new_folder <- stringr::str_replace(folder_dataset, 'external_parquets',replacement = 'masked/archive')
  dir.create(new_folder)
}
datasetCohortLevelFolders <- list.dirs(local_dest_external, recursive = T)
for(folder_cohort_dataset in datasetCohortLevelFolders){
  new_folder <- stringr::str_replace(folder_cohort_dataset, 'external_parquets',replacement = 'masked/archive')
  dir.create(new_folder)
}

## Recreate dataset by removing ParticipantIdentifier column
for(file_path in aa$file_path){
  print(file_path)
  temp_parquet <- arrow::open_dataset(paste0(local_dest_external,'/',file_path), format = 'parquet') %>% 
    dplyr::select(-ParticipantIdentifier) %>% 
    dplyr::collect()
  temp_writen <- arrow::write_parquet(temp_parquet, sink = paste0(local_dest_masked, '/',file_path))
  gc()
  rm(temp_parquet)
  rm(temp_writen)
}

aa2 <- list.files(local_dest_masked, recursive = T) %>% 
  as.data.frame() %>% 
  `colnames<-`('file_path') %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(file_name = str_split(file_path,'/')[[1]][3]) %>%
  dplyr::mutate(datasetType = str_split(file_path,'/')[[1]][1]) %>%
  dplyr::mutate(cohort = str_split(file_path,'/')[[1]][2]) %>%
  dplyr::ungroup()

all.equal(aa, aa2)

main_end_time <- Sys.time()
print(main_end_time - main_start_time)


############
# sync to s3
############
#############################
# Cohort builder: Sync curated parquets to S3
#############################
### Params
ARCHIVE_VERSION <- '2024-06-13'
LOCAL_COHORT_LOCATION <- paste0('./cohort_builder/masked/archive/',ARCHIVE_VERSION,'/')
AWS_DESTINATION_LOCATION <- paste0('s3://recover-main-project/masked/archive/',ARCHIVE_VERSION,'/')

####
# Required libraries and functions
####
library(tidyverse)
library(synapser)
# synapser::synLogin()
synapser::synLogin(authToken = 'eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTcwOTc0MzM2MiwiaWF0IjoxNzA5NzQzMzYyLCJqdGkiOiI1OTM5Iiwic3ViIjoiMzQ1NTYwNCJ9.VoSaQxTuziDsSlTq5HRhLUf1GHSq1pGUqcK-hpujDwbSjiBGJSoppaUOy6tPlelVF179OzsHYovHOqaTHIigoz2NoC1iKLXsb6E9vUOBBpQEsP8BW4mZ7rmDekfo76a0twpwiNmkLmSABuYuTkc_zMUOT6HIiN--bhAFrYY16uqTI2CxUsR4_ekjKwYzE3MhGZJ4smjn4tTJm0VIEIWBTF3WGl8jCe6bsOOIToz6k-09oZG5-Y9j601xfpA2PVLTmhGoKpyR_8r7PC-FeJZns2CFMPkABYemcx03ELa3tzT2hkvRo7Oc6XIfgN1PrwMM2uwXYe6ofY1RuN6wrzyq4w')

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
sts_token <- synapser::synGetStsStorageToken(entity = 'syn61607386', # sts enabled folder in Synapse linked to S3 location
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

s3SyncFromLocal(local_source = LOCAL_COHORT_LOCATION ,
                destination_bucket = AWS_DESTINATION_LOCATION,
                aws_profile = 'env-var')


############
# Get a manifest of files to upload
#############

library(synapser)

ARCHIVE_VERSION <- '2024-06-13'

LOCATION_TO_UPLOAD <- paste0('./cohort_builder/masked/archive/',ARCHIVE_VERSION,'/')
# Local location which needs to be indexed in Synapse

SYNAPSE_PARENT_ID = 'syn61607594' #Synapse location
# Synapse location where the S3 bucket objects are listed


## Replace '=' to '_' in dir names
all_dir <- list.dirs(LOCATION_TO_UPLOAD, recursive = TRUE)

# with = 
all_dir_err <- all_dir[grepl('=',all_dir)]

for(path_err in all_dir_err){
  new_path <- stringr::str_replace_all(path_err,'=','_')
  print(new_path)
  file.rename(path_err, new_path)
}

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

SYSTEM_COMMAND <- glue::glue('synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_masked_manifest.tsv {LOCATION_TO_UPLOAD}')

### If the above fails
# SYNAPSE_AUTH_TOKEN = Sys.getenv('SYNAPSE_AUTH_TOKEN')
# SYSTEM_COMMAND <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_manifest.tsv {AWS_DOWNLOAD_LOCATION}')

## Generate manifest file
system(SYSTEM_COMMAND)

## Create a versioned copy of current_manifest.csv
file.copy('current_masked_manifest.tsv', paste0('current_masked_manifest_', ARCHIVE_VERSION, '.tsv'), overwrite = TRUE)

################################
## Index S3 Objects in Synapse
################################

## First run sync_to_s3.R and generate_manifest.R in that order, before executing code below

#############
### Synapse credentials
#############
# Set the environment variables .Renviron file in your home folder. Refer to README for more details
SYNAPSE_AUTH_TOKEN = Sys.getenv('SYNAPSE_AUTH_TOKEN') 
# Login as synapse-service-sysbio-recover-data-storage-01, as this is the creator of S3 bucket
S3_BUCKET <- 'recover-main-project'
#############
# Required functions and libraries
#############
library(tidyverse)
library(synapser)
library(synapserutils)
library(rjson)
# synapser::synLogin(authToken=SYNAPSE_AUTH_TOKEN)
synapser::synLogin(authToken = 'eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTcwOTc0MzM2MiwiaWF0IjoxNzA5NzQzMzYyLCJqdGkiOiI1OTM5Iiwic3ViIjoiMzQ1NTYwNCJ9.VoSaQxTuziDsSlTq5HRhLUf1GHSq1pGUqcK-hpujDwbSjiBGJSoppaUOy6tPlelVF179OzsHYovHOqaTHIigoz2NoC1iKLXsb6E9vUOBBpQEsP8BW4mZ7rmDekfo76a0twpwiNmkLmSABuYuTkc_zMUOT6HIiN--bhAFrYY16uqTI2CxUsR4_ekjKwYzE3MhGZJ4smjn4tTJm0VIEIWBTF3WGl8jCe6bsOOIToz6k-09oZG5-Y9j601xfpA2PVLTmhGoKpyR_8r7PC-FeJZns2CFMPkABYemcx03ELa3tzT2hkvRo7Oc6XIfgN1PrwMM2uwXYe6ofY1RuN6wrzyq4w')

#############
# Required Parameters
#############
ARCHIVE_VERSION <- '2024-06-13'

AWS_DOWNLOAD_LOCATION <- paste0('./cohort_builder/masked/archive/',ARCHIVE_VERSION,'/')
S3_MAIN_KEY <- paste0('masked/archive/',ARCHIVE_VERSION,'/')
###########
## Get a list of all files to upload and their synapse locations(parentId) 
###########
STR_LEN_AWS_DOWNLOAD_LOCATION = stringr::str_length(AWS_DOWNLOAD_LOCATION)

## All files present locally from manifest
synapse_manifest <- read.csv(paste0('./current_masked_manifest_', ARCHIVE_VERSION, '.tsv'), sep = '\t', stringsAsFactors = F) %>% 
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
    
    # in s3 we have cohort=adults_v1 and cohort=pediatric_v1; locally we have replaced the = with _, so 
    # get it back before indexing
    s3_file_key = stringr::str_replace_all(s3_file_key,'cohort_','cohort=')
    
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

