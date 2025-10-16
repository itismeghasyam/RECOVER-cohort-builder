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
S3_BUCKET <- 'recover-velsera-integration'
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
ARCHIVE_VERSION <- '2025-08-08'

AWS_DOWNLOAD_LOCATION <- paste0('./cohort_builder/main/archive/',ARCHIVE_VERSION,'/')
S3_MAIN_KEY <- paste0('main/archive/',ARCHIVE_VERSION,'/')
###########
## Get a list of all files to upload and their synapse locations(parentId) 
###########
STR_LEN_AWS_DOWNLOAD_LOCATION = stringr::str_length(AWS_DOWNLOAD_LOCATION)

## All files present locally from manifest
synapse_manifest <- read.csv(paste0('./current_manifest_', ARCHIVE_VERSION, '.tsv'), sep = '\t', stringsAsFactors = F) %>% 
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
