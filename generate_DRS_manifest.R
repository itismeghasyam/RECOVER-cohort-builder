################################
## Generate DRS manifest using synapse manifest
################################

## First run sync_to_s3.R, generate_manifest.R and synIndex.R in that order, before executing code below

#############
### Synapse credentials
#############
# Set the environment variables .Renviron file in your home folder. Refer to README for more details
SYNAPSE_AUTH_TOKEN = Sys.getenv('SYNAPSE_AUTH_TOKEN') 
# Login as synapse-service-sysbio-recover-data-storage-01, as this is the creator of S3 bucket
S3_BUCKET <- 'recover-velsera-integration'
DRS_URL_BASE <- 'drs://repo-prod.prod.sagebase.org/'
ARCHIVE_VERSION <- '2025-08-08'

#############
# Required functions and libraries
#############
library(tidyverse)
library(synapser)
library(synapserutils)
library(rjson)
# synapser::synLogin(authToken=SYNAPSE_AUTH_TOKEN)

# Build fileview from all parentIds in a synapse manifest file
buildFileview <- function(syn_manifest){
  
  parent_ids <- syn_manifest$parent %>% 
    unique() %>% 
    as.character()
  
  bulk_manifest <- lapply(parent_ids, function(syn.id){
    synapser::synGetChildren(syn.id)$asList() %>% 
      data.table::rbindlist(fill = T) %>% 
      dplyr::mutate(parent = syn.id)
  }) %>% data.table::rbindlist(fill = T) %>% 
    dplyr::filter(grepl('FileEntity',type))
  
}

#############
# Get synIds of files to index and create DRS manifest
#############

# Get fileview using synapse manifest - this will give synId for each file
synapse_manifest <- read.csv(paste0('current_manifest_',ARCHIVE_VERSION,'.tsv'), sep = '\t', stringsAsFactors = F) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(name = paste0(tail(str_split(path,'/')[[1]],1), collapse = '/')) %>% 
  dplyr::ungroup()

# Get fileview using manifest
synapse_fileview <- buildFileview(synapse_manifest)

# get meta file to create DRS manifest
meta_drs <- synapse_fileview %>% 
  dplyr::left_join(synapse_manifest) %>% 
  dplyr::rowwise() %>% 
  # dplyr::mutate(name = paste0(tail(str_split(path,'/')[[1]],3), collapse = '/')) %>% 
  dplyr::mutate(drs_uri = paste0(DRS_URL_BASE,id,'.', versionNumber)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(path, drs_uri) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(name = paste0(tail(str_split(path,'/')[[1]],5), collapse = '/')) %>% 
  dplyr::mutate(name = paste0('RECOVER_DigitalHealth_RawData/',name)) %>% 
  dplyr::ungroup() %>% 
  # dplyr::left_join(synapse_fileview) %>% 
  dplyr::select(drs_uri, name)

## Remove all datasets like symptoms, treatment from drs manifest
## i.e 
## c('dataset_enrolledparticipants_customfields_symptoms',
# 'dataset_enrolledparticipants_customfields_treatments',
# 'Dataset_symptomlog',
# 'dataset_symptomlog_value_symptoms',
# 'dataset_symptomlog_value_treatments')
## AND get cohort based DRS manifests

# Get adults manifest
adults_drs_manifest <- meta_drs %>% 
  dplyr::filter(grepl('adults',name)) %>% 
  dplyr::filter(!grepl('symptoms',name)) %>% 
  dplyr::filter(!grepl('treatments',name)) %>% 
  dplyr::filter(!grepl('symptomlog',name))  

# Get pediatric manifest
pediatric_drs_manifest <- meta_drs %>% 
  dplyr::filter(grepl('pediatric',name)) %>% 
  dplyr::filter(!grepl('symptoms',name)) %>% 
  dplyr::filter(!grepl('treatments',name)) %>% 
  dplyr::filter(!grepl('symptomlog',name))  

## Write to file
write.csv(adults_drs_manifest, paste0('adults_drs_manifest_',ARCHIVE_VERSION,'.csv'), quote = FALSE, row.names = FALSE)
write.csv(pediatric_drs_manifest, paste0('pediatric_drs_manifest_',ARCHIVE_VERSION,'.csv'), quote = FALSE, row.names = FALSE)
write.csv(meta_drs, paste0('drs_manifest_',ARCHIVE_VERSION,'.csv'), quote = FALSE, row.names = FALSE)

## Store to Synapse

synapse.folder.id <- 'syn59474215'
OUTPUT_FILE <- paste0('adults_drs_manifest_',ARCHIVE_VERSION,'.csv') # name your file
synStore(File(OUTPUT_FILE, parentId=synapse.folder.id))

synapse.folder.id <- 'syn59474216'
OUTPUT_FILE <- paste0('pediatric_drs_manifest_',ARCHIVE_VERSION,'.csv') # name your file
synStore(File(OUTPUT_FILE, parentId=synapse.folder.id))

