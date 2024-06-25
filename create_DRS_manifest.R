########
# Create a DRS manifest for RECOVER data to upload to CAVATIVA
# Currently testing code on Curated Pilot Data - syn53453677
########

########
# Required Libraries
########
library(tidyverse)
library(synapser)
synapser::synLogin()

########
# Synapse and Endpoint Parameters
########
drs_endpoint <- 'repo-prod.prod.sagebase.org/syn' 
# For files in synapse, i.e if using a fileview 
# in case of synapse drs endpoint the id will be <synapseId>.<version>
fileview_id <- 'syn53453677'

########
# Create a DRS manifest that can be imported
########
## download list of files to get synIds for DRS URL
## right now we are using fileview to get these synIds. we will move away from fileview

# get fileview
manifest_df <- synapser::synTableQuery(paste0("SELECT * FROM  ",fileview_id))$asDataFrame()

# create a DRS URL using synapseId and currentVersion of the file
manifest_df <- manifest_df %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(drs_uri = paste0('drs://',drs_endpoint,stringr::str_sub(id,start=4),'.',currentVersion)) %>% 
  dplyr::ungroup()

# Folder hierarchy in the name column
# Currently we have datasetType->participantIdentifier->file type file hierarchy below
manifest_df <- manifest_df %>% 
  dplyr::group_by() %>% 
  dplyr::mutate(name = paste(datasetType,ParticipantIdentifier,name,sep = '/')) %>% 
  dplyr::ungroup()

# We can include further annotations in the name(folder hierarchy) from the
# enrolled participants internal parquet data. 
# 'cohort' for eg., to get to cohort->datasetType->participantIdentifier->file
# We can also include annotations on the file, but without a fileview
# that means pulling them up everytime using synGetAnnotations for each file

## Only export drs_uri, name columns for now
manifest_df <- manifest_df %>% 
  dplyr::select(drs_uri, name)

write.csv(manifest_df,'drs_manifest.csv',row.names = FALSE, quote = FALSE)

# We can export more columns in the manifest file, all of those
# columns will be treated as annotations for the file 
# https://docs.cavatica.org/docs/import-from-a-drs-server


# Log into Cavatica and navigate to RECOVER test project
# Click "Add files" and GA4GH DRS option
# select from a manifest option and upload your manifest
# click refresh

# This will create a DRS manifest that can be imported
