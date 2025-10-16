########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)
library(tictoc)

########
# Set up Access to dataset
########
synapser::synLogin()

## Set up Token access
sts_token <- synapser::synGetStsStorageToken(entity = 'syn65870872', # sts enabled destination folder
                                             permission = 'read_only',   # request a read only token
                                             output_format = 'json')

s3_external <- arrow::S3FileSystem$create(access_key = sts_token$accessKeyId,
                                          secret_key = sts_token$secretAccessKey,
                                          session_token = sts_token$sessionToken,
                                          region="us-east-1")

## Get list of datasets in the S3 bucket
base_s3_uri_adults <- paste0(sts_token$bucket, "/", sts_token$baseKey,'/adults/')
parquet_datasets_adults <- s3_external$GetFileInfo(arrow::FileSelector$create(base_s3_uri_adults, recursive=F))

base_s3_uri_pediatric <- paste0(sts_token$bucket, "/", sts_token$baseKey,'/pediatric/')
parquet_datasets_pediatric <- s3_external$GetFileInfo(arrow::FileSelector$create(base_s3_uri_pediatric, recursive=F))

parquet_datasets <- c(parquet_datasets_adults, parquet_datasets_pediatric)

i <- 0
valid_paths <- character()
for (dataset in parquet_datasets) {
  if (grepl('recover-nsrr-transfer/example', dataset$path, perl = T, ignore.case = T)) {
    i <- i+1
    cat(i)
    cat(":", dataset$path, "\n")
    valid_paths <- c(valid_paths, dataset$path)
  }
}

valid_paths_df <- valid_paths %>%
  as.data.frame() %>%
  `colnames<-`('parquet_path') %>%
  dplyr::rowwise() %>%
  dplyr::mutate(datasetType = str_split(parquet_path,'/')[[1]][4]) %>%
  dplyr::mutate(cohort = str_split(parquet_path,'/')[[1]][3]) %>%
  dplyr::ungroup() 

########
# Read a dataset [whole]
########
subset_paths <- valid_paths_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitintradaycombined') %>% 
  dplyr::filter(cohort == 'adults')

# get parquet path in S3
parquet_path <- subset_paths$parquet_path[1]
print(parquet_path)

# read parquet
parquet.df <- arrow::open_dataset(s3_external$path(as.character(parquet_path))) 

# load all parquet into local env
parquet.df.local <- parquet.df %>% 
  dplyr::collect()


########
# Read a dataset: data from a particular participant
########
participant_id_considered <- parquet.df.local$ParticipantIdentifier[1]
participant_parquet_path <- paste0(parquet_path,'/',participant_id_considered)
print(participant_parquet_path)

# read participant specific parquet
participant.parquet.df <- arrow::open_dataset(s3_external$path(as.character(participant_parquet_path))) 

# load all participant specific parquet into local env
participant.parquet.df.local <- participant.parquet.df %>% 
  dplyr::collect()


########
# Read a dataset: data from a particular FILE
########
participant_id_considered <- unique(parquet.df.local$ParticipantIdentifier)[4]
participant_parquet_path <- paste0(parquet_path,'/',participant_id_considered)
print(participant_parquet_path)

# read participant specific parquet
participant.parquet.df <- arrow::open_dataset(s3_external$path(as.character(participant_parquet_path))) 

# list of all files in the participant parquet
file_list <- participant.parquet.df$files

# read the first file in the list
file.parquet.df.local <- arrow::read_parquet(s3_external$path(as.character(file_list[1])), format = 'parquet') %>% 
  dplyr::collect()
