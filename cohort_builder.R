## !!! REDO Fitbit Intraday Combined!

#############################
# Cohort builder: Parquet folder structure on External Parquet Data
# Folder structure: cohort->datasetType->participant->file; 
# We will add the cohort level annotation for file path while generating the DRS manifest
# datasets to do: dataset_fitbitintradaycombined, dataset_healthkitv2samples
#############################

#############################
# Code performance on a r6a.8x (256GB instance). On ARCHIVE_VERSIONS of external parquet data

########### custom limits set for max rows per file and max files open
## This helps in keeping the number of parquets files per participant small and 
## in NOT splitting the files per each participant into multiple small parquets(~40-60KB each)
# 10.20058 hours for 2024-05-21
# 7.307739 hours for 2024-04-23
# 3.535469 hours for 2024-02-29

########### NO limits set for max rows per file and max files open
# 3.906232 hours for 2024-02-01
# 1.167115 hours for 2023-12-06
# 1.280577 hours for 2023-11-10
# 21 mins for 2023-09-21
# 15 mins for 2023-09-08
#############################

main_start_time <- Sys.time()
########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)
library(tictoc)

########
# Set up Access and download dataset
########
synapser::synLogin()
ARCHIVE_VERSION <- '2025-08-08'
# To get a list of possible ARCHIVE_VERSION (dates), look at syn52506069 in Synapse.
# It will have a list of possible dates as subfolders
# unlink(paste0('./cohort_builder/main/archive/', ARCHIVE_VERSION), recursive = TRUE) # remove old partitioning

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

## Get list of datasets in the S3 bucket
base_s3_uri_external <- paste0(sts_token$bucket, "/", sts_token$baseKey,'/',ARCHIVE_VERSION)
parquet_datasets_external <- s3_external$GetFileInfo(arrow::FileSelector$create(base_s3_uri_external, recursive=F))

## Get all valid datasets
i <- 0
valid_paths <- character()
for (dataset in parquet_datasets_external) {
  if (grepl('recover-main-project/main/archive/', dataset$path, perl = T, ignore.case = T)) {
    i <- i+1
    cat(i)
    cat(":", dataset$path, "\n")
    valid_paths <- c(valid_paths, dataset$path)
  }
}

## Get dataset type (for eg., dataset_enrolledparticipants)
valid_paths_ext_df <- valid_paths %>% 
  as.data.frame() %>% 
  `colnames<-`('parquet_path_external') %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(datasetType = str_split(parquet_path_external,'/')[[1]][5]) %>%
  dplyr::ungroup() 

# %>% 
#   dplyr::filter(!datasetType %in% c('dataset_enrolledparticipants_customfields_symptoms',
#                                    'dataset_enrolledparticipants_customfields_treatments',
#                                    'dataset_symptomlog',
#                                    'dataset_symptomlog_value_symptoms',
#                                    'dataset_symptomlog_value_treatments'))
# For all these datasets, index but don't include them in DRS manifest. 

##################
## Partition and write parquet at cohort->datasetType->participantIdentifier level
##################
### Set1: dataset_enrolled_participants* (x3), dataset_googlefitsamples; MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(!grepl('fitbit',datasetType)) %>% 
  dplyr::filter(!grepl('healthkit',datasetType)) %>%  
  dplyr::filter(!grepl('symptomlog',datasetType))

subset_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>% 
    dplyr::mutate(datasetType = datasetType)
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- subset_paths_df$datasetType %>% as.character()

### Set2: dataset_symptomlog*(x3); MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('symptomlog',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_symptomlog')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, DataPointKey) %>%
  unique() %>% 
  as.data.frame()

symptomlog_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set3: dataset_fitbit*(x4), # still have to consider fitbit intradaycombined data, fitbitsleeplogs*; MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('fitbit',datasetType)) %>% 
  dplyr::filter(!grepl('intraday',datasetType)) %>% 
  dplyr::filter(!grepl('sleep',datasetType)) %>% 
  dplyr::filter(!grepl('ecg',datasetType))

fitbit_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>%
    dplyr::mutate(datasetType = datasetType)
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_rows_per_file = 100000, 
                         max_open_files = 4000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set4: dataset_fitbitsleeplogs*(x2), # still have to consider fitbit intradaycombined data; MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('fitbit',datasetType)) %>% 
  dplyr::filter(grepl('sleep',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitsleeplogs')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, LogId) %>% 
  unique() %>% 
  as.data.frame()

fitbit_sleeplogs_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external)))
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_rows_per_file = 100000,
                         max_open_files = 4000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set5: dataset_fitbitecg*(x2), # still have to consider fitbit intradaycombined data; MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('fitbit',datasetType)) %>% 
  dplyr::filter(grepl('ecg',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitecg')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, FitbitEcgKey) %>% 
  unique() %>% 
  as.data.frame()

fitbit_ecglogs_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df) # to get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set6: dataset_healthkitv2activitysummaries,dataset_healthkitv2statistics; MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType %in% c('dataset_healthkitv2statistics',
                                   'dataset_healthkitv2activitysummaries')) 

healthkit_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>% 
    dplyr::mutate(datasetType = datasetType)
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set7: dataset_healthkitv2heartbeat* (x2); MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('heartbeat',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2heartbeat
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2heartbeat')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, HealthKitHeartbeatSampleKey) %>% 
  unique() %>% 
  as.data.frame()

healthkit_heartbeat_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))  
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set 8: dataset_healthkitv2electrocardiogram* (x2); MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('electro',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2electrocardiogram
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2electrocardiogram')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, HealthKitECGSampleKey) %>% 
  unique() %>% 
  as.data.frame()

healthkit_ecg_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set 9: dataset_healthkitv2workouts* (x2); MAX_ROWS_PER_FILE = 100,000
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('workout',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2workouts
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2workouts')

# id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
#   dplyr::select(ParticipantIdentifier, HealthKitWorkoutKey) %>% 
#   unique() %>% 
#   as.data.frame()

healthkit_workouts_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) 
  # %>% 
  #   dplyr::left_join(id_map_df) # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
    dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
    arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                         format = 'parquet',
                         max_open_files = 4000,
                         max_rows_per_file = 100000,
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

##################
## BIG datasets - [dataset_fitbitintradaycombined, dataset_healthkitv2samples]
#### [!! NOTE: USE ATLEAST 128GB RAM INSTANCE FOR THIS SECTION; WILL CRASH IF RAM IS NOT BIG ENOUGH TO LOAD FULL DATASET]
##################
## Set 10: dataset_fitbitintradaycombined; MAX_ROWS_PER_FILE = 5,000,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitintradaycombined')

# tic()
# Get number of rows per participant in the given dataset(dataset_fitbitintradaycombined)
participant_ids <- arrow::open_dataset(s3_external$path(as.character(subset_paths_df$parquet_path_external))) %>%
  dplyr::group_by(ParticipantIdentifier) %>%
  dplyr::count() %>% 
  dplyr::collect() %>% 
  as.data.frame()
# %>% 
# dplyr::arrange(n) # ascending order => participant with least number of rows comes first
# 
# participant_ids <- bb %>% dplyr::select(ParticipantIdentifier, nExternal, nCohort) %>% dplyr::filter(!is.na(nCohort)) %>% dplyr::select(ParticipantIdentifier, n = nExternal)

MAX_ROWS_PER_CHUNK <- 800000000 
# 1 Billion rows per chunk. 
# Four participants have above 40Million rows each, the next have around 10Million and less, and so on. 
# Hence instead of treating it as participants per chunk, we will pick the participants
# based on number of rows in a chunk (as it keeps the chunk size approx same)
participant_ids$n_cumsum <- cumsum(as.numeric(participant_ids$n))
participant_ids$batch <- as.integer(participant_ids$n_cumsum/MAX_ROWS_PER_CHUNK)
participant_ids_chunks <- split(participant_ids, participant_ids$batch)
## Basically creates a subset of the dataset containing all the following participants
## Reduce this number if you hit RAM limits, it will increase compute time as we will now have
## more partitions, and for each partition we have to traverse the whole dataset to filter data
## NOTE: If you get into lot many partitions, try increasing the instance. Use memory optimized
## instances like r6a.4x(128GB) - this should be enough, r6a.8X (256GB memory)[this is best]

print(paste0('Total number of chunks is ', length(participant_ids_chunks)))
current_chunk <- 26
#### Not present in cohort 
#### First try: 1-5 chunk done; "dataset_fitbitintradaycombined--DONE"
#### Second try: 6-15Chunks  done; "dataset_fitbitintradaycombined--DONE"
#### Third try: 16-25Chunks done; "dataset_fitbitintradaycombined--DONE"
#### Fourth try: Chunks  done; "dataset_fitbitintradaycombined--DONE"
#### Fifth try: Chunks  done; "dataset_fitbitintradaycombined--DONE"
#### Sixth try: Chunks  

#### Present in cohort/ half present
## first try 2 chunks doing right now;
participant_ids_chunks <- participant_ids_chunks[current_chunk:min(50, length(participant_ids_chunks))]

tictoc::tic()
fitbit_intradaycombined_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  
  # Deal with data in chunks, so as to be easier on RAM
  for(current_participant_chunk in participant_ids_chunks){
    print(paste0('Current chunk is ', current_chunk))
    temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>%
      dplyr::filter(ParticipantIdentifier %in% current_participant_chunk$ParticipantIdentifier) %>%
      dplyr::mutate(datasetType = datasetType)
    
    temp_df %>%
      dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
      dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
      arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                           format = 'parquet',
                           max_open_files = 4000,
                           max_rows_per_file = 5000000,
                           max_partitions = 10000, # Max number of partitions possible, i.,e max participants
                           hive_style = FALSE)
    current_chunk <- current_chunk+1
    gc()
  }
  print(paste0(datasetType,'--DONE'))
})
tictoc::toc()
rm(current_chunk)

## Set 11: dataset_healthkitv2samples; MAX_ROWS_PER_FILE = 500,000
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2samples') 

# tic()
participant_ids <- arrow::open_dataset(s3_external$path(as.character(subset_paths_df$parquet_path_external))) %>%
  dplyr::group_by(ParticipantIdentifier) %>%
  dplyr::count() %>% 
  dplyr::collect() %>% 
  dplyr::arrange(n)

MAX_ROWS_PER_CHUNK <- 7000000
# 7 Million as one participant has around 7.65 Million rows, the highest. 
# the second highest has around 1.2 Million rows - HUGE difference.
# Hence instead of treating it as participants per chunk, we will pick the participants
# based on number of rows in a chunk
participant_ids$n_cumsum <- cumsum(as.numeric(participant_ids$n))
participant_ids$batch <- as.integer(participant_ids$n_cumsum/MAX_ROWS_PER_CHUNK)
participant_ids_chunks <- split(participant_ids, participant_ids$batch)
## Basically creates a subset of the dataset containing all the following participants
## Reduce this number if you hit RAM limits, it will increase compute time as we will now have
## more partitions, and for each partition we have to traverse the whole dataset to filter data
## NOTE: If you get into lot many partitions, try increasing the instance. Use memory optimized
## instances like r6a.4x(128GB) - this should be enough, r6a.8X (256GB memory)[this is best]

print(paste0('Total number of chunks is ', length(participant_ids_chunks)))
current_chunk <- 1;

healthkitv2samples_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  
  # Deal with data in chunks, so as to be easier on RAM
  for(current_participant_chunk in participant_ids_chunks){
    print(paste0('Current chunk is ', current_chunk))
    temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>%
      dplyr::filter(ParticipantIdentifier %in% current_participant_chunk$ParticipantIdentifier) %>% 
      dplyr::mutate(datasetType = datasetType) 
    
    temp_df %>% 
      dplyr::mutate(cohortGRP = cohort, ParticipantIdentifierGRP = ParticipantIdentifier) %>% # Add cohort and ParticipantIdentifier grouping columns
      dplyr::group_by(cohortGRP,datasetType,ParticipantIdentifierGRP) %>% 
      arrow::write_dataset(paste0('cohort_builder/main/archive/', ARCHIVE_VERSION),
                           format = 'parquet',
                           max_open_files = 4000,
                           max_rows_per_file = 500000,
                           max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                           hive_style = FALSE)
    current_chunk <- current_chunk+1
    gc()
  }
  print(paste0(datasetType,'--DONE'))
})
# toc()
rm(current_chunk)

### Rename cohort from adults_v1 to adults, similarly for pediatric
# Renaming folders here as it is easier (compute and time wise) and 
# as of now we only have one version, v1. When we have more versions
# we will revisit this
archive_path <- paste0('./cohort_builder/main/archive/', ARCHIVE_VERSION)
file.rename(paste0(archive_path,'/adults_v1'), paste0(archive_path,'/adults'))
file.rename(paste0(archive_path,'/pediatric_v1'), paste0(archive_path,'/pediatric'))

main_end_time <- Sys.time()
print(main_end_time - main_start_time)
