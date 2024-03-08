#############################
# Cohort builder: Parquet folder structure on External Parquet Data
# Folder structure: cohort->datasetType->participant->file; 
# We will add the cohort level annotation for file path while generating the DRS manifest
# datasets to do: dataset_fitbitintradaycombined, dataset_healthkitv2samples
#############################
main_start_time <- Sys.time()
########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)

########
# Set up Access and download dataset
########
synapser::synLogin()
ARCHIVE_VERSION <- '2024-02-29'
# To get a list of possible ARCHIVE_VERSION (dates), look at syn52506069 in Synapse.
# It will have a list of possible dates as subfolders
unlink('./cohort_builder/', recursive = TRUE) # remove old partitioning 

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

##################
## Partition and write parquet at cohort->datasetType->participantIdentifier level
##################
### Set1: dataset_enrolled_participants* (x3), dataset_googlefitsamples
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
    dplyr::group_by(cohort,datasetType,ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- subset_paths_df$datasetType %>% as.character()

### Set2: dataset_symptomlog*(x3),
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('symptomlog',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_symptomlog')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, DataPointKey) %>%
  unique()

symptomlog_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set3: dataset_fitbit*(x4), # still have to consider fitbit intradaycombined data, fitbitsleeplogs*
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
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set4: dataset_fitbitsleeplogs*(x2), # still have to consider fitbit intradaycombined data
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('fitbit',datasetType)) %>% 
  dplyr::filter(grepl('sleep',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitsleeplogs')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, LogId) %>% 
  unique()

fitbit_sleeplogs_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external)))
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set5: dataset_fitbitecg*(x2), # still have to consider fitbit intradaycombined data
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(grepl('fitbit',datasetType)) %>% 
  dplyr::filter(grepl('ecg',datasetType))

# get the participantIdentifier, logId mapping from dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_fitbitecg')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, FitbitEcgKey) %>% 
  unique() 

fitbit_ecglogs_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df) # to get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

### Set6: dataset_healthkitv2activitysummaries,dataset_healthkitv2statistics
subset_paths_df <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType %in% c('dataset_healthkitv2statistics',
                                   'dataset_healthkitv2activitysummaries')) 

healthkit_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) %>% 
    dplyr::mutate(datasetType = datasetType)
  
  temp_df %>% 
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set7: dataset_healthkitv2heartbeat* (x2)
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('heartbeat',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2heartbeat
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2heartbeat')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, HealthKitHeartbeatSampleKey) %>% 
  unique()

healthkit_heartbeat_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort, datasetType, ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))  
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set 8: dataset_healthkitv2electrocardiogram* (x2)
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('electro',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2electrocardiogram
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2electrocardiogram')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, HealthKitECGSampleKey) %>% 
  unique()

healthkit_ecg_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df)  # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort,datasetType,ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

# Set 9: dataset_healthkitv2workouts* (x2)
subset_paths_df <- valid_paths_ext_df  %>% 
  dplyr::filter(grepl('healthkit',datasetType)) %>% 
  dplyr::filter(grepl('workout',datasetType))

# get the participantIdentifier, logId mapping from dataset_healthkitv2workouts
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_healthkitv2workouts')

id_map_df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>%
  dplyr::select(ParticipantIdentifier, HealthKitWorkoutKey) %>% 
  unique()

healthkit_workouts_cohort_meta <- apply(subset_paths_df, 1, function(df_row){
  datasetType <- df_row[['datasetType']]
  parquet_path_external <- df_row[['parquet_path_external']]
  temp_df <- arrow::open_dataset(s3_external$path(as.character(parquet_path_external))) 
  
  temp_df <- temp_df %>% 
    dplyr::mutate(datasetType = datasetType) %>% 
    dplyr::left_join(id_map_df) # get ParticipantIdentifier column
  
  temp_df %>% 
    dplyr::group_by(cohort,datasetType,ParticipantIdentifier) %>% 
    arrow::write_dataset('cohort_builder/',
                         format = 'parquet',
                         max_partitions = 10000, # Max number of partitions possible, i.,e max participants 
                         hive_style = FALSE)
  print(paste0(datasetType,'--DONE'))
})
gc()
completed_datasets <- c(completed_datasets, subset_paths_df$datasetType)

#### BIG datasets [!! NOTE THIS SECTION WILL CRASH IF INSTANCE IS NOT BIG ENOUGH TO LOAD FULL DATASET]
## [[!! See if there is an alternate way of handling these two datasets?]]
## Set 9: dataset_fitbitintradaycombined
## Set 10: dataset_healthkitv2samples
main_end_time <- Sys.time()
print(main_end_time - main_start_time)