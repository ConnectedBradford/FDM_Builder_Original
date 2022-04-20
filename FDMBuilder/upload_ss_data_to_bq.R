library(odbc)
library(dplyr)
library(bigrquery)

upload_table_to_bq <- function(table_id, 
                               ss_dataset_id, 
                               bq_dataset_id, 
                               batch_size=100000) {
  options(scipen=999)
  # create connection to SQL server
  mssql_con <- dbConnect(odbc(),
                   Driver = "SQL Server",
                   Server = "bhts-conydevwd2",
                   Database = ss_dataset_id,
                   trusted_connection = TRUE)
  
  # create connection to BigQuery dataset
  bq_project_id <- "yhcr-prd-phm-bia-core" 
  upload_dataset <- bq_dataset(bq_project_id, bq_dataset_id)
  bq_con <- dbConnect(
    bigrquery::bigquery(),
    project = upload_dataset$project,
    dataset = upload_dataset$dataset
  )
  
  # check if upload table exists
  full_upload_table_id <- paste(upload_dataset$project, 
                                upload_dataset$dataset,  
                                table_id,
                                sep=".")
  upload_table_exists <- bq_table_exists(full_upload_table_id)
  # set variable for current batch uploaded to bq
  if (upload_table_exists) {
    current_bq_upload_batch <- unlist(tbl(bq_con, table_id)   
                                 %>% select(upload_batch) 
                                 %>% summarise(max = sql("MAX(upload_batch)"))   
                                 %>% collect())
  } else {
    current_bq_upload_batch <- 0
  }
  
  # set upload_batch to correct value based on current (if any) upload progress
  ss_table_colnames <- names(tbl(mssql_con, table_id) %>%  
                               head(1) %>%  
                               collect())
  upload_batch_in_ss_table_colnames <- "upload_batch" %in% ss_table_colnames
  if (upload_batch_in_ss_table_colnames) {
    # set any batches not uploaded to bq to NULL
    equalise_batch_sql <- paste(
      "UPDATE", table_id, "SET upload_batch = NULL",
      "WHERE upload_batch >", current_bq_upload_batch
    )
    invisible(dbGetQuery(mssql_con, equalise_batch_sql))
  } else if (!upload_table_exists) {
    # add upload_batch column to source table
    add_upload_batch_sql <- paste(
      "ALTER TABLE", table_id, "ADD upload_batch INT"
    )
    invisible(dbGetQuery(mssql_con, add_upload_batch_sql))
  } else {
    cat(paste("A table already exists with id", full_upload_table_id,
          "\nand upload looks to have been completed. Delete this table",
          "if you wish to start\nthe upload again.\n"))
    return(0)
  }
  
  # calculate values for iter loop and create upload_tbl object
  n_upload_rows <- unlist(
    tbl(mssql_con, table_id) 
    %>% filter(is.na(upload_batch)) 
    %>% count() 
    %>% collect()
  )
  n_iters <- ceiling(n_upload_rows/batch_size) + current_bq_upload_batch
  upload_tbl <- bq_table(upload_dataset$project, 
                         upload_dataset$dataset, 
                         table_id)
  start_idx = current_bq_upload_batch + 1
  progress_bar <- txtProgressBar(min=current_bq_upload_batch,  
                                 max=n_iters,  
                                 style=3,   
                                 initial=0, 
                                 width=40)
  for (i in start_idx:n_iters) {
    # update progress bar
    cat(paste(" Uploading chunk", i, "of", n_iters, ""))
    setTxtProgressBar(progress_bar, i)
    # add iter index to upload_batch for next n=batch_size rows in source table
    update_sql <- paste(
      "UPDATE TOP(", batch_size, ")", table_id, 
      "SET upload_batch =", i,
      "WHERE upload_batch is NULL"
    )
    invisible(dbGetQuery(mssql_con, update_sql))
    # collect rows where upload_batch == iter index and upload to BigQuery
    download_sql <- paste(
      "SELECT * FROM", table_id, 
      "WHERE upload_batch =", i
    )
    batch_data <- invisible(dbGetQuery(mssql_con, download_sql))
    invisible(bq_table_upload(upload_tbl, batch_data,
                              create_disposition="CREATE_IF_NEEDED",
                              write_disposition="WRITE_APPEND",
                              quiet=TRUE))
  }
  # drop upload_batch column in source table and BigQuery copy
  drop_upload_batch_ss_sql <- paste(
    "ALTER TABLE", table_id, "DROP COLUMN upload_batch"
  )
  invisible(dbGetQuery(mssql_con, drop_upload_batch_ss_sql))
  drop_upload_batch_bq_sql <- paste(
    "ALTER TABLE", full_upload_table_id, "DROP COLUMN upload_batch"
  )
  invisible(bq_project_query(bq_project_id,
                             drop_upload_batch_bq_sql,
                             quiet=TRUE))
  close(progress_bar)
}

###### CHANGE THESE VARIABLES TO SUIT #######

upload_table_id <- "tbl_SRAppointment"
upload_table_dataset_id <- "CY_FDM_TEST"
bq_dataset_id <- "CY_SAM_TEST"
upload_batch_size <- 2000000

#############################################

system.time({upload_table_to_bq(table_id=upload_table_id,  
                                ss_dataset_id=upload_table_dataset_id, 
                                bq_dataset_id=bq_dataset_id, 
                                batch_size=upload_batch_size)})

