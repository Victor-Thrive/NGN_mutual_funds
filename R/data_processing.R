# Load required libraries
library(httr)
library(readxl)
library(tidyverse)
library(lubridate)
library(janitor)
library(duckdb)
library(DBI)



# Generate Url
# Generate URL for the most recent Friday
library(lubridate)

generate_url <- function() {
  # Get today's date
  today <- Sys.Date()
  
  # Calculate the most recent Friday
  most_recent_friday <- today - (wday(today) - 6) %% 7
  
  # Add ordinal suffix to the day
  day <- day(most_recent_friday)
  ordinal <- ifelse(day %% 10 == 1 & day != 11, "st",
                    ifelse(day %% 10 == 2 & day != 12, "nd",
                           ifelse(day %% 10 == 3 & day != 13, "rd", "th")))
  
  formatted_date <- paste0(day, ordinal, "-", format(most_recent_friday, "%B-%Y"))
  
  # Construct the URL
  url <- paste0("https://sec.gov.ng/wp-content/uploads/",
                format(most_recent_friday, "%Y/%m/"),
                "NAV-as-at-", formatted_date, ".xlsx")
  
  return(url)
}

# Example usage
url <- generate_url()
print(url)


# save the new url in a variable
url <- generate_url()

# testing url with 
url <- "https://sec.gov.ng/wp-content/uploads/2024/11/NAV-as-at-22nd-November-2024.xlsx"

# Extract the date from the file-name
path <- url
date_string <- str_extract(path, "(?<=as-at-)[^\\.]+")
date_parsed <- dmy(date_string)
# Print the extracted and parsed date
print(date_parsed)

# download data and save 
file_name <- paste0("data/",date_parsed, ".xlsx")
download.file(url, file_name, mode = "wb")  # Use binary mode for non-text files

# Check file type and load
if (tools::file_ext(file_name) == "xlsx") {
  # Load Excel file
  loaded_data <- read_excel(file_name, skip = 2)
} else if (tools::file_ext(file_name) == "zip") {
  # Unzip file if it's a zip archive
  unzip(file_name, exdir = "unzipped_folder")
  # Load the first Excel file found inside
  excel_files <- list.files("unzipped_folder", pattern = "\\.xlsx$", full.names = TRUE)
  if (length(excel_files) > 0) {
    loaded_data <- read_excel(excel_files[1], skip = 2)
  } else {
    stop("No Excel files found in the zip archive.")
  }
} else {
  stop("Unexpected file type.")
}

# Optionally, check if new_data is NULL and handle it appropriately
if (!is.null(loaded_data)) {
  # Proceed with your data processing
  print("Data downloaded and read successfully.")
  # Further processing of new_data goes here
} else {
  print("No data to process.")
}

# Define categories and their counts
category_names <- c(
  "Equity Based" = 18,
  "Money Market" = 36,
  "Bond Fixed Income" = 36,
  "Dollar Eurobond" = 15,
  "Fixed Income" = 13,
  "Real Estate Investment Trust" = 5,
  "Balanced" = 29,
  "Ethical" = 3,
  "Sharia Equities" = 2,
  "Sharia Fixed Income" = 12,
  "Sharia Fixed Income Balance" = 1,
  "Specialised Fund" = 1,
  "Infrastructure" = 2,
  "ETF" = 12
)
# Create the category vector
category <- rep(names(category_names), category_names)

# Add the date column
loaded_data$date <- date_parsed

df <- loaded_data |>
  clean_names() |>
  filter(!grepl("[^0-9]", s_n)) |>
  drop_na(s_n) |>
  mutate(date = date_parsed, category = category)

names(df) <- gsub("_?[0-9]+", "", names(df))

names(df) <- gsub("percent_to_total", "percent_on_total", names(df))

# Identify duplicate column names
duplicates <- names(df)[duplicated(names(df))]

# Add suffix "_1", "_2", etc. to duplicate column names
for (i in 1:length(duplicates)) {
  count <- sum(names(df) == duplicates[i])
  names(df)[names(df) == duplicates[i]] <- paste0(duplicates[i], "_", seq(1, count))
}


df$fund_manager <- str_replace_all(df$fund_manager, "(Management|Limited)", function(x) ifelse(x == "Management", "Mgt", "Ltd"))


# con <- dbConnect(duckdb::duckdb())  # For an in-memory database
con <- dbConnect(duckdb::duckdb(), dbdir = "R/mutual_funds.db")

# Write the initial data to the DuckDB table (overwriting if it exists)
dbWriteTable(con, "mutual_funds", df, overwrite = TRUE)

# Append the new data to the existing DuckDB table
#dbWriteTable(con, "students", df, append = TRUE)

#list tables
dbListTables(con)

#dbExecute(con, "DROP TABLE table")
#result <- dbGetQuery(con, "SELECT * FROM mutual_funds")
#dim(result)

# Verify by reading back the table
#result <- dbReadTable(con, "mutual_funds")
#print(result)

# Disconnect from the database
dbDisconnect(con)
#write_csv(df,"clean.csv")

############### THIS CODE IS WORKING FINE