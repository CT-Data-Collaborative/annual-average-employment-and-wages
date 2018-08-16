library(dplyr)
library(datapkg)
library(readxl)
library(gdata)
library(tidyr)

##################################################################
#
# Processing Script for Annual-Average-Employment-and-Wages
# Created by Jenna Daly
# On 03/08/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
data_location <- grep("raw", sub_folders, value=T)
path <- (paste0(getwd(), "/", data_location))
drops <- c("")
round_up = function(x) trunc(x+0.5)

###Create subsets

##Town Data######################################################################################################################

#grabs town xls files
town_path <- file.path(path, "town")
town_xls <- dir(town_path, pattern = "Town")

#Create empty data frame
all_towns <- data.frame(stringsAsFactors = F)
for (i in 1:length(town_xls)) {
  current_file = read.xls(paste0(town_path, "/", town_xls[i]), sheet = 1, header = TRUE)  
  current_file <- current_file[,c(1:7)]
  colnames(current_file) <- c("Town/County", "NAICS Code", "Category", "Number of Employers", 
                              "Annual Average Employment", "Total Annual Wages", "Annual Average Wage")
  
  #only keep populated columns (remove blank columns)
  current_file <- current_file[ , !(names(current_file) %in% drops)]
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(town_xls[i])), "")))
  get_year <- substr(get_year, 1, 4)
  current_file$Year <- get_year
  #only take rows we need to processing, 
  #based on whenever the "Category" column is equal to "Total - All Industries" or "Total Government"
  current_file$`Town/County`[current_file$`Town/County` == ""] <- NA
  current_file$`Town/County` <- as.character(current_file$`Town/County`)
  currentTown = current_file[1,1]
  for (i in 1:nrow(current_file)) {
    if(is.na(current_file[i,1])) {
      current_file[i,1] <- currentTown
    } else {
      currentTown <- current_file[i,1]
    }
  }
  current_file <- current_file[current_file$Category %in% c("Total - All Industries", "Total Government"),]
  
  #Select columns
  current_file <- current_file[, c("Town/County", "Category", "Number of Employers", 
                                   "Annual Average Employment", "Annual Average Wage", "Year")] 
  
  #Convert to long format
  current_file <- gather(current_file, Variable, Value, 3:5, factor_key=F)
  
  #Configure Value column
  current_file$Value <- gsub("\\$", "", current_file$Value)
  current_file$Value <- gsub(",", "", current_file$Value)

  #Round Value column
  #makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
  current_file$Value <- round_up(as.numeric(current_file$Value))
  #bind together
  all_towns <- rbind(all_towns, current_file) 
}

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(all_towns, fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[ grep("Connecticut", town_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(all_towns, current_file, currentTown, fips)

##County Data####################################################################################################################

#grabs county xls files
county_path <- file.path(path, "county")
county_xls <- dir(county_path, pattern = "CNTY")

#read in entire xls file (all sheets)
read_excel_allsheets <- function(filename) {
  sheets <- readxl::excel_sheets(filename)
  x <-    lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  names(x) <- sheets
  x
}

#Create a list of all sheet data for all years
for (i in 1:length(county_xls)) {
  mysheets <- read_excel_allsheets(paste0(county_path, "/", county_xls[i]))
  #Cycle through all sheets, extract sheet name, and assign each sheet (county) its own data frame
  for (j in 1:length(mysheets)) {
    current_county_name <- colnames(mysheets[[j]])[1]
    current_county_file <- mysheets[[j]] 
    get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(county_xls[i])), "")))
    get_year <- get_year + 2000
    assign(paste0(current_county_name, "_", get_year), current_county_file)
  }
}

rm(current_county_file)
dfs <- ls()[sapply(mget(ls(), .GlobalEnv), is.data.frame)]
county_data <- grep("County_", dfs, value=T)

all_counties <- data.frame(stringsAsFactors = F)
#Process each county file
for (i in 1:length(county_data)) {
  #grab first county file
  current_county_df <- get(county_data[i])
  current_county_df <- current_county_df[,c(1:7)]
  
  #assign `Town/County` column
  current_county_df$`Town/County` <- colnames(current_county_df)[1]
  #Assign year column
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(county_data[i])), "")))
  current_county_df$Year <- get_year
  
  #name columns, so they can be selected
  colnames(current_county_df) <- c("Naics Code", "Blank", "Category", "Number of Employers", "Annual Average Employment", 
                                   "Total Annual Wages", "Annual Average Wage", "Town/County", "Year")
  #select columns
  current_county_df <- current_county_df[,c("Category", "Number of Employers", "Annual Average Employment", 
                                            "Annual Average Wage", "Town/County", "Year")]
  #only extract rows that need processing
  blankFilter <- logical()
  for(i in 1:nrow(current_county_df)) {
    blankFilter <- append(blankFilter, all(is.na(current_county_df[i,])))
  }
  current_county_df <- current_county_df[!blankFilter & current_county_df$Category %in% c("County Total", "Total Government"),]
  #bind together
  all_counties <- rbind(all_counties, current_county_df)  
}

#Convert to long format
county_long <- gather(all_counties, Variable, Value, 2:4, factor_key=F)

#Round "Value" column
#makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
county_long$Value <- round_up(as.numeric(county_long$Value))

#Merge in FIPS
county_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json'
county_fips_dp <- datapkg_read(path = county_fips_dp_URL)
fips <- (county_fips_dp$data[[1]])

county_long_fips <- merge(county_long, fips, by.x = "Town/County", by.y = "County", all=T)

#remove "Connecticut"
county_long_fips <- county_long_fips[ grep("Connecticut", county_long_fips$`Town/County`, invert = TRUE) , ]

#Reorder columns
county_long_fips <- county_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(list=ls(pattern="20"), all_counties, county_long, current_county_df, fips)

##State Data#####################################################################################################################

#grabs state xls files
state_path <- file.path(path, "state")
state_xls <- dir(state_path, pattern = "CT")

all_state_years <- data.frame(stringsAsFactors = F)
for (i in 1:length(state_xls)) {
  current_file = read.xls(paste0(state_path, "/", state_xls[i]), sheet = 1, header = TRUE)  
  current_file <- current_file[,c(1:7)]
  colnames(current_file) <- c("Town/County", "Blank", "Category", "Number of Employers", 
                              "Annual Average Employment", "Total Annual Wages", "Annual Average Wage")
  #only keep populated columns (remove blank columns)
  current_file <- current_file[ , !(names(current_file) %in% drops)]
  #assign year
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(state_xls[i])), "")))
  get_year <- get_year+2000
  current_file$Year <- get_year
  current_file <- current_file[current_file$Category %in% c("Statewide Total", "Total Government"),]
  #Select columns
  current_file <- current_file[, c("Town/County", "Category", "Number of Employers", 
                                   "Annual Average Employment", "Annual Average Wage", "Year")] 
  
  current_file$`Town/County` <- "Connecticut"
  
  #Convert to long format
  current_file <- gather(current_file, Variable, Value, 3:5, factor_key=F)
  
  #Configure Value column
  current_file$Value <- gsub("\\$", "", current_file$Value)
  current_file$Value <- gsub(",", "", current_file$Value)

  #Round "Value" column
  #makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
  current_file$Value <- round_up(as.numeric(current_file$Value))
  #bind together
  all_state_years <- rbind(all_state_years, current_file) 
}

#Add FIPS (doing this manually, because only one value)
state_long_fips <- all_state_years
state_long_fips$FIPS <- 9

#Reorder columns
state_long_fips <- state_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(current_file, all_state_years)

###Merge to create completed data set
annual_average_employment_and_wages <- rbind(town_long_fips, county_long_fips, state_long_fips)

#Add Measure Type
annual_average_employment_and_wages$"Measure Type" <- NA
##fill in 'Measure Type' column based on criteria listed below
annual_average_employment_and_wages$"Measure Type"[which(annual_average_employment_and_wages$Variable %in% c("Number of Employers", 
                                                                                                             "Annual Average Employment"))] <- "Number"
annual_average_employment_and_wages$"Measure Type"[which(annual_average_employment_and_wages$Variable %in% c("Annual Average Wage"))] <- "US Dollars"

#Relabel Category column
annual_average_employment_and_wages$`Category` <- as.character(annual_average_employment_and_wages$`Category`)
annual_average_employment_and_wages$`Category`[which(annual_average_employment_and_wages$`Category` %in% c("Total - All Industries", "Statewide Total", "County Total"))] <- "All Industries"
annual_average_employment_and_wages$`Category`[which(annual_average_employment_and_wages$`Category` %in% c("Total Government"))] <- "Government"

#Reorder columns
annual_average_employment_and_wages <- annual_average_employment_and_wages %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Measure Type`, `Variable`, `Value`) %>% 
  arrange(`Town/County`, `Year`, `Category`)

# Write to File
write.table(
  annual_average_employment_and_wages,
  file.path(getwd(), "data", "annual_average_employment_and_wages.csv"),
  sep = ",",
  na = "-9999",
  row.names = F
)


