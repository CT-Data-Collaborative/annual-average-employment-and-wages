library(dplyr)
library(datapkg)
library(readxl)

##################################################################
#
# Processing Script for raw_xls_reshape
# Created by Jenna Daly
# On 03/08/2017
#
##################################################################

#Updating processing steps, recreating concatenated file for 2004-2015

#Setup environment
sub_folders <- list.files()
data_location <- grep("raw", sub_folders, value=T)
path <- (paste0(getwd(), "/", data_location))

###Create subsets

##Town######################################################################################################################

#grabs town xls file
town_xls <- dir(path, pattern = "Town")

get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(town_xls)), "")))
get_year <- substr(get_year, 1, 4)

#read in entire xls file
town_file <- (read_excel(paste0(path, "/", town_xls), sheet=1, skip=))

#only keep populated columns (remove blank columns)
drops <- c("")
town_file <- town_file[ , !(names(town_file) %in% drops)]

#only keep populated rows (remove blank rows)
town_file2 <- town_file[rowSums(is.na(town_file)) != ncol(town_file),]

#now, only take rows we need to processing, 
#based on whenever the "Industry" column is equal to "Total - All Industries" or "Total Government"
blankFilter <- logical()
for(i in 1:nrow(town_file2)) {
  blankFilter <- append(blankFilter, all(is.na(town_file2[i,])))
}
town_file3 <- town_file2[!blankFilter & town_file2$Industry %in% c("Total - All Industries", "Total Government"),]

#populate blank town lines with corresponding town
currentTown = town_file3[1,1]
for (i in 1:nrow(town_file3)) {
  if(is.na(town_file3[i,1])) {
    town_file3[i,1] <- currentTown
  } else {
    currentTown <- town_file3[i,1]
  }
}

#cleanup
remove(blankFilter, currentTown, i, town_file, town_file2, drops)

#Select and reorder columns
town_file3 <- town_file3 %>% 
  select(`Town`, `Industry`, `Units`, `Annual Average Employment`, `Annual Average Wage`)

#Rename columns and add Year
town_file3$Year <- get_year

colnames(town_file3) <- c("Town/County", "Category", "Number of Employers", "Annual Average Employment", "Annual Average Wage", "Year")

#Convert to long format
cols_to_stack <- c("Number of Employers", 
                   "Annual Average Employment", 
                   "Annual Average Wage")

long_row_count = nrow(town_file3) * length(cols_to_stack)

town_long <- reshape(town_file3, 
                               varying = cols_to_stack, 
                               v.names = "Value", 
                               timevar = "Variable", 
                               times = cols_to_stack, 
                               new.row.names = 1:long_row_count,
                               direction = "long"
)

town_long$id <- NULL

#Round "Value" column
#makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
round_up = function(x) trunc(x+0.5)
town_long$Value <- round_up(as.numeric(town_long$Value))

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])

town_long_fips <- merge(town_long, fips, by.x = "Town/County", by.y = "Town",all=T)

#remove "Connecticut"
town_long_fips <- town_long_fips[!town_long_fips$`Town/County` == "Connecticut",]

#Reorder columns
town_long_fips <- town_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)


#cleanup
rm(fips, town_file3, town_long, long_row_count, town_fips_dp, town_fips_dp_URL)
##County####################################################################################################################

#grabs county xls file
county_xls <- dir(path, pattern = "CNTY")

#read in entire xls file (all sheets)
read_excel_allsheets <- function(filename) {
  sheets <- readxl::excel_sheets(filename)
  x <-    lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  names(x) <- sheets
  x
}

#Create a list of all sheet data
mysheets <- read_excel_allsheets(paste0(path, "/", county_xls))

#Cycle through all sheets, extract sheet name, and assign each sheet (county) its own data frame
for (i in 1:length(mysheets)) {
  current_county_name <- colnames(mysheets[[i]])[1]
  current_county_file <- mysheets[[i]]    
  assign(paste0(current_county_name), current_county_file)
}

dfs <- ls()[sapply(mget(ls(), .GlobalEnv), is.data.frame)]
county_data <- grep("County$", dfs, value=T)

all_counties <- data.frame(stringsAsFactors = F)
#Process each county file
for (i in 1:length(county_data)) {
  #grab first county file
  current_county_df <- get(county_data[i])
  #assign `Town/County` column
  current_county_df$`Town/County` <- colnames(current_county_df)[1]
  #name columns, so they can be selected
  colnames(current_county_df) <- c("Naics Code", "Category", "Number of Employers", "Annual Average Employment", "Total Annual Wages", "Annual Average Wage", "Average Weekly Wage", "Town/County")
  #select columns
  current_county_df <- current_county_df[,c("Category", "Number of Employers", "Annual Average Employment", "Annual Average Wage", "Town/County")]
  #only extract rows that need processing
  blankFilter <- logical()
  for(i in 1:nrow(current_county_df)) {
    blankFilter <- append(blankFilter, all(is.na(current_county_df[i,])))
  }
  current_county_df <- current_county_df[!blankFilter & current_county_df$Category %in% c("County Total", "Total Government"),]
  #Rename all industries category
  current_county_df$`Category`[which(current_county_df$`Category` %in% c("County Total"))] <- "Total - All Industries"
  #bind together
  all_counties <- rbind(all_counties, current_county_df)  
}

#add Year
all_counties$Year <- get_year

#Convert to long format
# cols_to_stack <- c("Number of Employers", 
#                    "Annual Average Employment", 
#                    "Annual Average Wage")

long_row_count = nrow(all_counties) * length(cols_to_stack)

county_long <- reshape(all_counties, 
                     varying = cols_to_stack, 
                     v.names = "Value", 
                     timevar = "Variable", 
                     times = cols_to_stack, 
                     new.row.names = 1:long_row_count,
                     direction = "long"
)

county_long$id <- NULL

#Round "Value" column
#makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
county_long$Value <- round_up(as.numeric(county_long$Value))

#Merge in FIPS
county_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json'
county_fips_dp <- datapkg_read(path = county_fips_dp_URL)
fips <- (county_fips_dp$data[[1]])

county_long_fips <- merge(county_long, fips, by.x = "Town/County", by.y = "County", all=T)

#remove "Connecticut"
county_long_fips <- county_long_fips[!county_long_fips$`Town/County` == "Connecticut",]

#Reorder columns
county_long_fips <- county_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(county_long, current_county_df, `Fairfield County`, fips, `Hartford County`, `Litchfield County`, `Middlesex County`, 
   `New Haven County`, `New London County`, `Tolland County`, `Windham County`, blankFilter, county_data, county_fips_dp, 
   county_fips_dp_URL, current_county_name, dfs, i, long_row_count, mysheets, all_counties, current_county_file)

##State#####################################################################################################################

#grabs state xls file
state_xls <- dir(path, pattern = "CT")

#read in entire xls file
state_file <- (read_excel(paste0(path, "/", state_xls), sheet=1, skip=0))

#assign `Town/County` column
state_file$`Town/County` <- "Connecticut"

#name columns, so they can be selected
colnames(state_file) <- c("Naics Code", "Category", "Number of Employers", "Annual Average Employment", "Total Annual Wages", "Annual Average Wage", "Average Weekly Wage", "Town/County")

#select columns
state_file <- state_file[,c("Category", "Number of Employers", "Annual Average Employment", "Annual Average Wage", "Town/County")]

#only extract rows that need processing
blankFilter <- logical()
for(i in 1:nrow(state_file)) {
  blankFilter <- append(blankFilter, all(is.na(state_file[i,])))
}
state_file <- state_file[!blankFilter & state_file$Category %in% c("Statewide Total", "Total Government"),]

#Rename all industries category
state_file$`Category`[which(state_file$`Category` %in% c("Statewide Total"))] <- "Total - All Industries"

#add Year
state_file$Year <- get_year

long_row_count = nrow(state_file) * length(cols_to_stack)

state_long <- reshape(state_file, 
                       varying = cols_to_stack, 
                       v.names = "Value", 
                       timevar = "Variable", 
                       times = cols_to_stack, 
                       new.row.names = 1:long_row_count,
                       direction = "long"
)

state_long$id <- NULL

#Round "Value" column
#makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
state_long$Value <- round_up(as.numeric(state_long$Value))


#Add FIPS (doing this manually, because only one value)
state_long_fips <- state_long
state_long_fips$FIPS <- 9

#Reorder columns
state_long_fips <- state_long_fips %>% 
  select(`Town/County`, `FIPS`, `Year`, `Category`, `Variable`, `Value`)

#Cleanup
rm(state_file, state_long, blankFilter, i, long_row_count)

###Merge to create completed 2015 data set
annual_employment_and_wages_2015 <- rbind(town_long_fips, county_long_fips, state_long_fips)

#Add Measure Type
annual_employment_and_wages_2015$"Measure Type" <- NA
##fill in 'Measure Type' column based on criteria listed below
annual_employment_and_wages_2015$"Measure Type"[which(annual_employment_and_wages_2015$Variable %in% c("Number of Employers", 
                                                                                                       "Annual Average Employment", 
                                                                                                       "Annual Average Wage"))] <- "Number"

###Merge 2015 with rest of years

###Save 2004-2015 file





