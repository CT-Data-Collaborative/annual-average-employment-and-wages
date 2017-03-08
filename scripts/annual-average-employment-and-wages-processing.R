library(dplyr)
library(datapkg)
library(readxl)
##################################################################
#
# Processing Script for Annual-Average-Employment-and-Wages
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

##Town Data######################################################################################################################

#grabs town xls file
town_path <- file.path(path, "town")
town_xls <- dir(town_path, pattern = "Town")
drops <- c("")
round_up = function(x) trunc(x+0.5)


for (i in 1:length(town_xls)) {
  current_file <- (read_excel(paste0(town_path, "/", town_xls[i]), sheet=1, skip=0))
  current_file <- current_file[,c(1:7)]
  colnames(current_file) <- c("Town/County", "NAICS Code", "Category", "Number of Employers", 
                              "Annual Average Employment", "Total Annual Wages", "Annual Average Wage")
  
  #only keep populated columns (remove blank columns)
  current_file <- current_file[ , !(names(current_file) %in% drops)]
  #only keep populated rows (remove blank rows)
  current_file <- current_file[rowSums(is.na(current_file)) != ncol(current_file),]
  get_year <- unique(as.numeric(unlist(gsub("[^0-9]", "", unlist(town_xls[i])), "")))
  get_year <- substr(get_year, 1, 4)
  current_file$Year <- get_year
  #now, only take rows we need to processing, 
  #based on whenever the "Category" column is equal to "Total - All Industries" or "Total Government"
  blankFilter <- logical()
  for(i in 1:nrow(current_file)) {
    blankFilter <- append(blankFilter, all(is.na(current_file[i,])))
  }
  current_file <- current_file[!blankFilter & current_file$Category %in% c("Total - All Industries", 
                                                                           "Total Government"),]
  
  #populate blank town lines with corresponding town
  currentTown = current_file[1,1]
  for (i in 1:nrow(current_file)) {
    if(is.na(current_file[i,1])) {
      current_file[i,1] <- currentTown
    } else {
      currentTown <- current_file[i,1]
    }
  }

  #Select columns
  current_file <- current_file[, c("Town/County", "Category", "Number of Employers", "Annual Average Employment", "Annual Average Wage", "Year")] 
  
  #Convert to long format
  cols_to_stack <- c("Number of Employers", 
                     "Annual Average Employment", 
                     "Annual Average Wage")
  
  long_row_count = nrow(current_file) * length(cols_to_stack)
  
  current_file <- reshape(current_file, 
                       varying = cols_to_stack, 
                       v.names = "Value", 
                       timevar = "Variable", 
                       times = cols_to_stack, 
                       new.row.names = 1:long_row_count,
                       direction = "long"
  )
  
  current_file$id <- NULL
  
  #Round "Value" column
  #makes sure 0.5 gets rounded as 1.0, transforms "*" to "NA"
  current_file$Value <- round_up(as.numeric(current_file$Value))
  
  assign(paste0("town_", get_year), current_file)
}






