## Load and prepare data
setwd("/home/szhang/MultiChannel/")
source("lib.R")
# Input and parameters
daterange <- seq(as.Date("2014-07-21"), as.Date(ydate()[2]), by = "day")
filedir <- "Data/EnrolmentDaily/MCMG/"
filelist <- list.files(filedir)
#pcusfile <- "Data/mcmgbiopriority.csv"
param <- dget("parammcmg")

# Output files
outfile <- "Output/MCMGEnrollmentData.csv"
#poutfile <- paste0("Output/MCMG PriorityCustomer", ydate()[1], ".csv")
exoutfile <- paste0("Output/MCMG ExceptionalCustomer", ydate()[1], ".csv")

## Initialize the data
cusdt <- data.table()
daodt <- data.table()
branchdt <- data.table()

# Read tables
load("enrolMCMG.RData")

for (file in filelist) {
  filepath <- paste(filedir, file, sep = "")
  
  if (grepl("CUSTOMER.+", file) & !(file %in% param$cuslist)) {
    # Process Customer data
    newdata <- LoadCustomer(filepath)
    cusdt <- MergeTable(cusdt, newdata, mkey = "Id")
    param$cuslist <- c(param$cuslist, file)
  }
  if (grepl("DEPT.ACCT.OFFICER.+", file) & !(file %in% param$daolist)) {
    # Process DAO data
    newdata <- LoadDAO(filepath)
    daodt <- MergeTable(daodt, newdata, mkey = "Id")
    
    param$daolist <- c(param$daolist, file)
  }
  if (grepl("MCR.BRANCH.TABLE.+", file) & !(file %in% param$branchlist)) {
    # Process Branch data
    newdata <- LoadBranch(filepath)
    branchdt <- MergeTable(branchdt, newdata, mkey = "Id")
    
    param$branchlist <- c(param$branchlist, file)
  }
  
}

# Create a DAOBranch table for later use
DAOBranch <- JoinDAOBranch(daodt, branchdt)

##################################
## Create the Enrolment Report
agrdf <- data.frame(Date = as.Date(character()),
                    LocalDAO = character(),
                    TotalCustomer = integer(),
                    NewCustomer = integer(),
                    TotalEnrolled = integer(),
                    NewEnrolled = integer(),
                    stringsAsFactors = FALSE)

i <- 1

for (date in daterange) {
  date <- as.Date(date, origin = "1970-01-01")
  # no duplicates in customer
  # Note: this only means when the customer join MC, which DAO it belongs
  # Customer's aquisition
  cusdtsub1 <- cusdt[OpeningDate <= date & CurrNo == 1, ]
  cusdtsub1[, TotalCustomer := length(CustomerLocalId), by = AccountOfficer]
  
  # There is an issue here, when calculate total enrolled customer, if the customer's DAO is updated
  # It it can be calculated multiple times in different DAOs, this has been solved!
  
  # Another issue might be 
  cusdtsub2 <- cusdt[BioEnrollDate <= date & !is.na(ConfTelNo) & BioEnrollFlag == "Y" 
                     & BioEnrolStat == "UNLOCKED", ]
  if (nrow(cusdtsub2) > 0) {
    cusdtsub2[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
    cusdtsub2[, Latest := LatestVerNo == CurrNo]
    cusdtsub2 <- cusdtsub2[Latest == TRUE, ]
    cusdtsub2[, TotalEnrolled := length(unique(CustomerLocalId)), by = AccountOfficer]
  }
  
  # Initialize the DAOs for that day
  dao1 <- unique(cusdtsub1$AccountOfficer)
  dao2 <- unique(cusdtsub2$AccountOfficer)
  daos <- unique(c(dao1, dao2))
  for (dao in daos) {
    # Build each column in this table
    # DAO, Date
    agrdf[i, "Date"] <- date
    agrdf[i, "LocalDAO"] <- dao
    
    # Total Customer and new customer
    if (dao %in% dao1) {
      agrdf[i, "TotalCustomer"] <- cusdtsub1[AccountOfficer == dao, "TotalCustomer", with = F][[1, 1]]
      agrdf[i, "NewCustomer"] <- length(cusdtsub1[OpeningDate == date & AccountOfficer == dao, ]$CustomerLocalId)
    } else {
      agrdf[i, "TotalCustomer"] <- 0
      agrdf[i, "NewCustomer"] <- 0
    }
    
    # Total Enrolled customer and New Enrolled Customer
    if (dao %in% dao2) {
      agrdf[i, "TotalEnrolled"] <- cusdtsub2[AccountOfficer == dao, "TotalEnrolled", with = F][[1, 1]]  
      agrdf[i, "NewEnrolled"] <- length(unique(cusdtsub2[BioEnrollDate == date & AccountOfficer == dao, ]$CustomerLocalId))
    } else {
      agrdf[i, "TotalEnrolled"] <- 0
      agrdf[i, "NewEnrolled"] <- 0
    }
    
    i <- i + 1
  }
}

# Join descriptive fields DAO description and Branch
EnrolmentReport <- merge(agrdf, DAOBranch, by = "LocalDAO", all.x = TRUE)

#############################
# ## Create the report with priority list
# cusdt[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
# cusdt[, Latest := LatestVerNo == CurrNo]
# livecusdt <- cusdt[Latest == TRUE, list(CustomerLocalId, BioEnrolStat, ConfTelNo, BioEnrollFlag, BioEnrollDate, UpdateTime)]
# 
# pflist <- c("CORRESPONDANT", "CLIENT NR")
# npflist <- c("Correspondant", "CustomerLocalId")
# pcusdt <- fread(pcusfile, header = TRUE, na.strings = "", stringsAsFactors = F, 
#                 colClasses = c("CLIENT NR" = "character"))
# setnames(pcusdt, pflist, npflist)
# pcusdt <- merge(pcusdt, livecusdt, by = "CustomerLocalId", all.x = TRUE)
# 
# # Flag the status of enrolment
# pcusdt[!is.na(ConfTelNo) & BioEnrollFlag == "Y" & BioEnrolStat == "UNLOCKED", EnrollStatus := "ENROLLED"]
# pcusdt[is.na(ConfTelNo) & (is.na(BioEnrollFlag) | BioEnrollFlag == "N" ) & is.na(BioEnrolStat), 
#        EnrollStatus := "NOT"]
# pcusdt[is.na(EnrollStatus), EnrollStatus := "EXCEPTION"]


########################################
## Create the exception list of half enrolled customer
cusinfodt <- data.table()

# Load the data file
for (file in filelist) {
  filepath <- paste(filedir, file, sep = "")
  
  if (grepl("CUSTOMER.+", file)) {
    # Process Customer data
    newdata <- LoadCustomerInfo(filepath)
    cusinfodt <- MergeTable(cusinfodt, newdata, mkey = "Id")
  }
}

# Filter the half enroled customers
cusinfodt[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
cusinfodt[, Latest := LatestVerNo == CurrNo]
cusLive <- cusinfodt[Latest == TRUE, ]

cusHalfEnrol <- cusLive[!is.na(BioEnrollDate) | !is.na(ConfTelNo) | BioEnrollFlag == "Y" 
                        | !is.na(BioEnrolStat), ]
cusHalfEnrol <- cusHalfEnrol[!(!is.na(ConfTelNo) & BioEnrollFlag == "Y" & BioEnrolStat == "UNLOCKED")]

cusHalfEnrol <- merge(cusHalfEnrol, DAOBranch, by = "LocalDAO", all.x = TRUE)

flist2 <- c("CustomerLocalId", 
            "OpeningDate", 
            "FirstName",
            "LastName",
            "TelMobile",
            "LocalDAO", 
            "DAODescription",
            "BranchLocalId",
            "BranchDescription",
            "BioEnrolStat", 
            "ConfTelNo", 
            "BioEnrollFlag", 
            "BioEnrollDate", 
            "Enroller",
            "Verifier",
            "UpdateTime",
            "Id")
cusHalfEnrol <- cusHalfEnrol[, flist2, with = F]


#############################
## Output
save(cusdt, daodt, branchdt, file = "enrolMCMG.RData")
dput(param, "parammcmg")
write.csv(EnrolmentReport, outfile, row.names = F)
write.csv(cusHalfEnrol, exoutfile, row.names = F)
# write.csv(pcusdt, poutfile, row.names = F)
