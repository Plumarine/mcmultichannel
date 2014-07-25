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
    newdata <- LoadCustomer(filepath, safeMode = TRUE)
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

#############################
## Create the report of customer with enrolment status
cusdt[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
cusdt[, Latest := LatestVerNo == CurrNo]
livecusdt <- cusdt[Latest == TRUE, ]

# Flag the status of enrolment
livecusdt[!is.na(ConfTelNo) & BioEnrollFlag == "Y" & BioEnrolStat == "UNLOCKED", EnrollStatus := "ENROLLED"]
livecusdt[is.na(ConfTelNo) & (is.na(BioEnrollFlag) | BioEnrollFlag == "N" ) & is.na(BioEnrolStat), EnrollStatus := "NOT"]
livecusdt[is.na(EnrollStatus), EnrollStatus := "EXCEPTION"]

livecusdt[, EnrollStatus := as.factor(EnrollStatus)]

# Merge the DAO and Branch info
EnrolmentReport <- merge(livecusdt, DAOBranch, by = "LocalDAO", all.x = TRUE)

# Select fields for final report
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
            "EnrollStatus",
            "Enroller",
            "Verifier",
            "UpdateTime",
            "Id")
EnrolmentReport <- EnrolmentReport[, flist2, with = FALSE]


#############################
## Output
save(cusdt, daodt, branchdt, file = "enrolMCMG.RData")
dput(param, "parammcmg")
write.csv(EnrolmentReport, outfile, na = "", row.names = FALSE)
# write.csv(pcusdt, poutfile, row.names = F)
