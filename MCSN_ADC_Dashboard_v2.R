
###################Start#########################
## Load and prepare data
setwd("/home/szhang/MultiChannel/")
source("libv2.R")
source("../Library/lib.R")

# Input and parameters
filedir <- "Data/MCSN/Daily/"
# All the data files will be extracted to this folder by ETL process
filelist <- list.files(filedir)

param <- dget("Data/MCSN/parammcsn")


# Output files
outfile <- "Data/MCSN/Output/MCSN_ADC_Enrollment_v2.csv"
txnoutfile <- "Data/MCSN/Output/MCSN_ADC_Transaction.csv"

## Initialize the data
cusdt <- data.table()
daodt <- data.table()
branchdt <- data.table()
ftdt <- data.table()
newftdt <- data.table()
# Read tables
load("Data/MCSN/ADC_MCSN_v2.RData")

newfile <- 0

for (file in filelist) {
  filepath <- paste(filedir, file, sep = "")
  
  if (grepl("CUSTOMER.+", file) & !(file %in% param$cuslist)) {
    # Process Customer data
    newdata <- LoadCustomer(filepath, safeMode = TRUE)
    cusdt <- MergeTable(cusdt, newdata, mkey = "Id")
    
    param$cuslist <- c(param$cuslist, file)
    
    newfile <- newfile + 1
  }
  if (grepl("DEPT.ACCT.OFFICER.+", file) & !(file %in% param$daolist)) {
    # Process DAO data
    newdata <- LoadDAO(filepath)
    daodt <- MergeTable(daodt, newdata, mkey = "Id")
    param$daolist <- c(param$daolist, file)
    
    newfile <- newfile + 1
  }
  if (grepl("MCR.BRANCH.TABLE.+", file) & !(file %in% param$branchlist)) {
    # Process Branch data
    newdata <- LoadBranch(filepath)
    branchdt <- MergeTable(branchdt, newdata, mkey = "Id")
    
    param$branchlist <- c(param$branchlist, file)
    
    newfile <- newfile + 1
  }
  if (grepl("FUNDS.TRANSFER.+", file) & !(file %in% param$ftlist)) {
    # Process Branch data
    newdata <- LoadFT(filepath)
    newftdt <- MergeTable(newftdt, newdata, mkey = "Id")
    
    param$ftlist <- c(param$ftlist, file)
    
    newfile <- newfile + 1
  }
}

############## Now Generate Transaction report (FT) ###############
# Get branch information
if (nrow(newftdt) > 0){
  livebranchdt <- LiveBranch(branchdt)
  setnames(livebranchdt, c("BranchLocalId", "BranchDescription"), c("AgentBranchId", "AgentBranch"))
  newftdt <- merge(newftdt, livebranchdt, by = "AgentBranchId", all.x = TRUE)
  setnames(livebranchdt, c("AgentBranchId", "AgentBranch"), c("BranchLocalId", "BranchDescription"))
  
  # Merge daily data with historical data
  ftdt <- MergeTable(ftdt, newftdt, mkey = "Id")
  
  # Select fields for final report
  flistft <- c("Id",
               "TransactionType",
               "DebitAcctNo",
               "DebitCurrency",
               "DebitAmount",
               "DebitValueDate",
               "CreditAcctNo",
               "AcctWorkBal",
               "InitiatorId",
               "MessageTypes",
               "CardNumber",
               "ProcCode",
               "StanId",
               "RetrRefNo",
               "TerminalId",
               "TerminalLoc",
               "AgentAcctNo",
               "AgentComAmt",
               "AgentBranchId",
               "AgentBranch",
               "Type",
               "UpdateTime",
               "UpdateDate",
               "CustomerLocalId")
  
  TxnReport <- ftdt[, flistft, with = FALSE]
  
  
  #Output
  write.csv(TxnReport, txnoutfile, na = "", row.names = FALSE)
  
  bqtxnreport <- "mcsn_adc_txnreport"
  # Upload
  UploadBigQuery(TxnReport, bqtxnreport, "adc_txn", bqdaproject, overwrite = TRUE)
  
  #Incremental upload
  UploadBigQuery(newftdt, "mcsn_adc_ft", "adc_txn", bqdaproject)
}

# Save database
save(cusdt, daodt, branchdt, ftdt, file = "Data/MCSN/ADC_MCSN_v2.RData")
dput(param, "Data/MCSN/parammcsn")



################## (Hisotrical) Data ###################################
# generate historical data from at least 140714
# when this script is running daily, the dates sequence should be only yesterday

#dates <- seq(as.Date("2014-9-19"), Sys.Date()-1, "days")
dates <- seq(as.Date("2014-9-19"), as.Date("2014-9-20"), "days")
for (i in 1:length(dates)) {
  date <- dates[i]
  message(paste0("Generating data for: ", date))
# Loop on dates

  # Create a DAOBranch table for later use
  DAOBranch <- JoinDAOBranch(daodt, branchdt, date)
  
  ## Create the report of customer with enrolment status
  cusdtDate <- cusdt[UpdateDate <= date, ]
  cusdtDate[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
  livecusdt <- cusdtDate[LatestVerNo == CurrNo, ]
  
  # Flag the status of enrolment
  livecusdt[!is.na(ConfTelNo) & BioEnrollFlag == "Y" & BioEnrolStat == "UNLOCKED", EnrollStatus := "ENROLLED"]
  livecusdt[is.na(ConfTelNo) & (is.na(BioEnrollFlag) | BioEnrollFlag == "N" ) & is.na(BioEnrolStat), EnrollStatus := "NOT"]
  livecusdt[is.na(EnrollStatus), EnrollStatus := "EXCEPTION"]
  
  livecusdt[, EnrollStatus := as.factor(EnrollStatus)]
  
  # Merge the DAO and Branch info
  EnrolmentReport <- merge(livecusdt, DAOBranch, by = "LocalDAO", all.x = TRUE)
  
  # Get and Merge Inactive Customer info
  cusactsql <- paste0('SELECT a.CustomerLocalID CustomerLocalId, IF(FIRST(InactivCustomer)="N", "ACTIVE", "INACTIVE") ActiveStatus
                      FROM
                      (SELECT CustomerLocalID, InactivCustomer, MAX(DateData) MaxDate
                      FROM [mcr_dw.materialized_fact_savings_mcse]
                      WHERE DateData <= "', date, ' 00:00"', '
                      GROUP BY 1, 2
                      ORDER BY 1, 3 DESC) a
                      GROUP BY 1')
  
  
  cusactdt <- LoadBigQueryDW(cusactsql)
  EnrolmentReport <- merge(EnrolmentReport, cusactdt, by = "CustomerLocalId", all.x = TRUE)
  EnrolmentReport[is.na(ActiveStatus), ActiveStatus := "INACTIVE"]
  
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
              "Inputter",
              "Authoriser",
              "UpdateTime",
              "Id",
              "ActiveStatus")
  EnrolmentReport <- EnrolmentReport[, flist2, with = FALSE]
  
  
  ## Generate data for ADC adoption
  require(plyr)
  ftdtDate <- ftdt[UpdateDate <= date, ]
  
  adoptdt1 <- data.table(ddply(ftdtDate, .(CustomerLocalId), summarize, 
                               ADCTxnNumber = length(Id),
                               ADCTxnAmount = sum(DebitAmount),
                               ADCTxnWeekStreak = LatestStreakWeeks(UpdateDate),
                               ADCTxnFirstDate = min(UpdateDate),
                               ADCTxnLastDate = max(UpdateDate)))
  adoptdt2 <- data.table(ddply(ftdtDate[LocalChargeAmt > 0, ], .(CustomerLocalId), summarize,
                               ADCPaidTxnNumber = length(Id),
                               ADCPaidTxnAmount = sum(DebitAmount),
                               ADCTxnFeePaid = sum(LocalChargeAmt)))
  if (nrow(adoptdt1) == 0) {
    EnrolmentReport[, `:=`(ADCTxnNumber = as.integer(0),
                           ADCTxnAmount = as.integer(0),
                           ADCTxnWeekStreak = as.integer(0),
                           ADCTxnFirstDate = as.Date(NA),
                           ADCTxnLastDate =as.Date(NA),
                           ADCPaidTxnNumber = as.integer(0), 
                           ADCPaidTxnAmount = as.integer(0), 
                           ADCTxnFeePaid = as.integer(0))]
    
  } else if (nrow(adoptdt2) == 0) {
    adoptdt <- adoptdt1
    EnrolmentReport <- merge(EnrolmentReport, adoptdt, by = "CustomerLocalId", all.x = TRUE)
    
    EnrolmentReport[, `:=`(ADCPaidTxnNumber = as.integer(0), 
                           ADCPaidTxnAmount = as.integer(0), 
                           ADCTxnFeePaid = as.integer(0))]
    EnrolmentReport[is.na(ADCTxnNumber), `:=`(ADCTxnNumber = as.integer(0),
                                              ADCTxnAmount = as.integer(0),
                                              ADCTxnWeekStreak = as.integer(0))]
  } else {
    adoptdt <- merge(adoptdt1, adoptdt2, by = "CustomerLocalId", all.x = TRUE)
    EnrolmentReport <- merge(EnrolmentReport, adoptdt, by = "CustomerLocalId", all.x = TRUE)
    
    EnrolmentReport[is.na(ADCTxnNumber), `:=`(ADCTxnNumber = as.integer(0),
                                              ADCTxnAmount = as.integer(0),
                                              ADCTxnWeekStreak = as.integer(0))]
    EnrolmentReport[is.na(ADCPaidTxnNumber), `:=`(ADCPaidTxnNumber = as.integer(0), 
                                                  ADCPaidTxnAmount = as.integer(0), 
                                                  ADCTxnFeePaid = as.integer(0))]
  }
  
  EnrolmentReport[, DateData := date]
  
  ###
  # Upload daily data to BigQuery
  bqtable <- paste0("mcsn_adc_cus_adopt", format(date, "%Y%m%d"))
  # Upload
  UploadBigQuery(EnrolmentReport, bqtable, "adc_adoption", bqdaproject, overwrite = TRUE)
  
}

#Meantime upload the last avaiable data to a single table without date
bqtable <- "mcsn_adc_lastest_cus_adopt"
UploadBigQuery(EnrolmentReport, bqtable, "adc_adoption", bqdaproject, overwrite = TRUE)





