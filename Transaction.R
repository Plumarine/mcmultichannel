## This script will generate a report on monthly transaction
library(data.table)
library(plyr)
library(assertthat)
library(bigrquery)

#Parameters
param <- dget("param")
project <- param$bqproject
dataset <- param$bqdataset

mccompany <- "MCMG"

if (mccompany == "MCSE") {

  sql <- 'SELECT a.DisbYear, a.DisbMonth, a.LoanDisbursed, a.DisbAmount, b.ActiveLoan, b.LoanAmountPaid
  FROM
  (SELECT YEAR(LoanOpeningDate) DisbYear, MONTH(LoanOpeningDate) DisbMonth, COUNT(LoanLocalCode) LoanDisbursed, SUM(LoanDisbursedAmount) DisbAmount
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE (LoanOpeningDate=DateData)
  GROUP BY 1, 2) a
  JOIN
  (SELECT YEAR(DateData) DataYear, MONTH(DateData) DataMonth, COUNT(DISTINCT LoanLocalCode, 90000) ActiveLoan, SUM(LoanChargesPaid)+SUM(LoanInterestsPaid)+SUM(LoanCapitalPaid) LoanAmountPaid
  FROM [mcr_dw.materialized_fact_loans_mcse]
  WHERE (LoanStatus = "LIVING") OR (LoanStatus = "MATURED")
  GROUP BY 1, 2) b
  ON a.DisbYear=b.DataYear AND a.DisbMonth=b.DataMonth
  ORDER BY 1, 2;'

  ttfile <- "Data/Transaction/MCSN/TELLER_140606.txt"

  acfile <- "Data/Transaction/MCSN/ACCOUNT_140606.txt"
} else if (mccompany == "MCMG") {
  sql <- 'SELECT a.DisbYear, a.DisbMonth, a.LoanDisbursed, a.DisbAmount, b.ActiveLoan, b.LoanAmountPaid
  FROM
  (SELECT YEAR(LoanOpeningDate) DisbYear, MONTH(LoanOpeningDate) DisbMonth, COUNT(LoanLocalCode) LoanDisbursed, SUM(LoanDisbursedAmount) DisbAmount
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE (LoanOpeningDate=DateData)
  GROUP BY 1, 2) a
  JOIN
  (SELECT YEAR(DateData) DataYear, MONTH(DateData) DataMonth, COUNT(DISTINCT LoanLocalCode, 90000) ActiveLoan, SUM(LoanChargesPaid)+SUM(LoanInterestsPaid)+SUM(LoanCapitalPaid) LoanAmountPaid
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE (LoanStatus = "LIVING") OR (LoanStatus = "MATURED")
  GROUP BY 1, 2) b
  ON a.DisbYear=b.DataYear AND a.DisbMonth=b.DataMonth
  ORDER BY 1, 2;'
  
  # This is the SQL command to get if a customer has a loan in a month
  sql2 <- 'SELECT CustomerLocalID, CustomerStatus, YEAR(DateData) Year, MONTH(DateData) Month FROM [mcr_dw.materialized_fact_savings_mcmd]
WHERE DAY(DateData) == 1'
  
  # Load required data files TT and AC
  ttfile <- "Data/Transaction/MCMG/TELLER-20140610.TXT"
  
  acfile <- "Data/Transaction/MCMG/ACCOUNT-20140610.TXT"
}


# Select the fields to be loaded
slist <- c("@ID",
           "DR.CR.MARKER",
           "CUSTOMER.1",
           "ACCOUNT.1",
           "VALUE.DATE.1",
           "NET.AMOUNT",
           "DATE.TIME")

nlist <- c("Id",
           "DrCrMarker",
           "Customer1",
           "Account1",
           "ValueDate1",
           "NetAmount",
           "DateTime")
sclass <- c(NET.AMOUNT = "numeric")

if (mccompany == "MCSN") {
  slistnum <- c(1, 4, 6, 7, 12, 24, 74)
  
  ttdt1 <- fread(ttfile, header = T, sep = "^", na.strings = "", stringsAsFactors = F, 
                 select = slist, integer64 = "numeric",  nrows = 1117385)
  
  ttdt2 <- fread(ttfile, header = F, sep = "^", na.strings = "", stringsAsFactors = F, 
                 select = slistnum, integer64 = "numeric",  skip = 1117388)
  
  setnames(ttdt2, colnames(ttdt2), nlist)
  setnames(ttdt1, slist, nlist)
  ttdt <- rbind(ttdt1, ttdt2)
  
} else {
  ttdt <- fread(ttfile, header = T, sep = "^", na.strings = "", stringsAsFactors = F, 
               select = slist, integer64 = "numeric")
  setnames(ttdt, slist, nlist)
}


## Read account type

aclist <- c("@ID", "CATEGORY", "CURR.NO")
acnlist <- c("Account", "Category", "CurrNo")
acdt <- fread(acfile, header = T, sep = "^", na.strings = "", stringsAsFactors = F, 
              select = aclist)
setnames(acdt, aclist, acnlist)

acdt[, Account := gsub(";.*", "", Account)]

acdt[, LatestVerNo := max(CurrNo), by = Account]
acdt[, Latest := LatestVerNo == CurrNo]
aclive <- acdt[Latest == TRUE, list(Account, Category)]
aclive <- unique(aclive, by = "Account")

## Join the account category to transaction
setnames(ttdt, "Account1", "Account")
ttdt <- merge(ttdt, aclive, by = "Account", all.x = TRUE)
setnames(ttdt, "Account", "Acocunt1")

# Transform the data
ttdt <- ttdt[!is.na(Customer1), ]
ttdt[, ValueDate1 := as.Date(as.character(ValueDate1), "%Y%m%d")]
ttdt[, YearMonth := format(ValueDate1, "%Y-%m")]

#   ttdt[, CashTransCount := nrow(Id), by = YearMonth]
#   ttdt[, CashTransAmount := sum(NetAmount), by = YearMonth]
#   ttdt[, CashInCount: = sum(DrCrMarker == "CREDIT"), by = YearMonth]
#   ttdt[, CashInAmount: = sum(DrCrMarker == "CREDIT"), by = YearMonth]
#   ttdt[, CashOutCount: = sum(DrCrMarker == "DEBIT"), by = YearMonth]

# Calculate total Teller transaction
ttresult <- ddply(ttdt, .(YearMonth), summarize, 
                  CashAmount = sum(NetAmount),
                  CashCount = length(YearMonth), 
                  CashCustomerCount = length(unique(Customer1)))
# Calculate the CashIn Transaction
temp <- ddply(ttdt[DrCrMarker == "CREDIT", ], .(YearMonth), summarize, 
              CashInAmount = sum(NetAmount),
              CashInCount = length(YearMonth))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Calculate the CashOut Transaction
temp <- ddply(ttdt[DrCrMarker == "DEBIT"], .(YearMonth), summarize, 
              CashOutAmount = sum(NetAmount),
              CashOutCount = length(YearMonth))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Calculate the CashIn Transaction on Savings Account (none current account)
# MCSN 7313 Current Account
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category != 7313], .(YearMonth), summarize,
              SavingsCashInAmount = sum(NetAmount), 
              SavingsCashInCount = length(YearMonth),
              SavingsCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Calculate the CashOut Transaction on Savings Account (none current account)
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category != 7313], .(YearMonth), summarize,
              SavingsCashOutAmount = sum(NetAmount), 
              SavingsCashOutCount = length(YearMonth),
              SavingsCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Calculate the CashIn Transaction on Current Account
# MCSN 7313 Current Account
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category == 7313], .(YearMonth), summarize,
              CurrentCashInAmount = sum(NetAmount), 
              CurrentCashInCount = length(YearMonth),
              CurrentCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Calculate the CashOut Transaction on Current Account
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category == 7313], .(YearMonth), summarize,
              CurrentCashOutAmount = sum(NetAmount), 
              CurrentCashOutCount = length(YearMonth),
              CurrentCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

## Load loan file
loandf <- query_exec(project, dataset, sql, billing = project)
loandt <- data.table(loandf)
flist <- colnames(loandt)
nlist <- gsub("[a-z]_", "", flist)
setnames(loandt, flist, nlist)

loandt[, YearMonth := 
         paste(DisbYear, 
               paste0(substr(rep("0", length(loandt$DisbMonth)), 1, 2 - nchar(DisbMonth)), DisbMonth), 
               sep = "-")]
 
loandt <- loandt[, list(YearMonth, LoanDisbursed, DisbAmount, ActiveLoan, LoanAmountPaid)]


## ReadJoinLoanData <- function(tt, loan)

fresult <- merge(ttresult, loandt, by = "YearMonth")
