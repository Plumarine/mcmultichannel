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

  sql <- 'SELECT disb.YearMonth YearMonth, disb.DisbCustomerCount DisbCustomerCount, disb.DisbLoanCount DisbLoanCount, disb.DisbAmount DisbAmount, loan.active.WithLoanCustomerCount WithLoanCustomerCount, loan.active.ActiveLoanCount ActiveLoanCount, loan.payment.PaymentCustomerCount PaymentCustomerCount, loan.payment.PaymentDayCount PaymentDayCount, loan.payment.PaymentAmount PaymentAmount
FROM
(SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) DisbCustomerCount, COUNT(LoanLocalCode) DisbLoanCount, SUM(LoanDisbursedAmount) DisbAmount
  FROM [mcr_dw.materialized_fact_loans_mcse]
  WHERE (LoanOpeningDate=DateData)
  GROUP BY 1) disb
  JOIN
  (SELECT *
  FROM
  (SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) WithLoanCustomerCount, COUNT(DISTINCT LoanLocalCode, 90000) ActiveLoanCount
  FROM [mcr_dw.materialized_fact_loans_mcse]
  WHERE (LoanStatus = "LIVING") OR (LoanStatus = "MATURED")
  GROUP BY 1) active
  JOIN
  (SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) PaymentCustomerCount, COUNT(DateData) PaymentDayCount, SUM(LoanChargesPaid)+SUM(LoanInterestsPaid)+SUM(LoanCapitalPaid)+SUM(LoanPenalitySpreadPaid)+SUM(LoanPenalityPaid) PaymentAmount
  FROM [mcr_dw.materialized_fact_loans_mcse]
  WHERE LoanChargesPaid + LoanInterestsPaid + LoanCapitalPaid + LoanPenalitySpreadPaid + LoanPenalityPaid> 0
  GROUP BY 1) payment
  ON active.YearMonth = payment.YearMonth
  ) loan
  ON disb.YearMonth=loan.active.YearMonth
  ORDER BY 1;'
  
  sql2 <- 'SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, CustomerLocalID, CustomerStatus, COUNT(REGEXP_EXTRACT(SavingType, "(CURRENT)")) Current, COUNT(REGEXP_EXTRACT(SavingType, "(SAVINGS)")) Savings, COUNT(DateData)
FROM [mcr_dw.materialized_fact_savings_mcse]
WHERE DAY(DateData) = 1
GROUP EACH BY 1, 2, 3
ORDER BY 1, 2, 3;'

  ttfile <- "Data/Transaction/MCSN/TELLER_140606.txt"

  acfile <- "Data/Transaction/MCSN/ACCOUNT_140606.txt"
} else if (mccompany == "MCMG") {
  sql <- 'SELECT disb.YearMonth YearMonth, disb.DisbCustomerCount DisbCustomerCount, disb.DisbLoanCount DisbLoanCount, disb.DisbAmount DisbAmount, loan.active.WithLoanCustomerCount WithLoanCustomerCount, loan.active.ActiveLoanCount ActiveLoanCount, loan.payment.PaymentCustomerCount PaymentCustomerCount, loan.payment.PaymentDayCount PaymentDayCount, loan.payment.PaymentAmount PaymentAmount
FROM
(SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) DisbCustomerCount, COUNT(LoanLocalCode) DisbLoanCount, SUM(LoanDisbursedAmount) DisbAmount
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE (LoanOpeningDate=DateData)
  GROUP BY 1) disb
  JOIN
  (SELECT *
  FROM
  (SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) WithLoanCustomerCount, COUNT(DISTINCT LoanLocalCode, 90000) ActiveLoanCount
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE (LoanStatus = "LIVING") OR (LoanStatus = "MATURED")
  GROUP BY 1) active
  JOIN
  (SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, COUNT(DISTINCT CustomerLocalID, 90000) PaymentCustomerCount, COUNT(DateData) PaymentDayCount, SUM(LoanChargesPaid)+SUM(LoanInterestsPaid)+SUM(LoanCapitalPaid)+SUM(LoanPenalitySpreadPaid)+SUM(LoanPenalityPaid) PaymentAmount
  FROM [mcr_dw.materialized_fact_loans_mcmd]
  WHERE LoanChargesPaid + LoanInterestsPaid + LoanCapitalPaid + LoanPenalitySpreadPaid + LoanPenalityPaid> 0
  GROUP BY 1) payment
  ON active.YearMonth = payment.YearMonth
  ) loan
  ON disb.YearMonth=loan.active.YearMonth
  ORDER BY 1;'
  
  # This is the SQL command to get if a customer has a loan in a month
  sql2 <- 'SELECT STRFTIME_UTC_USEC(DateData, "%Y-%m") YearMonth, CustomerLocalID, CustomerStatus, COUNT(REGEXP_EXTRACT(SavingType, "(CURRENT)")) Current, COUNT(REGEXP_EXTRACT(SavingType, "(SAVINGS)")) Savings, COUNT(DateData)
FROM [mcr_dw.materialized_fact_savings_mcmd]
WHERE DAY(DateData) = 1
  GROUP EACH BY 1, 2, 3
  ORDER BY 1, 2, 3;'
  
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

## Check if the customer has a loan
# Get data from BigQuery
bqcusloandt <- data.table(query_exec(project, dataset, sql2, billing = project, max_pages = 200))
setnames(bqcusloandt, "CustomerLocalID", "Customer1")
bqcusloandt[, Customer1 := as.integer(Customer1)]
ttdt <- merge(ttdt, bqcusloandt, by = c("Customer1", "YearMonth"), all.x = TRUE)

####################################################

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

##Customer with Loans
# * Total # of customers with loans
# * loan disbursements: # of customers, # of disbursements, value of disbursements
# * loan repayments: # of customers, # of payments, value of payments

# Load loan file
loandt <- data.table(query_exec(project, dataset, sql, billing = project))
# ReadJoinLoanData <- function(tt, loan)
ttresult <- merge(ttresult, loandt, by = "YearMonth")


## * current accounts: # of loan customers with current accounts
temp <- ddply(bqcusloandt[CustomerStatus == "With Loan" & Current > 0], .(YearMonth), summarize,
              LoanCurrentCustomerCount = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # cash ins, and value of cash ins, 
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category == 7313 & CustomerStatus == "With Loan"], .(YearMonth), summarize,
              LoanCurrentCashInAmount = sum(NetAmount), 
              LoanCurrentCashInCount = length(YearMonth),
              LoanCurrentCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # of cash outs, and value of cash outs
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category == 7313 & CustomerStatus == "With Loan"], .(YearMonth), summarize,
              LoanCurrentCashOutAmount = sum(NetAmount), 
              LoanCurrentCashOutCount = length(YearMonth),
              LoanCurrentCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)


## * savings accounts: # of loan customers with savings accounts 
temp <- ddply(bqcusloandt[CustomerStatus == "With Loan" & Savings > 0], .(YearMonth), summarize,
              LoanSavingsCustomerCount = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # cash ins, and value of cash ins
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category != 7313 & CustomerStatus == "With Loan"], .(YearMonth), summarize,
              LoanSavingsCashInAmount = sum(NetAmount), 
              LoanSavingsCashInCount = length(YearMonth),
              LoanSavingsCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # of cash outs, and value of cash outs
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category != 7313 & CustomerStatus == "With Loan"], .(YearMonth), summarize,
              LoanSavingsCashOutAmount = sum(NetAmount), 
              LoanSavingsCashOutCount = length(YearMonth),
              LoanSavingsCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# Customers without loans
# * Total number of unique customers without loans who have either a savings or a current account.
temp <- ddply(bqcusloandt[CustomerStatus == "With No Loan"], .(YearMonth), summarize,
              NoLoanCustomerCount = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

# * current accounts: # of customers with current accounts 
temp <- ddply(bqcusloandt[CustomerStatus == "With No Loan" & Current > 0], .(YearMonth), summarize,
              NoLoanCurrentCustomerCount = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # cash ins, and value of cash ins, 
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category == 7313 & CustomerStatus == "With No Loan"], .(YearMonth), summarize,
              NoLoanCurrentCashInAmount = sum(NetAmount), 
              NoLoanCurrentCashInCount = length(YearMonth),
              NoLoanCurrentCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # of cash outs, and value of cash outs
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category == 7313 & CustomerStatus == "With No Loan"], .(YearMonth), summarize,
              NoLoanCurrentCashOutAmount = sum(NetAmount), 
              NoLoanCurrentCashOutCount = length(YearMonth),
              NoLoanCurrentCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

## * savings accounts: # of loan customers with savings accounts 
temp <- ddply(bqcusloandt[CustomerStatus == "With No Loan" & Savings > 0], .(YearMonth), summarize,
              NoLoanSavingsCustomerCount = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # cash ins, and value of cash ins
temp <- ddply(ttdt[DrCrMarker == "CREDIT" & Category != 7313 & CustomerStatus == "With No Loan"], .(YearMonth), summarize,
              NoLoanSavingsCashInAmount = sum(NetAmount), 
              NoLoanSavingsCashInCount = length(YearMonth),
              NoLoanSavingsCashInCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

#   * # of customers, # of cash outs, and value of cash outs
temp <- ddply(ttdt[DrCrMarker == "DEBIT" & Category != 7313 & CustomerStatus == "With No Loan"], .(YearMonth), summarize,
              NoLoanSavingsCashOutAmount = sum(NetAmount), 
              NoLoanSavingsCashOutCount = length(YearMonth),
              NoLoanSavingsCashOutCustomer = length(unique(Customer1)))
ttresult <- merge(ttresult, temp, by = "YearMonth", all.x = T)

