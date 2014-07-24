library(data.table)
library(plyr)

# Load Customer into data table
LoadCustomer <- function(cusfile, safeMode = FALSE) {
  
  # Select fields needed
  flist <- c("@ID",
             "OPENING.DATE",
             "ACCOUNT.OFFICER",
             "BIO.ENROL.STAT",
             "CONF.TEL.NO",
             "BIO.ENROLL.FLAG",
             "BIO.ENROLL.DATE",
             "DATE.TIME",
             "CURR.NO")
  nlist <- c("Id", 
             "OpeningDate", 
             "AccountOfficer", 
             "BioEnrolStat", 
             "ConfTelNo", 
             "BioEnrollFlag", 
             "BioEnrollDate", 
             "DateTime", 
             "CurrNo")
  if (safeMode) {
    # In case some tables cannot be read by fread.
    flist2 <- flist
    flist2[1] <- "X.ID"
    cusdt <- data.table(read.table(cusfile, header = TRUE, sep = "^", quote = "", na.strings = "", 
               colClasses = "character", stringsAsFactors = FALSE)[flist2])
    setnames(cusdt, "X.ID", "@ID")
    
  } else {
    cusdt <- fread(cusfile, header = TRUE, sep = "^", na.strings = "", stringsAsFactors = FALSE, 
                   select = flist, colClasses = c(CONF.TEL.NO = "character"))
  }
  
  cusdt <- cusdt[, flist, with = F]
  cusdt_copy <- cusdt[, flist, with = F]
  
  # preprocess the data table
  cusdt[, CustomerLocalId := gsub(";.+", "", cusdt[["@ID"]])]
  
  cusdt[, DATE.TIME := paste("20", DATE.TIME, sep = "")]
  cusdt[, UpdateTime := as.POSIXct(strptime(DATE.TIME, "%Y%m%d%H%M", "UTC"))]
  cusdt[, UpdateDate := as.Date(substr(DATE.TIME, 1, 8), "%Y%m%d")]
  cusdt[, OPENING.DATE := as.Date(as.character(OPENING.DATE), "%Y%m%d")]
  cusdt[, BIO.ENROLL.DATE := as.Date(as.character(BIO.ENROLL.DATE), "%Y%m%d")]
  
  cusdt[, BIO.ENROLL.FLAG := as.factor(BIO.ENROLL.FLAG)]
  cusdt[, BIO.ENROL.STAT := as.factor(BIO.ENROL.STAT)]
  
  cusdt[, CONF.TEL.NO := as.character(CONF.TEL.NO)]
  
  setnames(cusdt, flist, nlist)
  
  cusdt[, CurrNo := as.numeric(CurrNo)]
  #Setup keys
  setkey(cusdt)
  
  cusdt
}


## More detailed customer information, can be combined with previous function
LoadCustomerInfo <- function(cusfile) {
  
  # Select fields needed
  flist <- c("@ID",
             "OPENING.DATE",
             "SHORT.NAME",
             "NAME.1",
             "TEL.MOBILE",
             "ACCOUNT.OFFICER",
             "BIO.ENROL.STAT",
             "CONF.TEL.NO",
             "BIO.ENROLL.FLAG",
             "BIO.ENROLL.DATE",
             "DATE.TIME",
             "INPUTTER",
             "AUTHORISER",
             "CURR.NO")
  nlist <- c("Id", 
             "OpeningDate", 
             "FirstName",
             "LastName",
             "TelMobile",
             "LocalDAO", 
             "BioEnrolStat", 
             "ConfTelNo", 
             "BioEnrollFlag", 
             "BioEnrollDate", 
             "DateTime", 
             "Enroller",
             "Verifier",
             "CurrNo")
  colclass <- c(ACCOUNT.OFFICER = "character", 
                BIO.ENROL.STAT = "character", 
                BIO.ENROLL.FLAG = "character", 
                CONF.TEL.NO = "character")
  
  cusdt <- fread(cusfile, header = TRUE, sep = "^", na.strings = "", stringsAsFactors = F, 
                 select = flist, colClasses = colclass)
  
  cusdt <- cusdt[, flist, with = F]
  cusdt_copy <- cusdt[, flist, with = F]
  
  # preprocess the data table
  cusdt[, CustomerLocalId := gsub(";.+", "", cusdt[["@ID"]])]
  
  cusdt[, DATE.TIME := paste("20", DATE.TIME, sep = "")]
  cusdt[, UpdateTime := as.POSIXct(strptime(DATE.TIME, "%Y%m%d%H%M", "UTC"))]
  cusdt[, UpdateDate := as.Date(substr(DATE.TIME, 1, 8), "%Y%m%d")]
  cusdt[, OPENING.DATE := as.Date(as.character(OPENING.DATE), "%Y%m%d")]
  cusdt[, BIO.ENROLL.DATE := as.Date(as.character(BIO.ENROLL.DATE), "%Y%m%d")]
  
  cusdt[, BIO.ENROLL.FLAG := as.factor(BIO.ENROLL.FLAG)]
  cusdt[, BIO.ENROL.STAT := as.factor(BIO.ENROL.STAT)]
  
  cusdt[, CONF.TEL.NO := as.character(CONF.TEL.NO)]
  
  cusdt[, INPUTTER := gsub("([0-9]+_)|(___.+)", "", INPUTTER)]
  cusdt[, AUTHORISER := gsub("([0-9]+_)|(_OFS.+)", "", AUTHORISER)]
  
  setnames(cusdt, flist, nlist)
  #Setup keys
  setkey(cusdt)
  
  cusdt
}

## Load DAO files
LoadDAO <- function(daofile) {
  ## Load DAO table
  daodt <- fread(daofile, header = TRUE, sep = "^", na.strings = "", stringsAsFactors = F)
  
  flist <- c("@ID",
             "NAME",
             "BRANCH.LOC",
             "DATE.TIME",
             "CURR.NO")
  nlist <- c("Id",
             "Name",
             "BranchId",
             "DateTime",
             "CurrNo")
  
  daodt <- daodt[, flist, with = F]
  daodt_copy <- daodt[, flist, with = F]
  
  setnames(daodt, flist, nlist)
  # preprocess
  daodt[, DAO := gsub(";.+", "", daodt[["Id"]])]
  
  daodt[, DateTime := paste("20", DateTime, sep = "")]
  daodt[, UpdateTime := as.POSIXct(strptime(DateTime, "%Y%m%d%H%M", "UTC"))]
  daodt[, UpdateDate := as.Date(substr(DateTime, 1, 8), "%Y%m%d")]
  daodt[, BranchId := as.character(BranchId)]
  daodt <- daodt[!is.na(BranchId), ]
  
  
  #livedao <- daodt[Latest == TRUE, ]
  
  #Join key
  setkey(daodt, DAO, CurrNo, BranchId)
  
  daodt
}


## Load Branch tables
LoadBranch <- function(branchfile) {
  # Loand Branch table
  branchdt <- fread(branchfile, header = TRUE, sep = "^", na.strings = "", stringsAsFactors = F)
  
  flist <- c("@ID",
             "BRANCH.NAME",
             "DATE.TIME",
             "CURR.NO")
  nlist <- c("Id",
             "BranchName",
             "DateTime",
             "CurrNo")
  
  branchdt <- branchdt[, flist, with = F]
  branchdt_copy <- branchdt[, flist, with = F]
  # Preprocessing
  branchdt[, BranchId := gsub(";.+", "", branchdt[["@ID"]])]
  
  #livebranch <- branchdt[Latest == TRUE, ]
  
  setnames(branchdt, flist, nlist)
  #setnames(livebranch, flist, nlist)
  
  # Join key
  setkey(branchdt, BranchId)
  
  branchdt
}

## Join DAO and Branch table together
JoinDAOBranch <- function(daodt, branchdt) {
  # find out the latest record
  daodt[, LatestVerNo := max(CurrNo), by = DAO]
  daodt[, Latest := LatestVerNo == CurrNo]
  livedao <- daodt[Latest == TRUE, ]
  
  branchdt[, LatestVerNo := max(CurrNo), by = BranchId]
  branchdt[, Latest := LatestVerNo == CurrNo]
  livebranch <- branchdt[Latest == TRUE, ]
  
  ## Merge Branch into DAo
  DAOBranch <- merge(livedao, livebranch, by = "BranchId", all.x = TRUE)
  DAOBranch <- subset(DAOBranch, select = c("DAO", "Name", "BranchId", "BranchName"))
  setnames(DAOBranch, c("DAO", "Name", "BranchId", "BranchName"), 
           c("LocalDAO", "DAODescription", "BranchLocalId", "BranchDescription"))
  
  DAOBranch
}

# Save a table to database
# Not used yet
SaveTable <- function(table, df) {
  require("RSQLite")
  # Set up database    
  con <- dbConnect(dbDriver("SQLite"), dbname = "enrolment.db")
  
  if (dbExistsTable(con, table)) {
    dbRemoveTable(con, table)
  }
  result <- dbWriteTable(con, table, df)
  dbDisconnect(con)
  
  result
}

ReadTable <- function(table) {
  require("RSQLite")
  # Set up database
  con <- dbConnect(dbDriver("SQLite"), dbname = "enrolment.db")
  result <- dbGetQuery(con, paste("SELECT * FROM", table))
  
  dbDisconnect(con)
  result
}

## Merge 2 data tables together
MergeTable <- function(dt1, dt2, mkey) {
  # Merge only first mcol columns if the column of 2 tables are different
  
  mcol <- min(ncol(dt1), ncol(dt2))
  if (mcol == 0){
    alldata <- rbind(dt1, dt2)
    setkeyv(alldata, mkey)
    return(unique(alldata))
  } else {
    alldata <- rbind(dt1[, c(1:mcol), with = F], dt2[, c(1:mcol), with = F])
    setkeyv(alldata, mkey)
    return(unique(alldata))
  }
  
}

ydate <- function() {
  yd <- as.Date(julian(Sys.Date()) - 1, origin = "1970-1-1")
  c(format(yd, "%Y%m%d"),
    format(yd, "%Y-%m-%d"))
}

