#############################
## Create the report with priority list
cusdt[, LatestVerNo := max(CurrNo), by = CustomerLocalId]
cusdt[, Latest := LatestVerNo == CurrNo]
livecusdt <- cusdt[Latest == TRUE, list(CustomerLocalId, BioEnrolStat, ConfTelNo, BioEnrollFlag, BioEnrollDate, UpdateTime)]

pflist <- c("CORRESPONDANT", "CLIENT NR")
npflist <- c("Correspondant", "CustomerLocalId")
pcusdt <- fread(pcusfile, header = TRUE, na.strings = "", stringsAsFactors = F, 
                colClasses = c("CLIENT NR" = "character"))
setnames(pcusdt, pflist, npflist)
pcusdt <- merge(pcusdt, livecusdt, by = "CustomerLocalId", all.x = TRUE)

# Flag the status of enrolment
pcusdt[!is.na(ConfTelNo) & BioEnrollFlag == "Y" & BioEnrolStat == "UNLOCKED", EnrollStatus := "ENROLLED"]
pcusdt[is.na(ConfTelNo) & (is.na(BioEnrollFlag) | BioEnrollFlag == "N" ) & is.na(BioEnrolStat), 
       EnrollStatus := "NOT"]
pcusdt[is.na(EnrollStatus), EnrollStatus := "EXCEPTION"]





########################################
## Create the report of customer with enrolment status
cusinfodt <- cusdt

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



