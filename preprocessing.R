
library(tidyverse)
library(pracma)
library(lubridate)
library(data.table)
library(forecast)

if (format(now(),"%m-%d")=="10-15" | format(now(),"%m-%d")=="10-31"){
    quit(save = "no", status = 0, runLast = TRUE)
}

library(paws)
svc <- secretsmanager(
  config = list(
    region = "us-west-2"
  )
)

resp <- svc$get_secret_value(SecretId = "dw_production")$SecretString

UID <- jsonlite::fromJSON(resp)$username
PWD <- jsonlite::fromJSON(resp)$password
###################
library(RODBC)

con_mmi <- odbcDriverConnect(connection=paste0("Driver={ODBC Driver 17 for SQL Server};server=10.93.2.225;database=MMI Prod;uid=", UID, ";pwd=", PWD, ";"))
con_ds <- odbcDriverConnect(connection=paste0("Driver={ODBC Driver 17 for SQL Server};server=10.52.10.105;database=data_science;uid=", UID, ";pwd=", PWD, ";"))

data.membership <- sqlQuery(con_mmi,'
select * from MMI_MembershipDashboardv4_7
')

data.tracker <- sqlQuery(con_mmi,'
select * from MMI_Optix_Tracker
')

data.regressor <- sqlQuery(con_mmi,'
select * from MMI_Optix_Regressor_Dev_09_04_2020
')

data.adjustment <- sqlQuery(con_mmi,'
select * from MMI_Optix_Adjustment_Dev_2020_09_22
')

data.budget <- sqlQuery(con_ds,'
select * from Optix_Budget
')

odbcCloseAll()

Sys.setenv(TZ='MST')


# COMMAND ----------

# DBTITLE 1,Data manipulation
data.membership$PriorRenewedMemberCount <- data.membership$`Prior Year Member Count`-data.membership$`Prior Year New Member Count`
data.membership$RenewedMemberCount <- data.membership$`Member Count`-data.membership$`New Member Count`
colnames(data.membership)[colnames(data.membership)=="Report Date"] <- 'Date'
colnames(data.membership)[colnames(data.membership)=="Prior Year New Member Count"] <- 'PriorNewMemberCount'
colnames(data.membership)[colnames(data.membership)=="New Member Count"] <- 'NewMemberCount'
colnames(data.membership)[colnames(data.membership)=="Membership Type"] <- 'MembershipType'
colnames(data.membership)[colnames(data.membership)=="Prior Year Member Count"] <- 'PriorMemberCount'
colnames(data.membership)[colnames(data.membership)=="Member Count"] <- 'MemberCount'
colnames(data.membership)[colnames(data.membership)=="Prior Coverage"] <- 'PriorCoverage'
data.membership$'Run Date Time' <- NULL
data.membership$'YTD New Member Count' <- NULL
data.membership$'Prior Year YTD New Member Count' <- NULL
data.membership$Date <- as.Date(data.membership$Date)
data.membership$MembershipType <- as.factor(data.membership$MembershipType)
data.membership$Region <- as.factor(data.membership$Region)
data.membership$Coverage <- as.factor(data.membership$Coverage)
data.membership$PriorCoverage <- as.factor(data.membership$PriorCoverage)
data.budget$Date <- as.Date(data.budget$Date)
data.budget$MembershipType <- as.factor(data.budget$MembershipType)
data.budget$Region <- as.factor(data.budget$Region)
data.budget$Coverage <- as.factor(data.budget$Coverage)
data.coverage <- data.membership %>% dplyr::group_by(Date, MembershipType, Region, PriorCoverage) %>%
  dplyr::summarise(CoverageMemberCount=sum(MemberCount),
                   CoverageNewMemberCount=sum(NewMemberCount),
                   CoverageRenewedMemberCount=sum(RenewedMemberCount)) %>%
  dplyr::rename(Coverage=PriorCoverage)
data.membership$PriorCoverage <- NULL
data.membership <- data.membership %>% dplyr::group_by(Date, MembershipType, Region, Coverage) %>%
  summarise_all(funs(sum)) %>%
  left_join(data.coverage, by=c('Date','MembershipType','Region','Coverage'))
data.membership <- data.membership %>% dplyr::filter(Date >= "2014-01-31")
data.membership <- data.membership %>% dplyr::arrange(MembershipType, Region, Coverage)
total.models <- 70
group.length <- nrow(data.membership)/total.models
data.membership <- add_column(data.membership, Group=rep(1:total.models, each=group.length), .after='Date')
data.regressor$Date <- as.Date(data.regressor$Date)
data.adjustment$Date <- as.Date(data.adjustment$Date)
colnames(data.tracker)[colnames(data.tracker)=="Join Date"] <- 'Date'
colnames(data.tracker)[colnames(data.tracker)=="Join Count"] <- 'JoinCount'
colnames(data.tracker)[colnames(data.tracker)=="Member Count"] <- 'MemberCount'
data.tracker$Date <- as.Date(data.tracker$Date)
data.tracker <- data.tracker %>% dplyr::arrange(Date)
data.tracker$'Run Date' <- NULL
data.tracker$'Report Date' <- NULL
data.tracker$'Type' <- NULL
data.tracker$'Join Year' <- NULL
data.tracker$'Join Year Month' <- NULL

#copying member count values from 1 year ago to prior member count column
data.membership <- data.table(data.membership)
data.membership <- data.membership[, PriorMemberCount:=c(PriorMemberCount[1:12], MemberCount[1:(.N-12)]), by=Group]
setattr(data.membership, "class", c("tbl", "tbl_df", "data.frame"))

# COMMAND ----------

# DBTITLE 1,Model - Join
data.join <- data.tracker
data.join <- data.join %>% dplyr::group_by(Date=floor_date(Date, unit="week")) %>% summarise(JoinCount=sum(JoinCount))
data.join <- data.join[-nrow(data.join),]
regressor.join <- as.data.frame(data.regressor)
regressor.join <- regressor.join %>%
  dplyr::filter(Date >= "2015-03-01") %>%
  dplyr::rename(old.Date=Date) %>%
  dplyr::mutate(datekey=format(old.Date, "%Y-%m"))
join.temp <- data.frame(Date=as.Date(seq(head(data.join$Date, n=1), tail(regressor.join$old.Date, n=1), by="weeks")))
join.temp <- join.temp %>%
  left_join(data.join, by="Date") %>%
  dplyr::mutate(datekey=format(Date, "%Y-%m"))
regressor.join <- regressor.join %>%
  left_join(join.temp, by="datekey") %>%
  dplyr::select(-old.Date, -datekey, -JoinCount) %>%
  dplyr::select(Date, everything())
regressor.join <- as.data.frame(regressor.join)
regressor.past <- regressor.join[1:nrow(data.join),]
regressor.future <- regressor.join[(nrow(data.join)+1):nrow(regressor.join),]
                        
forecast.join <- function(category) {
  forecast.var <- ts(data.join[, category], start=c(2015,3,1), frequency=365.25/7)
  #  fit <- auto.arima(forecast.var, xreg=data.matrix(regressor.past), D=1)
  fit <- auto.arima(forecast.var, xreg=data.matrix(regressor.past[, c(2:4)]),
                    stepwise=F, approximation=F, parallel=T, D=1, num.cores=NULL)
  fcast <- forecast(fit, xreg=data.matrix(regressor.future[, c(2:4)]))
  fcast.values <- data.frame(Date=data.join$Date, Forecast=as.numeric(fcast$fitted))
  colnames(fcast.values)[colnames(fcast.values)=="Forecast"] <- paste('Forecast',category,sep="")
  fcast.values.future <- as.data.frame(fcast)
  fcast.values.future$Date <- regressor.future[, 1]
  rownames(fcast.values.future) <- NULL
  fcast.values.future <- fcast.values.future[,c(6,1)]
  colnames(fcast.values.future)[colnames(fcast.values.future)=="Point Forecast"] <- paste('Forecast',category,sep="")
  
  fcast.list <- list("past"=fcast.values, "future"=fcast.values.future)
  return(fcast.list)
}

fcast.list <- NULL
fcast.values.past <- NULL
fcast.values.future <- NULL
fcast.list <- forecast.join('JoinCount')
fcast.values.past <- fcast.list$past
fcast.values.future <- fcast.list$future

data.join <- fcast.values.past %>% right_join(data.join, by="Date")
rownames(data.join) <- NULL
fcast.values.future <- fcast.values.future %>% dplyr::filter(Date > tail(fcast.values.past$Date, 1))
data.join <- bind_rows(data.join, fcast.values.future)

daily.join.months <- data.tracker %>%
  dplyr::filter(Date >= tail(data.tracker$Date,1) %m-% months(6))
daily.join.months.last.year <- data.tracker %>%
  dplyr::filter(Date <= tail(data.tracker$Date,1)-years() & Date >= tail(data.tracker$Date,1) %m-% months(6) %m-% years())
data.join.daily <- daily.join.months %>%
  dplyr::mutate(JoinCountLastYear=daily.join.months.last.year$JoinCount) %>%
  dplyr::mutate(JoinDifference=as.numeric(daily.join.months$JoinCount-daily.join.months.last.year$JoinCount)) %>%
  dplyr::select(-MemberCount)

# COMMAND ----------

# DBTITLE 1,Model - Group
data.membership <- data.membership %>% dplyr::group_by(Date) %>%
  rowwise() %>%
  dplyr::mutate(Group2=1+((Group-1)%/%5))

data.groups <- data.membership %>% dplyr::group_by(Date) %>%
  rowwise() %>%
  dplyr::mutate(Group2=1+((Group-1)%/%5)) %>%
  dplyr::mutate(MemberGrowth=sum((MemberCount-PriorMemberCount)/PriorMemberCount)*100) %>%
  dplyr::mutate(NewMemberGrowth=sum((NewMemberCount-PriorNewMemberCount)/PriorNewMemberCount)*100) %>%
  dplyr::mutate(RenewedMemberGrowth=sum((RenewedMemberCount-PriorRenewedMemberCount)/PriorRenewedMemberCount)*100) %>%
  dplyr::mutate(NewRatio=sum((MemberCount-RenewedMemberCount)/PriorMemberCount)*100)

data.groups <- data.groups %>% 
  dplyr::group_by(Date, MembershipType, Region) %>%
  dplyr::mutate(RenewRatio=(sum(MemberCount-NewMemberCount)/sum(PriorMemberCount))*100)


train_data_begins <- '2014-01-01'
train_data_ends <- Sys.Date()
current_date <- tail(data.membership, 1)$Date

### Detrending & differencing regressors
data.regressor.filtered <- data.regressor[which(data.regressor$Date > train_data_begins),]
# reg_date <- data.regressor.filtered[2:nrow(data.regressor.filtered),'Date']
# reg_unemp <- diff(detrend(data.regressor.filtered$Unemployment_RBC, tt='linear'))
# reg_wti <- diff(detrend(data.regressor.filtered$WTI_EIA, tt='linear'))
# reg_accr <- diff(detrend(data.regressor.filtered$ACCR_ratio, tt='linear'))

reg_date <- data.regressor.filtered[1:nrow(data.regressor.filtered),'Date']
reg_unemp <- data.regressor.filtered$Unemployment_RBC
reg_wti <- data.regressor.filtered$WTI_EIA
reg_accr <- data.regressor.filtered$ACCR_ratio

reg_processed <- data.frame(Date=reg_date, Unemp=reg_unemp, WTI=reg_wti, ACCR=reg_accr)

regressor.new <- reg_processed[, c(1:3)]
regressor.renew <- reg_processed[, c(1:4)]

forecast.groups <- function(group_num, category, regressor) {
  npast <- nrow(data.membership[which(data.membership$Group==1 & data.membership$Date>train_data_begins & data.membership$Date<=train_data_ends), ])
  most_recent_date <- tail(data.membership, 1)$Date
  
  if (category == 'RenewRatio'){
    past_cndn <- which(data.membership$Group2==group_num & data.membership$Date>train_data_begins& data.membership$Date > train_data_begins & data.membership$Date<=train_data_ends)
    series_past <- data.groups[past_cndn, category]
    dropped_past <- data.groups[which(data.membership$Group2==group_num & data.membership$Date >= train_data_ends),]
    dropped_dates <- dropped_past[1:(nrow(dropped_past)/5), 'Date']$Date
  } else {
    past_cndn <- which(data.membership$Group==group_num & data.membership$Date>train_data_begins& data.membership$Date > train_data_begins & data.membership$Date<=train_data_ends)
    series_past <- data.groups[past_cndn, category]
    dropped_dates <- data.groups$Date[which(data.membership$Group==group_num & data.membership$Date >= train_data_ends)]
  }

#   regressor.past <- regressor[1:npast-1,  c(-1)]    
  regressor.past <- regressor[1:npast,  c(-1)]

  series_past <- series_past[1:npast, ]
  
  past_init_val <- series_past[1, ][[category]]
#   series_past_diff <- diff(series_past[[category]])
  series_past_diff <- series_past[[category]]
  
  forecast.var <- ts(series_past_diff, start=c(2014, 1), frequency=12) #dummy dates
  
  regressor.future <- regressor[(npast):nrow(regressor), ]
  future_dates <- regressor.future %>% select(1)
  regressor.future <- regressor.future %>% select(-1)
  
  fit <- auto.arima(forecast.var, stepwise=F, approximation=F, parallel=T, seasonal=T, 
                    xreg=data.matrix(regressor.past), num.cores=NULL)
  
  
  fcast <- forecast(fit, xreg=data.matrix(regressor.future))
  past_cndn <- which(data.membership$Group==group_num & data.membership$Date>train_data_begins& data.membership$Date > train_data_begins & data.membership$Date<=train_data_ends)
  if (category == 'NewMemberCount'){
    fcast.values <- data.frame(Date=data.groups$Date[past_cndn][1:npast],
                               MembershipType=data.groups$MembershipType[past_cndn][1:npast],
                               Region=data.groups$Region[past_cndn][1:npast],
                               Coverage=data.groups$Coverage[past_cndn][1:npast],
#                                Forecast=diffinv(as.numeric(fcast$fitted), xi=past_init_val))
                               Forecast=as.numeric(fcast$fitted))
    
    fcast.values <- add_column(fcast.values, Group=group_num)
  } else {
    fcast.values <- data.frame(Date=data.groups$Date[past_cndn][1:npast],
#                                Forecast=diffinv(as.numeric(fcast$fitted), xi=past_init_val))#, dropped_past[[category]]))
                               Forecast=as.numeric(fcast$fitted))
  }
  
  colnames(fcast.values)[colnames(fcast.values)=="Forecast"] <- paste('Forecast',category,sep="")
  
  future_init_val <- tail(series_past, 1)[[category]]
  fcast.values.future <- as.data.frame(fcast)
  fcast.values.future$Date <- future_dates$Date
  rownames(fcast.values.future) <- NULL
  fcast.values.future <- fcast.values.future[,c(6,1)]
  if (category == 'NewMemberCount'){
    fcast.values.future <- add_column(fcast.values.future, Group=group_num, .after='Date')
    fcast.values.future$MembershipType <- data.groups$MembershipType[which(data.membership$Group==group_num)][1:nrow(fcast.values.future)]
    fcast.values.future$Region=data.groups$Region[which(data.membership$Group==group_num)][1:nrow(fcast.values.future)]
    fcast.values.future$Coverage=data.groups$Coverage[which(data.membership$Group==group_num)][1:nrow(fcast.values.future)]
  }
#   fcast.values.future[, 'Point Forecast'] <- diffinv(fcast.values.future[, 'Point Forecast'], xi=future_init_val)[2:(nrow(fcast.values.future)+1)]
  fcast.values.future[, 'Point Forecast'] <- fcast.values.future[, 'Point Forecast']
  colnames(fcast.values.future)[colnames(fcast.values.future)=="Point Forecast"] <- paste('Forecast',category,sep="")
  
  fcast.values <- rbind(fcast.values, fcast.values.future[fcast.values.future$Date %in% dropped_dates,])
  
  fcast.values.future <- fcast.values.future[fcast.values.future$Date > most_recent_date,]
  
  fcast.list <- list("past"=fcast.values, "future"=fcast.values.future)
  return(fcast.list)
}

fcast.values.past <- NULL
fcast.values.future <- NULL
fcast.values.past2 <- NULL
fcast.values.future2 <- NULL
fcast.values.past3 <- NULL
fcast.values.future3 <- NULL
fcast.values.past4 <- NULL
fcast.values.future4 <- NULL
fcast.values.past5 <- NULL
fcast.values.future5 <- NULL

for (n in 1:total.models) {
  fcast.list <- forecast.groups(n, 'NewMemberCount', regressor.new)
  fcast.values.past2 <- rbind(fcast.values.past2, fcast.list$past)
  fcast.values.future2 <- rbind(fcast.values.future2, fcast.list$future)
  }

for (n in 1:(total.models/5)) {
  fcast.list <- forecast.groups(n, 'RenewRatio', regressor.renew)
  for (m in 1:5){
    fcast.values.past3 <- rbind(fcast.values.past3, fcast.list$past)
    fcast.values.future3 <- rbind(fcast.values.future3, fcast.list$future)
  }
}

fcast.values.past <- cbind(fcast.values.past2,
                           ForecastRenewRatio=fcast.values.past3$ForecastRenewRatio)

fcast.values.future <- cbind(fcast.values.future2,
                             ForecastRenewRatio=fcast.values.future3$ForecastRenewRatio)

# Old Date -> date 1 year ago from date of forecast
data.groups <- fcast.values.past %>% right_join(data.groups, by=c('Date', 'Group', 'MembershipType', 'Region', 'Coverage'))

fcast.values.future <- fcast.values.future %>%
  dplyr::mutate(OldDate=ceiling_date(Date[dplyr::row_number()], "month") -years(1) -days(1))

if (nrow(fcast.values.future[fcast.values.future$Group==1,]) > 12) {
  fcast.values.future.mod1 <- fcast.values.future %>%
    dplyr::filter(fcast.values.future$Date <= fcast.values.future$Date[12])
  fcast.values.future.mod1 <- fcast.values.future.mod1 %>%
    rowwise() %>%
    dplyr::mutate(PriorMemberCount=data.groups$MemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(PriorNewMemberCount=data.groups$NewMemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(PriorRenewedMemberCount=data.groups$RenewedMemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(ForecastRenewedMemberCount=0.01*ForecastRenewRatio*PriorMemberCount)
  fcast.values.future.mod2 <- fcast.values.future %>%
    dplyr::filter(fcast.values.future$Date > fcast.values.future$Date[12])
  fcast.values.future.mod2 <- fcast.values.future.mod2 %>%
    rowwise() %>%
    dplyr::mutate(PriorNewMemberCount=fcast.values.future.mod1$ForecastNewMemberCount[fcast.values.future.mod1$Date==OldDate & fcast.values.future.mod1$Group==Group]) %>%
    dplyr::mutate(PriorRenewedMemberCount=fcast.values.future.mod1$ForecastRenewedMemberCount[fcast.values.future.mod1$Date==OldDate & fcast.values.future.mod1$Group==Group]) %>%
    dplyr::mutate(PriorMemberCount=PriorNewMemberCount+PriorRenewedMemberCount) %>%
    dplyr::mutate(ForecastRenewedMemberCount=0.01*ForecastRenewRatio*PriorMemberCount)
  fcast.values.future.mod <- bind_rows(fcast.values.future.mod1, fcast.values.future.mod2)
} else {
  fcast.values.future <- fcast.values.future %>%
    rowwise() %>%
    dplyr::mutate(PriorMemberCount=data.groups$MemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(PriorNewMemberCount=data.groups$NewMemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(PriorRenewedMemberCount=data.groups$RenewedMemberCount[data.groups$Date==OldDate & data.groups$Group==Group]) %>%
    dplyr::mutate(ForecastRenewedMemberCount=0.01*ForecastRenewRatio*PriorMemberCount)
}
fcast.values.future.mod$OldDate <- NULL

data.groups <- bind_rows(data.groups, fcast.values.future.mod)
data.groups <- dplyr::arrange(data.groups, Group, Date)
data.groups <- data.groups %>%
  rowwise() %>%
  dplyr::mutate(ForecastMemberCount=sum(ForecastNewMemberCount+ForecastRenewedMemberCount)) %>%
  dplyr::mutate(ForecastMemberGrowth=sum((ForecastMemberCount-PriorMemberCount)/PriorMemberCount)*100) %>%
  dplyr::mutate(ForecastNewMemberGrowth=sum((ForecastNewMemberCount-PriorNewMemberCount)/PriorNewMemberCount)*100) %>%
  dplyr::mutate(ForecastRenewedMemberGrowth=sum((ForecastRenewedMemberCount-PriorRenewedMemberCount)/PriorRenewedMemberCount)*100) %>%
  dplyr::mutate(ForecastNewRatio=sum((ForecastMemberCount-ForecastRenewedMemberCount)/PriorMemberCount)*100) %>%
  dplyr::mutate(ForecastCoverageMemberCount=ForecastMemberCount) %>%
  dplyr::select(-CoverageNewMemberCount, -CoverageRenewedMemberCount, -Group2)

### ANNUAL AND QUARTER BUDGET UPDATE
if (format(now(),"%m-%d")=="10-31") {
  data.groups.adjust <- data.groups %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(member.total.sum=sum(ForecastMemberCount), newmember.total.sum=sum(ForecastNewMemberCount),
                  renewedmember.total.sum=sum(ForecastRenewedMemberCount),
                  coveragemember.total.sum=sum(ForecastCoverageMemberCount)) %>%
    dplyr::mutate(AdjustMemberCount=((ForecastMemberCount/member.total.sum)*
                                       ((ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,2], NA))+
                                          (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,3], NA)))+
                                       ForecastMemberCount),
                  AdjustCoverageMemberCount=((ForecastCoverageMemberCount/coveragemember.total.sum)*
                                               ((ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,2], NA))+
                                                  (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,3], NA)))+
                                               ForecastCoverageMemberCount),
                  AdjustNewMemberCount=((ForecastNewMemberCount/newmember.total.sum)*
                                          (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,2], NA))+
                                          ForecastNewMemberCount),
                  AdjustRenewedMemberCount=((ForecastRenewedMemberCount/renewedmember.total.sum)*
                                              (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,3], NA))+
                                              ForecastRenewedMemberCount)) %>%
    rowwise() %>%
    dplyr::mutate(AdjustMemberGrowth=sum((AdjustMemberCount-PriorMemberCount)/PriorMemberCount)*100) %>%
    dplyr::mutate(AdjustNewMemberGrowth=sum((AdjustNewMemberCount-PriorNewMemberCount)/PriorNewMemberCount)*100) %>%
    dplyr::mutate(AdjustRenewedMemberGrowth=sum((AdjustRenewedMemberCount-PriorRenewedMemberCount)/PriorRenewedMemberCount)*100) %>%
    dplyr::select(Date, Group, AdjustMemberCount, AdjustCoverageMemberCount, AdjustNewMemberCount, AdjustRenewedMemberCount,
                  AdjustMemberGrowth, AdjustNewMemberGrowth, AdjustRenewedMemberGrowth) %>%
    dplyr::arrange(Group, Date)
  data.groups.adjust2 <- data.budget %>%
    dplyr::select(Date, Group, QuarterMemberCount, QuarterCoverageMemberCount, QuarterNewMemberCount, QuarterRenewedMemberCount,
                  QuarterMemberGrowth, QuarterNewMemberGrowth, QuarterRenewedMemberGrowth)
  data.groups.adjust <- data.groups.adjust %>%
    left_join(data.groups.adjust2, by=c("Date", "Group"))
} else if (format(now(),"%m-%d")=="04-15" | format(now(),"%m-%d")=="07-15" | format(now(),"%m-%d")=="10-15" | format(now(),"%m-%d")=="01-31") {
  data.groups.adjust <- data.groups %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(member.total.sum=sum(ForecastMemberCount), newmember.total.sum=sum(ForecastNewMemberCount),
                  renewedmember.total.sum=sum(ForecastRenewedMemberCount),
                  coveragemember.total.sum=sum(ForecastCoverageMemberCount)) %>%
    dplyr::mutate(QuarterMemberCount=((ForecastMemberCount/member.total.sum)*
                                        ((ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,4], NA))+
                                           (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,5], NA)))+
                                        ForecastMemberCount),
                  QuarterCoverageMemberCount=((ForecastCoverageMemberCount/coveragemember.total.sum)*
                                                ((ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,4], NA))+
                                                   (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,5], NA)))+
                                                ForecastCoverageMemberCount),
                  QuarterNewMemberCount=((ForecastNewMemberCount/newmember.total.sum)*
                                           (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,4], NA))+
                                           ForecastNewMemberCount),
                  QuarterRenewedMemberCount=((ForecastRenewedMemberCount/renewedmember.total.sum)*
                                               (ifelse(!is.null(Date), data.adjustment[data.adjustment$Date==Date,5], NA))+
                                               ForecastRenewedMemberCount)) %>%
    rowwise() %>%
    dplyr::mutate(QuarterMemberGrowth=sum((QuarterMemberCount-PriorMemberCount)/PriorMemberCount)*100) %>%
    dplyr::mutate(QuarterNewMemberGrowth=sum((QuarterNewMemberCount-PriorNewMemberCount)/PriorNewMemberCount)*100) %>%
    dplyr::mutate(QuarterRenewedMemberGrowth=sum((QuarterRenewedMemberCount-PriorRenewedMemberCount)/PriorRenewedMemberCount)*100) %>%
    dplyr::select(Date, Group, QuarterMemberCount, QuarterCoverageMemberCount, QuarterNewMemberCount, QuarterRenewedMemberCount,
                  QuarterMemberGrowth, QuarterNewMemberGrowth, QuarterRenewedMemberGrowth) %>%
    dplyr::arrange(Group, Date)
  data.groups.adjust2 <- data.budget %>%
    dplyr::select(Date, Group, AdjustMemberCount, AdjustCoverageMemberCount, AdjustNewMemberCount, AdjustRenewedMemberCount,
                  AdjustMemberGrowth, AdjustNewMemberGrowth, AdjustRenewedMemberGrowth)
  data.groups.adjust <- data.groups.adjust %>%
    left_join(data.groups.adjust2, by=c("Date", "Group"))
} else {
  data.groups.adjust <- data.budget %>%
    dplyr::select(Date, Group, AdjustMemberCount, AdjustCoverageMemberCount, AdjustNewMemberCount, AdjustRenewedMemberCount,
                  AdjustMemberGrowth, AdjustNewMemberGrowth, AdjustRenewedMemberGrowth, QuarterMemberCount, QuarterCoverageMemberCount,
                  QuarterNewMemberCount, QuarterRenewedMemberCount, QuarterMemberGrowth, QuarterNewMemberGrowth, QuarterRenewedMemberGrowth) %>%
    dplyr::arrange(Group, Date)
}

data.groups <- data.groups %>%
  left_join(data.groups.adjust, by=c("Date", "Group"))

forecast_dates <- data.groups.adjust[(data.groups.adjust$Group==1) & (data.groups.adjust$Date > tail(data.membership, 1)$Date), 'Date']

if (length(forecast_dates) > 12) {
  data.groups.n1 <- data.groups %>%
    dplyr::filter(Date <= tail(data.membership, 1)$Date)

  data.groups.n1 <- data.groups.n1 %>%
    rowwise() %>%
    dplyr::mutate(PriorQuarterNewMemberCount=PriorNewMemberCount) %>%
    dplyr::mutate(PriorQuarterRenewedMemberCount=PriorRenewedMemberCount) %>%
    dplyr::mutate(PriorQuarterMemberCount=PriorMemberCount)
  
  data.groups.n2 <- data.groups %>%
    dplyr::filter(Date >= train_data_ends)
  data.groups.n2 <- data.table(data.groups.n2)
  data.groups.n2 <- data.groups.n2[, PriorQuarterNewMemberCount:=c(PriorNewMemberCount[1:12], QuarterNewMemberCount[1:(.N-12)]), by=Group]
  data.groups.n2 <- data.groups.n2[, PriorQuarterRenewedMemberCount:=c(PriorRenewedMemberCount[1:12], QuarterRenewedMemberCount[1:(.N-12)]), by=Group]
  data.groups.n2 <- data.groups.n2[, PriorQuarterMemberCount:=c(PriorMemberCount[1:12], QuarterMemberCount[1:(.N-12)]), by=Group]
  setattr(data.groups.n2, "class", "data.frame")
  
  data.groups <- bind_rows(data.groups.n1, data.groups.n2)
  data.groups <- data.groups %>%
    dplyr::arrange(Group, Date)
} else {
  data.groups <- data.groups %>%
    rowwise() %>%
    dplyr::mutate(PriorQuarterNewMemberCount=PriorNewMemberCount)
    dplyr::mutate(PriorQuarterRenewedMemberCount=PriorRenewedMemberCount)
    dplyr::mutate(PriorQuarterMemberCount=PriorMemberCount)
}

# COMMAND ----------

# DBTITLE 1,Model - Daily
month.sum <- data.tracker %>% dplyr::filter(Date >= floor_date(now(), "month"))
end.of.month <- data.groups %>% 
  dplyr::select(Date, ForecastMemberCount, PriorMemberCount, ForecastNewMemberCount) %>%
  dplyr::filter(Date==ceiling_date(now(),"month")-days(1)) %>%
  dplyr::group_by(Date) %>%
  summarize_all(sum) %>%
  dplyr::mutate(ForecastMemberGrowth=(ForecastMemberCount-PriorMemberCount)/PriorMemberCount*100,
                ForecastRenewalRate=(ForecastMemberCount-ForecastNewMemberCount)/PriorMemberCount*100) %>%
  dplyr::select(-PriorMemberCount, -ForecastNewMemberCount)

if (nrow(month.sum)!=0) {
  if (length(which(diff(month.sum$MemberCount) < -3000)) == 0) {
    data.month.end <- data.frame("Date"=end.of.month$Date, "ForecastMemberCount"="---",
                                 "ForecastMemberGrowth"="---", "ForecastRenewalRate"=end.of.month$ForecastRenewalRate)
  } else {
    end.of.last.month <- month.sum[which(diff(month.sum$MemberCount) < -3000)+1,]
    daysoff <- which(diff(month.sum$MemberCount) < -3000)
    month.sum <- month.sum[-c(1:which(diff(month.sum$MemberCount) < -3000)),]
    month.difference <- end.of.month$ForecastMemberCount-end.of.last.month$MemberCount
    percentage <- as.numeric((day(tail(month.sum$Date, n=1))-daysoff)/(days_in_month(tail(month.sum$Date, n=1))-daysoff))
    new.month.end <- end.of.month$ForecastMemberCount + (tail(month.sum$MemberCount, n=1) - (end.of.last.month$MemberCount + month.difference*percentage))
    data.month.end <- data.frame("Date"=end.of.month$Date, "ForecastMemberCount"=new.month.end,
                                 "ForecastMemberGrowth"=end.of.month$ForecastMemberGrowth, "ForecastRenewalRate"=end.of.month$ForecastRenewalRate)
  }
} else {
  data.month.end <- data.frame("Date"=end.of.month$Date, "ForecastMemberCount"="---",
                               "ForecastMemberGrowth"="---", "ForecastRenewalRate"=end.of.month$ForecastRenewalRate)
}

month.join <- data.tracker %>% 
  dplyr::filter(Date >= floor_date(now(), "month")) %>%
  dplyr::summarize(JoinCount=sum(JoinCount))
month.join.last.year <- data.tracker %>%
  dplyr::filter(Date >= floor_date(now()-years(), "month") & Date <= tail(data.tracker$Date,1)-years()) %>%
  dplyr::summarize(JoinCount=sum(JoinCount))
data.month.end <- data.month.end %>%
  dplyr::mutate(JoinDifference=as.numeric(month.join-month.join.last.year))

# if (!is.na(data.month.end$ForecastMemberCount) != "---") {
if (data.month.end$ForecastMemberCount != "---") {
  data.groups.monthend.difference <- data.month.end$ForecastMemberCount-end.of.month$ForecastMemberCount
  data.groups.monthend.past <- data.groups %>%
    dplyr::filter(Date < today()) %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(ForecastCurrentMemberCount=ForecastMemberCount) %>%
    dplyr::select(Date, Group, ForecastCurrentMemberCount)
  data.groups.monthend.future <- data.groups %>%
    dplyr::filter(Date >= today()) %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(member.total.sum=sum(ForecastMemberCount), newmember.total.sum=sum(ForecastNewMemberCount),
                  renewedmember.total.sum=sum(ForecastRenewedMemberCount)) %>%
    ungroup() %>%
    dplyr::mutate(ForecastCurrentMemberCount=((ForecastMemberCount/member.total.sum)*data.groups.monthend.difference)+ForecastMemberCount) %>%
    dplyr::select(Date, Group, ForecastCurrentMemberCount)
} else {
  data.groups.monthend.past <- data.groups %>%
    dplyr::filter(Date < today()) %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(ForecastCurrentMemberCount=ForecastMemberCount) %>%
    dplyr::select(Date, Group, ForecastCurrentMemberCount)
  data.groups.monthend.future <- data.groups %>%
    dplyr::filter(Date >= today()) %>%
    dplyr::group_by(Date) %>%
    dplyr::mutate(ForecastCurrentMemberCount=ForecastMemberCount) %>%
    dplyr::select(Date, Group, ForecastCurrentMemberCount)
}
data.groups.monthend <- bind_rows(data.groups.monthend.past, data.groups.monthend.future)
data.groups.monthend <- data.groups.monthend %>%
  dplyr::arrange(Group, Date)
data.groups <- data.groups %>%
  left_join(data.groups.monthend, by=c("Date", "Group"))

# if (!is.na(data.month.end$ForecastMemberCount) != "---") {
if (data.month.end$ForecastMemberCount != "---") {
  end.of.month2 <- data.groups %>% 
    dplyr::select(Date, ForecastCurrentMemberCount, PriorMemberCount) %>%
    dplyr::filter(Date==ceiling_date(now(),"month")-days(1)) %>%
    dplyr::group_by(Date) %>%
    summarize_all(sum) %>%
    dplyr::mutate(ForecastMemberGrowth=(ForecastCurrentMemberCount-PriorMemberCount)/PriorMemberCount*100) %>%
    dplyr::select(-ForecastCurrentMemberCount, -PriorMemberCount)
  data.month.end <- data.month.end %>%
    dplyr::select(-ForecastMemberGrowth) %>%
    dplyr::left_join(end.of.month2, by="Date")
}

# if (!is.na(data.month.end$ForecastMemberCount) != "---") {
if (data.month.end$ForecastMemberCount != "---") {
  data.acquisition.temp <- data.month.end %>%
    dplyr::mutate(Date=as.Date(Date)) %>%
    dplyr::select(-JoinDifference) %>%
    dplyr::rename(MemberCount=ForecastMemberCount)
  data.acquisition <- month.sum %>%
    dplyr::select(-JoinCount)
  data.acquisition <- bind_rows(data.acquisition, data.acquisition.temp)
  data.acquisition <- data.acquisition %>%
    tidyr::complete(Date=seq.Date(min(Date), max(Date), by="day"))
  xout <- data.acquisition %>%
    dplyr::filter(is.na(MemberCount))
  data.acquisition.temp <- as.data.frame(approx(data.acquisition$Date, data.acquisition$MemberCount, xout=xout$Date))
  data.acquisition.temp <- data.acquisition.temp %>%
    dplyr::rename(Date=x, MemberCount=y)
  data.acquisition.temp <- rbind(tail(month.sum[,1:2], n=1), data.acquisition.temp)
  data.acquisition <- data.acquisition %>%
    dplyr::left_join(data.acquisition.temp, by="Date") %>%
    dplyr::rename(MemberCount=MemberCount.x, ForecastMemberCount=MemberCount.y)
  data.acquisition$ForecastMemberCount <- replace(data.acquisition$ForecastMemberCount, length(data.acquisition$ForecastMemberCount), tail(data.acquisition$MemberCount, n=1)) 
  data.acquisition$MemberCount <- replace(data.acquisition$MemberCount, length(data.acquisition$MemberCount), NA) 
} else {
  data.acquisition <- end.of.month %>%
    tidyr::complete(Date=seq.Date(floor_date(Date,"month"), max(Date), by="day")) %>%
    dplyr::mutate(MemberCount=0) %>%
    dplyr::mutate(ForecastMemberCount=0)
}

### WIPE PAST FORECAST DATA FOR CHARTS
data.groups.wipe.past <- data.groups %>%
  dplyr::filter(Date < dplyr::last(data.membership$Date)) %>%
  dplyr::mutate(ForecastMemberCount=NA,
                ForecastNewMemberCount=NA,
                ForecastRenewedMemberCount=NA,
                ForecastCurrentMemberCount=NA,
                ForecastCoverageMemberCount=NA,
                AdjustMemberCount=ifelse(Date < floor_date(today(), "year"), NA, AdjustMemberCount),
                AdjustNewMemberCount=ifelse(Date < floor_date(today(), "year"), NA, AdjustNewMemberCount),
                AdjustRenewedMemberCount=ifelse(Date < floor_date(today(), "year"), NA, AdjustRenewedMemberCount),
                AdjustCoverageMemberCount=ifelse(Date < floor_date(today(), "year"), NA, AdjustCoverageMemberCount),
                QuarterMemberCount=ifelse(Date < floor_date(today(), "year"), NA, QuarterMemberCount),
                QuarterNewMemberCount=ifelse(Date < floor_date(today(), "year"), NA, QuarterNewMemberCount),
                QuarterRenewedMemberCount=ifelse(Date < floor_date(today(), "year"), NA, QuarterRenewedMemberCount),
                QuarterCoverageMemberCount=ifelse(Date < floor_date(today(), "year"), NA, QuarterCoverageMemberCount))
data.groups.wipe.current <- data.groups %>%
  dplyr::filter(Date == dplyr::last(data.membership$Date)) %>%
  dplyr::mutate(ForecastRenewRatio=RenewRatio,
                ForecastMemberCount=MemberCount,
                ForecastNewMemberCount=NewMemberCount,
                ForecastRenewedMemberCount=RenewedMemberCount,
                ForecastCurrentMemberCount=MemberCount,
                ForecastCoverageMemberCount=CoverageMemberCount)
data.groups.wipe.future <- data.groups %>%
  dplyr::filter(Date > dplyr::last(data.membership$Date))
data.groups.wipe <- bind_rows(data.groups.wipe.past, data.groups.wipe.current, data.groups.wipe.future)  
data.groups <- data.groups.wipe %>%
  dplyr::arrange(Group, Date)
data.join.wipe.past <- data.join %>%
  dplyr::filter(Date<data.join[dplyr::last(which(!is.na(data.join$JoinCount))),1]) %>%
  dplyr::mutate(ForecastJoinCount=NA)
data.join.wipe.current <- data.join %>%
  dplyr::filter(Date==data.join[dplyr::last(which(!is.na(data.join$JoinCount))),1]) %>%
  dplyr::mutate(ForecastJoinCount=JoinCount)
data.join.wipe.future <- data.join %>%
  dplyr::filter(Date>data.join[dplyr::last(which(!is.na(data.join$JoinCount))),1])
data.join.wipe <- bind_rows(data.join.wipe.past, data.join.wipe.current, data.join.wipe.future)  
data.join <- data.join.wipe

# COMMAND ----------

# DBTITLE 1,Export
data.groups$Date <- as.character(data.groups$Date)
data.groups$MembershipType <- as.character(data.groups$MembershipType)
data.groups$Region <- as.character(data.groups$Region)
data.groups$Coverage <- as.character(data.groups$Coverage)
is.num <- sapply(data.groups, is.numeric)
data.groups[is.num] <- lapply(data.groups[is.num], round, digits=1)
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))
data.groups[is.nan(data.groups)] <- 0
is.infinite.data.frame <- function(x)
  do.call(cbind, lapply(x, is.infinite))
data.groups[is.infinite(data.groups)] <- NA

data.join$Date <- as.character(data.join$Date)
is.num <- sapply(data.join, is.numeric)
data.join[is.num] <- lapply(data.join[is.num], round, digits=3)
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))
data.join[is.nan(data.join)] <- NA
is.infinite.data.frame <- function(x)
  do.call(cbind, lapply(x, is.infinite))
data.join[is.infinite(data.join)] <- NA

data.join.daily$Date <- as.character(data.join.daily$Date)
is.num <- sapply(data.join.daily, is.numeric)
data.join.daily[is.num] <- lapply(data.join.daily[is.num], round, digits=3)
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))
data.join.daily[is.nan(data.join.daily)] <- NA
is.infinite.data.frame <- function(x)
  do.call(cbind, lapply(x, is.infinite))
data.join.daily[is.infinite(data.join.daily)] <- NA

data.acquisition$Date <- as.character(data.acquisition$Date)
is.num <- sapply(data.acquisition, is.numeric)
data.acquisition[is.num] <- lapply(data.acquisition[is.num], round, digits=3)
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))
data.acquisition[is.nan(data.acquisition)] <- NA
is.infinite.data.frame <- function(x)
  do.call(cbind, lapply(x, is.infinite))
data.acquisition[is.infinite(data.acquisition)] <- NA

data.month.end$Date <- as.character(data.month.end$Date)
if (is.numeric(data.month.end$ForecastMemberCount)) {
  data.month.end$ForecastMemberCount <- lapply(data.month.end$ForecastMemberCount, round, digits=0)
  data.month.end$ForecastMemberCount <- format(data.month.end$ForecastMemberCount, big.mark=',')
}
if (is.numeric(data.month.end$ForecastRenewalRate)) {
  data.month.end$ForecastRenewalRate <- lapply(data.month.end$ForecastRenewalRate, round, digits=2)
  data.month.end$ForecastRenewalRate <- paste0(data.month.end$ForecastRenewalRate,"%")
}
if (is.numeric(data.month.end$ForecastMemberGrowth)) {
  data.month.end$ForecastMemberGrowth <- lapply(data.month.end$ForecastMemberGrowth, round, digits=2)
  data.month.end$ForecastMemberGrowth <- paste0(data.month.end$ForecastMemberGrowth,"%")
}
data.month.end$JoinDifference <-
  if(data.month.end$JoinDifference>0) {
    data.month.end$JoinDifference <- format(data.month.end$JoinDifference, big.mark=',')
    data.month.end$JoinDifference<-paste0("+",data.month.end$JoinDifference)
  } else {
    data.month.end$JoinDifference <- format(data.month.end$JoinDifference, big.mark=',')
  }
data.month.end$LastUpdated <- now(tzone="America/Edmonton")
data.month.end$LastUpdated <- gsub(" 0", " ", format(data.month.end$LastUpdated,'%b %d %Y %I:%M %p'))
data.month.end$Version <- "v3.0"

# Create a copy and add LastUpdate
data.groups2 <- data.frame(data.groups)
data.join2 <- data.frame(data.join)
data.join.daily2 <- data.frame(data.join.daily)
data.acquisition2 <- data.frame(data.acquisition)
data.month.end2 <- data.frame(data.month.end)

dtm <- now(tzone="America/Edmonton")
data.groups2["LastUpdate"]  <- dtm
data.join2["LastUpdate"]  <- dtm
data.join.daily2["LastUpdate"]  <- dtm
data.acquisition2["LastUpdate"]  <- dtm
data.month.end2["LastUpdate"]  <- dtm

library(odbc)

con_ds <- dbConnect(odbc(),
                    Driver = "ODBC Driver 17 for SQL Server",
                    Server = "10.52.10.105",
                    Database = "data_science",
                    UID    = UID,
                    PWD    = PWD,
                    Port = 1433)

dbWriteTable(con_ds, "Optix_Group", data.groups, append=FALSE, overwrite=TRUE)
dbWriteTable(con_ds, "Optix_Join", data.join, append=FALSE, overwrite=TRUE)
dbWriteTable(con_ds, "Optix_Join_Daily", data.join.daily, append=FALSE, overwrite=TRUE)
dbWriteTable(con_ds, "Optix_Acquisition", data.acquisition, append=FALSE, overwrite=TRUE)
dbWriteTable(con_ds, "Optix_MonthEnd", data.month.end, append=FALSE, overwrite=TRUE)

dbWriteTable(con_ds, "Optix_Group_Hist_V3", data.groups2, append=TRUE, overwrite=FALSE)
dbWriteTable(con_ds, "Optix_Join_Hist_V3", data.join2, append=TRUE, overwrite=FALSE)
dbWriteTable(con_ds, "Optix_Join_Daily_Hist_V3", data.join.daily2, append=TRUE, overwrite=FALSE)
dbWriteTable(con_ds, "Optix_Acquisition_Hist_V3", data.acquisition2, append=TRUE, overwrite=FALSE)
dbWriteTable(con_ds, "Optix_MonthEnd_Hist_V3", data.month.end2, append=TRUE, overwrite=FALSE)

### ANNUAL AND QUARTER BUDGET UPDATE
if (format(now(),"%m-%d")=="10-31" | format(now(),"%m-%d")=="04-15" | format(now(),"%m-%d")=="07-15" | format(now(),"%m-%d")=="10-15"
    | format(now(),"%m-%d")=="01-15") {
  dbWriteTable(con_ds, "Optix_Budget", data.groups, append=FALSE, overwrite=TRUE)
  
  dbWriteTable(con_ds, "Optix_Budget_Hist_V3", data.groups2, append=TRUE, overwrite=FALSE)
}

dbDisconnect(con_ds)
