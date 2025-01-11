# ==================================================================================================
# NGOC_DEV.R - 2024-05-28 17:26
# ==================================================================================================

# rm(list = ls())
# ..................................................................................................
source("c:/R/TRAINEE_CONFIG.r", echo=T)

try(source(paste0('c:/R/', "TRAINEE_INTRO.R"),                                                echo=F))

try(source(paste0("D:/python/etf_indexes/Script/LIBRARY/TRAINEE_LIBRARY.R"),                                              echo=F))
# try(source(paste0(SDLibrary, "TRAINEE_REPORT_LIBRARY.R"),                                       echo=F))
# try(source(paste0(SDLibrary, "DBL_LIBRARY.R"),                                                  echo=F))
# try(source(paste0(SDLibrary, "DBL_VOLATILITY_LIBRARY.R"),                                       echo=F))
# try(source(paste0(SDLibrary, "IFRCBEQ_LIBRARY.R"),                                              echo=F))

# DBL_RELOAD_INSREF (ToForce = F)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# LOAD LIBRARY
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# ==================================================================================================
IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE = function(pSource = 'yah', pData = data.table()){
  # ------------------------------------------------------------------------------------------------
  xData = pData
  ParseFilter = paste0('!is.na(', tolower(pSource),')')
  ParseFields = paste0('.(codesource=', tolower(pSource), ',code, name =  etf_name, sector_code = symbol)') 
  # dt_extract = dt_raw[eval(parse(text=pCondition))]
  
  
  # ins_ref = ins_ref [active != 0]
  etf_ref = setDT(read.xlsx("D:/python/etf_indexes/List/list_etfdb.xlsx"))
  xRef = etf_ref[eval(parse(text=ParseFilter)), eval(parse(text=ParseFields))]
  
  xData = merge(xData[, -c('code','name')], xRef, all.x=T, by='codesource')
  return(xData)
}

# ==================================================================================================
IFRCBEQHG_CALCULATE_CHANGE_RT_VARPC = function(dt = data.table()){
  # ------------------------------------------------------------------------------------------------
  if (nrow(dt) > 0){
    if ('code' %in% names(dt)){
      dt = dt[order(code,date)]
      dt = unique(dt, by = c('code','date'))
      
      
      if ('close_adj' %in% names(dt)){
        dt[, change := close_adj - shift(close_adj), by = 'code']
        dt[, rt:= close_adj/shift(close_adj) - 1, by = 'code']
        dt[, varpc:= 100*rt]
      } else {
        dt[, change := close - shift(close), by = 'code']
        dt[, rt:= close/shift(close) - 1, by = 'code']
        dt[, varpc:= 100*rt]
      }
      
    } else {
      dt = dt[order(codesource,date)]
      dt = unique(dt, by = c('codesource','date'))
      
      
      if ('close_adj' %in% names(dt)){
        dt[, change := close_adj - shift(close_adj), by = 'codesource']
        dt[, rt:= close_adj/shift(close_adj) - 1, by = 'codesource']
        dt[, varpc:= 100*rt]
      } else {
        dt[, change := close - shift(close), by = 'codesource']
        dt[, rt:= close/shift(close) - 1, by = 'codesource']
        dt[, varpc:= 100*rt]
      }
    }

  }
  return (dt)
}

# ==================================================================================================
IFRCBEQHG_CLEAN_OHLC = function (pData)  {
  # ------------------------------------------------------------------------------------------------
  if(nrow(pData) > 0)
  {
    if('high' %in% names(pData)){pData[, high := as.numeric(high)]}
    if('open' %in% names(pData)){pData[, open := as.numeric(open)]}
    if('low' %in% names(pData)){pData[, low := as.numeric(low)]}
    if('close' %in% names(pData)){pData[, close := as.numeric(close)]}
    if('volume' %in% names(pData)){pData[, volume := as.numeric(volume)]}
  }
  return(pData)
}

# ==================================================================================================
IFRCBEQHG_CLEAN_TIMESTAMP = function (data) {
  # ------------------------------------------------------------------------------------------------
  if (nrow (data) >0)
  {
    if ('timestamp' %in% names (data)) { data[, timestamp := substr(as.character (timestamp), 1, 19)]}
    if ('datetime' %in% names (data)) { data[, datetime := substr(as.character (datetime), 1, 19)]}
    if ('timestamp_vn' %in% names (data)) { data[, timestamp_vn := substr(as.character (timestamp_vn), 1, 19)]}
    if ('timestamp_loc' %in% names (data)) { data[, timestamp_loc := substr(as.character (timestamp_loc), 1, 19)]}
    if ('timestamp_utc' %in% names (data)) { data[, timestamp_utc := substr(as.character (timestamp_utc), 1, 19)]}
  }
  return (data)
}

# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_ETFDB_BY_CODESOURCE = function(pCodesource = 'IBIT', pNbdays = 30){
  # ------------------------------------------------------------------------------------------------
  data = data.table()
  
  purl = paste0(
    'https://etfflows.websol.barchart.com/proxies/timeseries/queryeod.ashx?symbol=', pCodesource,
    '&data=daily&maxrecords=',pNbdays,'&volume=contract&order=asc&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration'
  )
  
  response = GET(purl, add_headers(
    `User-Agent` = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    `Accept-Language` = "en-US,en;q=0.5"
  ))
  
  response_text = content(response, as = "text", encoding = "UTF-8")
  
  lines = unlist(strsplit(response_text, "\n"))
  
  lines = lines[grep(paste0("^", pCodesource, ","), lines)]
  
  data  = fread(paste(lines, collapse = "\n"), header = FALSE)
  
  setnames(data, c("codesource", "date", "open", "high", "low", "close", "volume"))
  
  data = try(IFRCBEQHG_CLEAN_OHLC(data))
  
  
  if ('timestamp' %in% names(data)){
    data = try(IFRCBEQHG_CLEAN_TIMESTAMP(data))
  }
  data[, source := 'ETFDB']
  data = try(IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE(pSource = 'etfdb', data))
  data = try(IFRCBEQHG_CALCULATE_CHANGE_RT_VARPC(data))
  
  data = UPDATE_UPDATED(data)
  
  My.Kable(data)
  
  return(data)
}

# ==================================================================================================
IFRCBEQHG_CONVERT_NUMBER <- function(val) {
  # ------------------------------------------------------------------------------------------------
  last_char <- toupper(substr(val, nchar(val), nchar(val)))  #chuyen thanh in hoa
  num_value <- as.numeric(gsub("[^0-9.]", "", val))
  multiplier <- switch(last_char,
                       "K" = 1000,
                       "M" = 1000000,
                       "%" = 0.01,
                       "B" = 1000000000,
                       "T" = 1000000000000,
                       1)  
  
  converted_value <- num_value * multiplier
  
  return(converted_value)
}

# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_YAH_BY_CODESOURCE = function(pCodesource="UYG", pInterval="1d", pNbdays = 60, ToKable=T, Hour_adjust = 0) {
  # ------------------------------------------------------------------------------------------------
  # pCodesource="^GSPC"; pInterval="5m"; ToKable=T; Hour_adjust = -4
  MyURL       = paste0("https://query1.finance.yahoo.com/v8/finance/chart/", pCodesource, "?region=US&lang=en-US&includePrePost=false&interval=", 
                       pInterval, "&range=",pNbdays,"d&corsDomain=finance.yahoo.com&.tsrc=finance")
  rm(dt_json)
  x.combined  = data.table()
  dt_json  = jsonlite::fromJSON(MyURL)
  
  if (exists("dt_json"))
  {
    x = as.data.table(dt_json[[1]]$result$meta)
    hour_adjust = dt_json[[1]]$result$meta$gmtoffset/3600
    if (nchar (hour_adjust > 0)) {
      Hour_adjust = hour_adjust
    }
    # str(x)
    rCodesource = x[1]$symbol
    DATACENTER_LINE_BORDER(rCodesource)
    xtr = data.table(field = names(x), t(x))
    # My.Kable(xtr, Nb=nrow(xtr), HeadOnly = T)
    
    x = as.data.table(dt_json[[1]]$result$meta)
    
    x.timestamp  = as.data.table(dt_json[[1]]$result$timestamp)
    x.timestamp[, ID:=seq.int(1, nrow(x.timestamp))]
    
    if ("V1" %in% colnames(x.timestamp)) {setnames(x.timestamp, "V1", "timestamp")}
    
    x.quote      = dt_json[[1]]$result$indicators$quote
    # x.quote[[1]]$open
    
    x.open       = as.data.table(x.quote[[1]]$open)
    if (nrow(x.open)>2)
    {
      x.open[, ID:=seq.int(1, nrow(x.open))]
      setnames(x.open, "V1", "open")
      
      x.high       = as.data.table(x.quote[[1]]$high)
      x.high[, ID:=seq.int(1, nrow(x.high))]
      setnames(x.high, "V1", "high")
      
      x.low       = as.data.table(x.quote[[1]]$low)
      x.low[, ID:=seq.int(1, nrow(x.low))]
      setnames(x.low, "V1", "low")
      
      x.close       = as.data.table(x.quote[[1]]$close)
      x.close[, ID:=seq.int(1, nrow(x.close))]
      setnames(x.close, "V1", "close")
      
      x.closeadj  = as.data.table(unlist(dt_json[[1]]$result$indicators$adjclose))
      x.closeadj[, ID:=seq.int(1, nrow(x.closeadj))]
      if ("V1" %in% names(x.closeadj))
      {
        setnames(x.closeadj, "V1", "close_adj")
      }else{
        x.closeadj[, close_adj:=as.numeric(NA)]
      }
      
      x.volume       = as.data.table(x.quote[[1]]$volume)
      x.volume[, ID:=seq.int(1, nrow(x.volume))]
      setnames(x.volume, "V1", "volume")
      
      # x.merge     = merge(x.timestamp, x.close, by="ID")
      # My.Kable(x.merge)
      
      x.combined = cbind(x.timestamp, x.open[, .(open)],  x.high[, .(high)],  x.low[, .(low)], x.close[, .(close)], x.closeadj[, .(close_adj)], x.volume[, .(volume)])
      # My.Kable(x.combined)
      x.combined[, datetime:=as.POSIXct(as.numeric(as.character(timestamp)),origin="1970-01-01",tz="GMT")]
      x.combined <- x.combined %>%   select("datetime", everything())
      x.combined[, codesource:=rCodesource]
      x.combined[, source:="YAH"]
      x.combined <- x.combined %>%   select("codesource", everything())
      
      DBL_RELOAD_INSREF ()
      insyah_ref = ins_ref[!is.na(yah)]
      x.combined = transform(x.combined, code=insyah_ref$code[match(codesource, insyah_ref$yah)])
      # x.combined = DBL_UPDATE_TRANSFORM(x.combined, "YAH")
      x.combined = IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE (pSource='yah', x.combined)
      x.combined[, date:=as.Date(datetime)]
      x.combined = x.combined[, -c("timestamp", "ID")]
      
      # x.combined[, ':='(timestamp=as.character(datetime), datetime=as.character(datetime))]
      x.combined[, timestamp_loc := datetime + Hour_adjust*60*60]
      x.combined[, timestamp_vn := datetime + 7*60*60]
      x.combined[, datetime := timestamp_loc]
      x.combined[, timestamp := timestamp_loc]
      x.combined[, date:=as.Date(datetime)]
      x.combined[, timestamp_loc := NULL]
      x.combined[, cur := x$currency]
      
      pURL = paste0('https://finance.yahoo.com/quote/',pCodesource,'/')
      response   = GET(pURL, add_headers(
        `User-Agent`      = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
        ,`Accept-Language`= "en-US,en;q=0.5"
      ))
      
      xweb         = content(response, as = "text", encoding = "UTF-8")
      content    = try(rvest::read_html(xweb))
      capi = html_nodes(content,
                        xpath = '//*[@id="nimbus-app"]/section/section/section/article/div[2]/ul/li[9]/span[2]')
      
      value = xml_text(capi)
      if (length(value) > 0 && nchar(value) > 0){
        capi = IFRCBEQHG_CONVERT_NUMBER(as.vector(gsub("^\\s+|\\s+$",'',value)))
      } else {
        capi = NA
      }
      x.combined[, capi := capi]
      x.combined = IFRCBEQHG_CLEAN_OHLC(x.combined)
      x.combined = IFRCBEQHG_CLEAN_TIMESTAMP(x.combined)
      x.combined = try(IFRCBEQHG_CALCULATE_CHANGE_RT_VARPC(x.combined))
      
      x.combined = UPDATE_UPDATED(x.combined)
      if (pInterval=="1d") { x.combined = unique(x.combined[order(timestamp)], by = "date") }
      if (ToKable) { My.Kable.MaxCols(x.combined)}
    }
  }
  return(x.combined)
}

# ==================================================================================================
IFRCBEQHG_SMOOTH = function (pOption = "after", pdata = data.table(), dataset = "close", toreplace = F,
                             by = "code", column = "timestamp_vn") {
  #-------------------------------------------------------------------------------------------------
  #by = "codesource"
  #column = "date"
  #column = "timestamp"
  #pOption = "after/before"
  
  if (nrow(pdata) > 0 & column %in% names(pdata)) {
    pdata = CLEAN_COLNAMES(pdata)
    dataset = tolower(dataset)
    pdata = pdata[order(get(column))]
    
    if (dataset %in% names(pdata) & by %in% names(pdata)) {
      if (length(by) > 0) {
        if (pOption == "after") {
          
          pdata[, (paste0("sm_", dataset)) := {
            
            non_leading <- !is.na(get(dataset)) | cumsum(!is.na(get(dataset))) > 0
            filled <- ifelse(non_leading, na.locf(get(dataset), na.rm = FALSE), NA)
            rep_len(filled, .N) 
          }, by = by]
        } else {
          
          pdata[, (paste0("sm_", dataset)) := {
            filled <- na.locf(na.locf(get(dataset), na.rm = FALSE), fromLast = TRUE)
            rep_len(filled, .N)  
          }, by = by]
        }
        
        if (toreplace) {
          pdata[is.na(get(dataset)), (dataset) := get(paste0("sm_", dataset))]
          pdata[, paste0("sm_", dataset) := NULL]
        }
      } else {
        if (pOption == "before") {
          
          pdata[, (paste0("sm_", dataset)) := {
            non_leading <- !is.na(get(dataset)) | cumsum(!is.na(get(dataset))) > 0
            filled <- ifelse(non_leading, na.locf(get(dataset), na.rm = FALSE), NA)
            filled
          }]
        } else {
          
          pdata[, (paste0("sm_", dataset)) := {
            filled <- na.locf(na.locf(get(dataset), na.rm = FALSE), fromLast = TRUE)
            filled
          }]
        }
        
        if (toreplace) {
          pdata[is.na(get(dataset)), (dataset) := get(paste0("sm_", dataset))]
          pdata[, paste0("sm_", dataset) := NULL]
        }
      }
    } else {
      CATln(paste0("Invalid column: ", dataset, ", ", by))
    }
  } else {
    CATln(paste0("Invalid table or column: ", column))
  }
  
  return(pdata)
  
}


# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_EIK_BY_CODESOURCE = function(pCodesource = "REE.HM", start = "1920-12-31", end = Sys.Date(), ToKable = T) {
  # ------------------------------------------------------------------------------------------------
  CATln_Border(paste('DOWNLOAD EIK = ', pCodesource))
  dt = try(setDT(rd_GetHistory(RD, universe = pCodesource, start = start, end = end,
                               fields = c("TR.SharesListed", "TR.SharesFreeFloat","TR.SharesOutstanding", 
                                          "TR.CompanyMarketCap", "TR.Volume", "TR.DailyValueTraded", "TR.PriceClose.Currency",
                                          "TR.ClosePrice(Adjusted=0)", "TR.ClosePrice(Adjusted=1)", "TR.TotalReturn1D"))))
  if (all(class(dt)!="try-error"))
  {
    dt = dt[,.(source = "EIK", codesource = Instrument, date = Date, shareslis = as.numeric(TR.SHARESLISTED),
               share_outstanding = as.numeric(TR.SHARESOUTSTANDING), float = as.numeric(TR.SHARESFREEFLOAT),
               close = as.numeric(`TR.CLOSEPRICE(ADJUSTED=0)`), close_adj = as.numeric(`TR.CLOSEPRICE(ADJUSTED=1)`),
               rt = as.numeric(TR.TOTALRETURN1D)/100, capi = as.numeric(TR.COMPANYMARKETCAP), volume = as.numeric(TR.VOLUME),
               turnover = as.numeric(TR.DAILYVALUETRADED), cur = TR.PRICECLOSE.CURRENCY)]
    dt = dt[!is.na(volume) & !is.na(close)]
    dt[is.na(share_outstanding) & !is.na(shareslis), share_outstanding:=shareslis]
    dt = IFRCBEQHG_SMOOTH(pOption = "after", pdata = dt, dataset = "share_outstanding", toreplace = T, by = "codesource", column = "date")
    dt = IFRCBEQHG_SMOOTH(pOption = "after", pdata = dt, dataset = "shareslis", toreplace = T, by = "codesource", column = "date")
    
    dt = IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE('eik', dt)
    if (ToKable) { My.Kable(dt)}
    
    # dt_ref = CCPR_READRDS("S:/CCPR/DATA/EIK/","STK_GICS_ICB.rds")
    # dt = merge(dt, dt_ref[,.(codesource = `Identifier.(RIC)`, name = toupper(Company.Name), market = `Exchange.Market.Identifier.Code`)],
    #            by = "codesource", all.x=T)
  }
  return(dt)
}

# ==================================================================================================
CHECK_CLASS = function(pData) {
  # ------------------------------------------------------------------------------------------------
  rData = data.table()
  if (all(class(pData)!='try-error') && nrow(pData)>0)
  {
    rData = pData
  }
  return(rData)
}

# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_LIST = function(list_code = list(), pSource = 'yah', Nbdays = ''){
  # ------------------------------------------------------------------------------------------------
  final_data = data.table()
  xList = list()
  
  if (length(list_code) > 0){
    for (i in 1:length(list_code)){
      dt_download = data.table()
      
      Codesource = list_code[[i]]
      
      switch(pSource,
             'yah' = {
               if (nchar(Nbdays) == 0){
                 Nbdays = 10000
               }
               dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_YAH_BY_CODESOURCE(pCodesource = Codesource, pInterval="1d", pNbdays = Nbdays, ToKable=T, Hour_adjust = 0)))
             },
             'etfdb' = {
               dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_ETFDB_BY_CODESOURCE(pCodesource = Codesource, pNbdays = Nbdays)))
             },
             'eik' = {
               start_date = if (nchar(Nbdays) == 0) {
                 "1920-12-31"
               } else {
                 as.character(Sys.Date() - as.numeric(Nbdays))
               }
               
               
               dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_EIK_BY_CODESOURCE(pCodesource = Codesource, start = start_date, end = Sys.Date(), ToKable = T)))
             },
             'nasdaq' = {
               if (nchar(Nbdays) == 0){
                 Nbdays = 100000
               }
               dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_NASDAQ_BY_CODESOURCE(pCodesource = Codesource, pNbdays = Nbdays)))
             })
      
      if (nrow(dt_download) > 0){
        dt_download = IFRCBEQHG_CLEAN_OHLC(dt_download)
        dt_download = IFRCBEQHG_CLEAN_TIMESTAMP(dt_download)
        xList[[i]] = dt_download
      } else {
        file_code_not_correct = CHECK_CLASS(try(read.xlsx('D:/python/etf_indexes/List/list_codesource_not_correct.xlsx')))
        etf_ref = setDT(read.xlsx("D:/python/etf_indexes/List/list_etfdb.xlsx"))
        
        dt_not_correct = data.table(source = pSource, codesource = Codesource)
        dt_not_correct = try(IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE(pSource, dt_not_correct))
        file_code_not_correct = rbind(dt_not_correct, file_code_not_correct,fill = T)
        file_code_not_correct = UPDATE_UPDATED(file_code_not_correct)
        write.xlsx(file_code_not_correct, 'D:/python/etf_indexes/List/list_codesource_not_correct.xlsx')
      }
      
    }
    final_data = try(rbindlist(xList, fill = T))
  }
  
  return(final_data)
}

# ==================================================================================================
IFRCBEQHG_GET_LIST_DOWNLOAD = function(pOption = '') {
  # ------------------------------------------------------------------------------------------------
  list_etf = setDT(read.xlsx('H:/List/list_etfdb.xlsx'))
  my_list_etf = data.table()
  if (nchar(pOption) != 0 && pOption != 'TOP') {
    my_list_etf = list_etf[symbol == pOption]
  } else {
    if(pOption == 'TOP')
    {
      my_list_etf = list_etf[top == 1]
    }else{
      my_list_etf = list_etf
      
    }
  }
  return(my_list_etf)
}

#===================================================================================================
IFRCBEQHG_SPLIT_DATA_TABLE = function(dt, rows_per_split = 20) {
  # ------------------------------------------------------------------------------------------------
  split_list = split(dt, (seq_len(nrow(dt)) - 1) %/% rows_per_split + 1)
  return(split_list)
}

# ==================================================================================================
DATACENTER_LINE_BORDER = function(l.msg="hello", l.justify="centre", clearscreen=F, NbCarLine = 125) {
  # ------------------------------------------------------------------------------------------------
  if (clearscreen) {shell("cls")}
  CATln(strrep("=",NbCarLine))
  CATln(format(l.msg, width=NbCarLine, justify=l.justify))
  CATln(strrep("=",NbCarLine))
}


# ==================================================================================================
IFRCBEQHG_WRITE_SUMMARY = function(pData=data.table(), pFileName, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T){
  # ------------------------------------------------------------------------------------------------
  # pData = data
  # pFileName = paste0(pFolder,'ifrcbeq_etf_',source,'_history.csv')
  if (nrow(pData)>0)
  {
    x.rds = pData
  } else {
    # x.rds     = try(read.csv(pFileName))
    x.rds     = try(readRDS(pFileName))
  }
  if (all(class(x.rds)!='try-error') && nrow(x.rds)>0)
  {
    if (ToPrompt) {My.Kable.Min(x.rds)}
    if (ToPrompt) {str(x.rds)}
    if (!"codesource" %in% colnames(x.rds) & "ticker" %in% colnames(x.rds)) { 
      x.rds[, codesource:=as.character(ticker)]} 
    if (!"codesource" %in% colnames(x.rds)) { x.rds[, codesource:=NA]} else{ x.rds[, codesource:=as.character(codesource)]}
    if (!"source" %in% colnames(x.rds)) { x.rds[, source:=NA]} else{ x.rds[, source:=as.character(source)]}
    if (!"close" %in% colnames(x.rds)) { x.rds[, close:=as.numeric(NA)]}
    if ("code" %in% colnames(x.rds) & "codesource" %in% colnames(x.rds) & nrow(x.rds[!is.na(code)])==0) { 
      x.rds[, code:=as.character(codesource)]} 
    if (!"updated" %in% colnames(x.rds)) { 
      x.rds[, updated:=SYS.TIME()]} else {
        if (nrow (x.rds [!is.na (updated)]) ==0 ){x.rds [, updated := SYS.TIME()]} else {
          x.rds [is.na(updated), updated := SYS.TIME()]
        }
      }
    # My.Kable (x.rds [!is.na(updated),.(code, date, name, updated)])
    # My.Kable (x.rds [is.na(updated),.(code, date, name, updated)])
    
    setkey(x.rds, code, date)
    
    # my.data[,":="(nbdays=append(NA,diff(date))),by=list(code)]
    if (ToExclude)
    {  
      if ('timestamp_loc' %in% names(x.rds))
      {
        my.summary = x.rds[date<=Sys.Date(),.(start=date[1], date=date[.N], nb=.N, datelast=date[.N], last=close[.N], 
                                              nbdays=as.numeric(NA), source=source[.N], name="", type="", 
                                              codesource=codesource[.N],timestamp_loc = timestamp_loc[.N], updated = updated[.N]), by='code']
      } else {
        my.summary = x.rds[date<=Sys.Date(),.(start=date[1], date=date[.N], nb=.N, datelast=date[.N], last=close[.N], 
                                              nbdays=as.numeric(NA), source=source[.N], name="", type="", 
                                              codesource=codesource[.N], updated = updated[.N]), by='code']
      }
      
    } else {
      
      if ('timestamp_loc' %in% names(x.rds))
      {
        my.summary = x.rds[,.( 
          start=date[1], date=date[.N], nb=.N, datelast=date[.N], 
          last=close[.N], nbdays=as.numeric(NA), source=source[.N],
          name="", type="", codesource=codesource[.N],timestamp_loc = timestamp_loc[.N],updated = updated[.N]), by='code']
      } else {
        my.summary = x.rds[,.( 
          start=date[1], date=date[.N], nb=.N, datelast=date[.N], 
          last=close[.N], nbdays=as.numeric(NA), source=source[.N],
          name="", type="", codesource=codesource[.N],updated = updated[.N]), by='code']
      }
    }
    
    
    # DBL_RELOAD_INSREF()
    list_etf = setDT(read.xlsx('H:/List/list_etfdb.xlsx'))
    
    my.summary = merge(my.summary[, -c('name')], list_etf[, .(code, name=etf_name)], all.x=T, by='code')
    my.summary = my.summary[order(-date, code)]
    if (ToPrompt) {My.Kable.TB(my.summary)}
    
    my.FileTXT  = gsub(".rds", "_summary.txt", pFileName, ignore.case=T)
    CATln(my.FileTXT)
    my.summary = my.summary[order(-date)]
    # my.summary = merge(my.summary[, -c("name", "type", "fcat")], ins_ref[, .(code, name=short_name, type, fcat)], by='code', all.x=T)
    # my.summary = transform(my.summary, type=ins_ref$type[match(code, ins_ref$code)])
    
    # if (AddName) { 
    #   my.summary = transform(my.summary, name=ins_ref$short_name[match(code, ins_ref$code)])
    #   }
    if (ToPrompt) {My.Kable.TB(my.summary)}
    if (ToRemoveCodeNA) { my.summary= my.summary[!is.na(code)] }
    fwrite(my.summary, my.FileTXT, col.names = T, sep="\t", quote = F)
    
    DATACENTER_LINE_BORDER(paste(my.FileTXT, ":", 
                                 min(my.summary[!is.na(start)]$start), 'to', max(my.summary[!is.na(date)]$date),
                                 ">>>", trimws(FormatNS(nrow(my.summary),0,12,"")), "/", 
                                 trimws(FormatNS(sum(my.summary$nb),0,16,"")), " = at", substr(as.character(Sys.time()),12,16)))
    return(my.summary)
  }
  
}


# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_SOURCE = function(pFolder = 'H:/Data/data_raw/', source = 'yah', list_todo = list()){
  # ------------------------------------------------------------------------------------------------
  
  list_code_not_download = list()
  data_new               = data.table()
  list_download          = list_todo
  
  file_history         = paste0(pFolder,'ifrcbeq_etf_',source,'_history.rds')
  file_day             = paste0(pFolder,'ifrcbeq_etf_',source,'_prices.rds')
  file_date            = paste0(pFolder,'ifrcbeq_etf_',source,'_prices_',gsub('-','',Sys.Date()),'.rds')
  
  file_history_summary = paste0(pFolder,'ifrcbeq_etf_',source,'_history_summary.txt')
  
  file_code_not_correct = CHECK_CLASS(try(read.xlsx('D:/python/etf_indexes/List/list_codesource_not_correct.xlsx')))
  
  if (file.exists(file_history_summary)){
    dt_summary = setDT(fread(file_history_summary))
    
    list_code_not_download = setdiff(list_download, dt_summary$codesource)
    
    list_code_not_enough   = dt_summary[nb <= 100]$codesource
    list_code_not_download = c(list_code_not_download, list_code_not_enough)
    
    list_code_not_download = list(setdiff(unlist(list_code_not_download), unlist(file_code_not_correct[source == source]$codesource)))[[1]]
    
    if (length(list_code_not_download) > 0){
      data_new = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_LIST (list_code = list_code_not_download, pSource = source, Nbdays = '')))
    }
    
    nbDay_download = 30
  } else {
    nbDay_download = ''
  }
  
  data = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_LIST (list_code = list(setdiff(unlist(list_download), unlist(list_code_not_download)))[[1]], pSource = source, Nbdays = nbDay_download)))
  
  data = unique(rbind(data, data_new, fill = T), by = c('code','date'))
  
  # RBIND DATE
  data_old = CHECK_CLASS(try(setDT(readRDS(file_date))))
  if (nrow(data_old) > 0){
    data_old = IFRCBEQHG_CLEAN_OHLC(data_old)
    data_old = IFRCBEQHG_CLEAN_TIMESTAMP(data_old)
    data_old[, date := as.Date(date)]
    data[, date := as.Date(date)]
    
    data_old[, updated := as.character(updated)]
    data_old = unique(rbind(data, data_old, fill = T), by = c('code','date'))
    data_old = data_old[order(code,date)]
    
    # fwrite(data, file_day)
    # fwrite(data, file_date)
    try(saveRDS(data_old, file_day))
    try(saveRDS(data_old, file_date))
    try(IFRCBEQHG_WRITE_SUMMARY(pData = data_old, file_day))
  } else {
    try(saveRDS(data_old, file_day))
    try(saveRDS(data_old, file_date))
    try(IFRCBEQHG_WRITE_SUMMARY(pData = data_old, file_day))
  }
  
  # RBIND HISTORY
  if (file.exists(file_history)){
    file_info       = file.info(file_history)
    current_time    = Sys.time()
    file_mtime      = file_info$mtime
    time_diff_hours = as.numeric(difftime(current_time, file_mtime, units = "hours"))
    if (time_diff_hours >= 0) {
      data_history = CHECK_CLASS(try(setDT(readRDS(file_history))))
      data_history[, date := as.Date(date)]
      data_history[, updated := as.character(updated)]
      data_history = IFRCBEQHG_CLEAN_OHLC(data_history)
      data_history = IFRCBEQHG_CLEAN_TIMESTAMP(data_history)
      data_history = unique(rbind(data_history, data, fill = T), by = c('code','date'))
      
      # try(fwrite(data_history, file_file_historyhistory))
      try(saveRDS(data_history, file_history))
      try(IFRCBEQHG_WRITE_SUMMARY(pData = data_history, file_history))
      
    } 
  } else {
    data_history = data_old
    try(saveRDS(data_history, file_history))
    try(IFRCBEQHG_WRITE_SUMMARY(pData = data, file_history))
  }
  
  return (data)
}

# IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_OPTION (Folder = 'H:/Monitor/', pOption = '', list_source = list('YAH','ETFDB','NASDAQ'))

# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_OPTION = function(Folder = 'H:/Data/data_raw/', pOption = '', list_source = list( 'NASDAQ')){
  # ------------------------------------------------------------------------------------------------
  pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
  
  list_code = try(IFRCBEQHG_GET_LIST_DOWNLOAD(pOption))
  
  list_todo = try(IFRCBEQHG_SPLIT_DATA_TABLE(list_code, ifelse(nrow(list_code) >= 50, 50, nrow(list_code))))
  
  
  for (i in 1:length(list_todo)){
    # i = 1
    list_i = list_todo[[i]]
    
    
    for (k in 1:length(list_source)){
      # k = 1
      list_download = list()
      pSource = tolower(list_source[[k]])
      
      if (tolower(pSource) %in% colnames(list_i)) {
        list_download = list_i[[tolower(pSource)]]
        list_download = list_download[!is.na(list_download)]
        print(list_download)
      } else {
        message(sprintf("Column '%s' not found in list_todo[[%d]]", pSource, i))
      }
      
      is_blocked = tryCatch({
        IFRCBEQHG_CHECK_DOWNLOAD_PC_BY_SOURCE(toupper(pSource))
      }, error = function(e) {
        message(sprintf("Error checking source '%s': %s", pSource, e$message))
        TRUE
      })
      
      if (is_blocked) {
        message(sprintf("Source '%s' is blocked. Skipping...", pSource))
        next
      }
      
      
      if (length(list_download) > 0){
        if (pSource == 'eik' & pMyPC %in% list('BEQ-INDEX')){
          
          library(Refinitiv)
          # if (!exists('RD')){}
          RD <- RDConnect(application_id =  "0e5ab9d05f624c0eb37fbf17af88d96502cabda8",  PythonModule = "RD")
          
          dt = try(IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_SOURCE (pFolder = Folder, source = 'eik', list_todo = list_download))
        } else {
          if (pSource != 'eik'){
            lock_file = paste0('H:/Monitor/',pSource,"_waiting_", i, ".lock")
            
            if (file.exists(lock_file)) {
              file_info = file.info(lock_file)
              lock_age  = as.numeric(Sys.time() - file_info$mtime, units = "secs")
              
              if (lock_age > 300) {
                file.remove(lock_file)
              }
            }
            
            if (!file.exists(lock_file) && file.create(lock_file)) {
              
              dt = try(IFRCBEQHG_DOWNLOAD_PRICES_ETF_BY_SOURCE (pFolder = Folder, source = pSource, list_todo = list_download))
              
              file.remove(lock_file)
            } 
          }
          
        }
      }
      
    }
  }
}

# ==================================================================================================
IFRCBEQHG_DOWNLOAD_PRICES_NASDAQ_BY_CODESOURCE = function(pCodesource = 'IBIT', pNbdays = 100000){
  # ------------------------------------------------------------------------------------------------
  data = data.table()
  
  purl = paste0('https://api.nasdaq.com/api/quote/',pCodesource,'/historical?assetclass=etf&fromdate=1900-01-01&limit=',pNbdays)
  
  response = GET(purl, add_headers(
    `User-Agent` = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    `Accept-Language` = "en-US,en;q=0.5"
  ))
  
  content = content(response, as = "text", encoding = "UTF-8")
  json_data = fromJSON(content, flatten = TRUE)
  
  if ("data" %in% names(json_data) && "tradesTable" %in% names(json_data$data)) {
    data = data.table(json_data$data$tradesTable$rows)
  }
  
  if (nrow(data) > 0){
    data[, date := as.Date(date, format = "%m/%d/%Y")]
    data[, volume := as.numeric(gsub(",", "", volume))] 
    data = try(IFRCBEQHG_CLEAN_OHLC(data))
    
    if ('timestamp' %in% names(data)){
      data = try(IFRCBEQHG_CLEAN_TIMESTAMP(data))
    }
    data[, codesource := pCodesource]
    data[, source := 'NASDAQ']
    
    data = try(IFRCBEQHG_MERGE_REF_ETFDB_BY_CODESOURCE(pSource = 'nasdaq', data))
    data = try(IFRCBEQHG_CALCULATE_CHANGE_RT_VARPC(data))
    
    data = UPDATE_UPDATED(data)
    
    My.Kable(data)
  }
  
  return(data)
}

#===================================================================================================
IFRCBEQHG_CHECK_DOWNLOAD_PC_BY_SOURCE = function (pSource = 'YAH')  {
  # ------------------------------------------------------------------------------------------------
  TO_DO = F
  xData = data.table()
  switch (pSource,
          'YAH' = {
            Codesource = 'IBIT'
            dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_YAH_BY_CODESOURCE(pCodesource = Codesource, pInterval="1d", pNbdays = 1000, ToKable=T, Hour_adjust = 0)))
          },
          'EIK' = {
            Codesource = "AIEQ.K"
            dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_EIK_BY_CODESOURCE(pCodesource = Codesource, start = "1920-12-31", end = Sys.Date(), ToKable = T)))
          },
          'ETFDB' = {
            Codesource = 'IBIT'
            dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_ETFDB_BY_CODESOURCE(pCodesource = Codesource, pNbdays = 100)))
          },
          'NASDAQ' = {
            Codesource = 'IBIT'
            dt_download = CHECK_CLASS(try(IFRCBEQHG_DOWNLOAD_PRICES_NASDAQ_BY_CODESOURCE(pCodesource = Codesource, pNbdays = 100)))
          }
  )
  
  if (nrow (dt_download) > 3)
  { TO_DO = T }
  
  CATln_Border(paste ('CHECK DOWNLOAD: SOURCE = ',pSource, '|', "RESULT = ", as.character(TO_DO)))
  return(TO_DO)
}
# task scheduler
# https://youtu.be/UDKy5_SQy2o