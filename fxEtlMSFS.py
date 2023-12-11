import pgpy
import os
import zipfile

import random
import string
import pandas as pd
import datetime as dt

import pyodbc
import sqlalchemy as db
from urllib import parse


def LoadRowsToDatabase(sSecName, dtAsOfDate, sAccount, sTopLevelTag, sStrategy, sBbgShortCode, sCcyCode, sSector, sAssetClass, sHedgeCore, sPositionType, sCustodian, fLS_Exposure, fQuantity, fQuantityStart, fQuantChange, fCost, fPrice, fPriceNat, fAvgUnitCostNat, fPriceUnderly, fMktValueGross, fEquityDeltaExpGross, fDeltaExpGross, fDelta, fDtdPnlUsd, fMtdPnlUsd, fYtdPnlUsd, sIssuerCode, sIssuerName, sIssuerSymbol, sSEDOL, sISIN, sCUSIP, sBbgCode, sSectorGICs, sTicker, sUnderlyCUSIP, sUnderlySYMBOL, sThesisType, sCountryCode, sIndustryGICs, sSecDescShort):
  conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
  prms = parse.quote_plus(conn_str)
  eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

  query = """
    DECLARE @out int;
    EXEC [dbo].[p_UpdateInsertAdminPosData] @SecName = :p1,
                                            @AsOfDate = :p2,
                                            @Account = :p3,
                                            @TopLevelTag = :p4,
                                            @Strategy = :p5,
                                            @BbgShortCode = :p6,
                                            @CcyCode = :p7,
                                            @Sector = :p8,
                                            @AssetClass = :p9,
                                            @HedgeCore = :p10,
                                            @PositionType = :p11,
                                            @Custodian = :p12,
                                            @LS_Exposure = :p13,
                                            @Quantity = :p14,
                                            @QuantityStart = :p15,
                                            @QuantChange = :p16,
                                            @Cost = :p17,
                                            @Price = :p18,
                                            @PriceNat = :p19,
                                            @AvgUnitCostNat = :p20,
                                            @PriceUnderly = :p21,
                                            @MktValueGross = :p22,
                                            @EquityDeltaExpGross = :p23,
                                            @DeltaExpGross = :p24,
                                            @Delta = :p25,
                                            @DtdPnlUsd = :p26,
                                            @MtdPnlUsd = :p27,
                                            @YtdPnlUsd = :p28,
                                            @IssuerCode = :p29,
                                            @IssuerName = :p30,
                                            @IssuerSymbol = :p31,
                                            @SEDOL = :p32,
                                            @ISIN = :p33,
                                            @CUSIP = :p34,
                                            @BbgCode = :p35,
                                            @SectorGICs = :p36,
                                            @Ticker = :p37,
                                            @UnderlyCUSIP = :p38,
                                            @UnderlySYMBOL = :p39,
                                            @ThesisType = :p40,
                                            @CountryCode = :p41,
                                            @IndustryGICs = :p42,
                                            @SecDescShort = :p43;
    SELECT @out AS the_output;
    """
  
  params = dict(p1= sSecName, p2=dtAsOfDate, p3=sAccount, p4=sTopLevelTag, p5=sStrategy, p6=sBbgShortCode, p7=sCcyCode, p8=sSector, p9=sAssetClass, p10=sHedgeCore, p11=sPositionType, p12=sCustodian, p13= fLS_Exposure, p14=fQuantity, p15=fQuantityStart, p16=fQuantChange, p17=fCost, p18=fPrice, p19=fPriceNat, p20=fAvgUnitCostNat, p21=fPriceUnderly, p22=fMktValueGross, p23=fEquityDeltaExpGross, p24=fDeltaExpGross, p25= fDelta, p26=fDtdPnlUsd, p27=fMtdPnlUsd, p28=fYtdPnlUsd, p29=sIssuerCode, p30=sIssuerName, p31=sIssuerSymbol, p32=sSEDOL, p33=sISIN, p34=sCUSIP, p35=sBbgCode, p36=sSectorGICs, p37= sTicker, p38=sUnderlyCUSIP, p39=sUnderlySYMBOL, p40=sThesisType, p41=sCountryCode, p42=sIndustryGICs, p43=sSecDescShort)
  
  with eng.connect() as conn:
    result = conn.execute(db.text(query), params).scalar()
    conn.commit()
    conn.close()
    print("".join(["Persisting row: ", dtAsOfDate, " | ", sSecName, " | ", sBbgShortCode,]))

def LoadPerformanceData(dtAsOfDate, sEntity, fReturn):
  conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
  prms = parse.quote_plus(conn_str)
  eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

  query = """
  DECLARE @out int;
  EXEC [dbo].[p_UpdateInsertPerfData] @AsOfDate = :p1, @Entity = :p2, @DailyReturn = :p3;
  SELECT @out AS the_output;
  """
  
  params = dict(p1=dtAsOfDate, p2=sEntity, p3=fReturn)

  with eng.connect() as conn:
    result = conn.execute(db.text(query), params).scalar()
    conn.commit()
    conn.close()
    print(dtAsOfDate + " >" + sEntity + " >" + str(fReturn))

def LoadFundAssetData(dtAsOfDate, sEntity, fAssetValue):
  conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
  prms = parse.quote_plus(conn_str)
  eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

  query = """
  DECLARE @out int;
  EXEC [dbo].[p_UpdateInsertFundAssetsData] @AsOfDate = :p1, @Entity = :p2, @FundAssets = :p3;
  SELECT @out AS the_output;
  """
  
  params = dict(p1=dtAsOfDate, p2=sEntity, p3=fAssetValue)
  
  with eng.connect() as conn:
    result = conn.execute(db.text(query), params).scalar()
    conn.commit()
    conn.close()
    print(dtAsOfDate + " >" + sEntity + " >" + str(fAssetValue))

def ClearAsOfAdminPositionData(dtAsOfDate):
  conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes'  
  prms = parse.quote_plus(conn_str)
  eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

  query = """
    DECLARE @out int;
    EXEC [dbo].[p_ClearAsOfAdminPositions] @AsOfDate = :p1;
    SELECT @out AS the_output;
    """
  
  params = dict(p1= dtAsOfDate)
  
  with eng.connect() as conn:
    result = conn.execute(db.text(query), params).scalar()
    conn.commit()
    conn.close()
    print(" Clearing existing admin position data for AsOfDate:  ".join([dtAsOfDate]))

def SendMsgToDb(sMsg, iPri, sCat, dtDate):
  conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
  prms = parse.quote_plus(conn_str)
  eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

  query = """
  DECLARE @out int;
  EXEC [dbo].[p_SetMsgQueueValues] @MsgValue = :p1, @MsgPriority = :p2, @MsgCatagory = :p3, @MsgInTs = :p4;
  SELECT @out AS the_output;
  """
  params = dict(p1=sMsg, p2=iPri, p3=sCat, p4=dtDate.strftime(r'%m/%d/%y %H:%M:%S'))
  with eng.connect() as conn:
    result = conn.execute(db.text(query), params).scalar()
    conn.commit()
    conn.close()
    print(sMsg + " >" + iPri + " >" + sCat)


def RunEtlMsfs():
  directory_in_str = "C:/Users/LeeKafafian/Allostery Investments LP/Trading - Documents/Data/MSFS/downloads/"
  directory_arc_str = "C:/Users/LeeKafafian/Allostery Investments LP/Trading - Documents/Data/MSFS/downloads/archive/"
  directory_out_str = "C:/Users/LeeKafafian/Allostery Investments LP/Trading - Documents/Data/MSFS/files/"
  directory_arc_file = "C:/Users/LeeKafafian/Allostery Investments LP/Trading - Documents/Data/MSFS/files/archive/"

  # LOOP through download files and decrypt to out directory (files)
  x_extension = ".pgp"
  os.chdir(directory_in_str)
  for dfile in os.listdir(directory_in_str):
      dfilename = os.fsdecode(dfile)
      if dfilename.endswith(x_extension):
        emsg = pgpy.PGPMessage.from_file(directory_in_str + dfilename)
        key,_ = pgpy.PGPKey.from_file('C:/Tech/MSFS/keys/PGP/0x15CAECD7-sec.asc')     

        with key.unlock('LCqU8EXS4uLkHk!'):
          dmsg = key.decrypt(emsg)
          s_filename = dmsg.filename
          f = open(directory_out_str +  s_filename, 'wb')
          f.write(dmsg.message)
          f.close()

  # Archive downloads to archive folder
  f_extension = ".pgp"
  os.chdir(directory_in_str)
  for f in os.listdir(directory_in_str):
      if f.endswith(f_extension):
        if os.path.exists(directory_arc_str + f):
          x = ''.join(random.choices(string.ascii_letters + string.digits, k=16))        
          os.rename(f, f + '.' + x)
          f = f + '.' + x
        src_path = os.path.join(directory_in_str, f)
        dst_path = os.path.join(directory_arc_str, f)
        os.rename(src_path, dst_path)

  # Unzip and delete zip files
  z_extension = ".zip"
  os.chdir(directory_out_str) 
  allfiles = os.listdir(directory_out_str)

  for zitem in allfiles:                                 # loop through items in dir
      if zitem.endswith(z_extension):                    # check for ".zip" extension
          zfile_name = os.path.abspath(zitem)            # get full path of files
          zip_ref = zipfile.ZipFile(zfile_name)          # create zipfile object
          strEncodedName = zip_ref.filelist[0].filename  # get parent zip file name
          zip_ref.extractall(directory_out_str)          # extract file to dir
          zip_ref.close()                                # close file
          os.remove(zfile_name)                          # delete zipped file

          allfilesPostUnZip = os.listdir(directory_out_str)
          for cfile in allfilesPostUnZip:
            if cfile == strEncodedName:
                if cfile.endswith(".mht"):
                  zitemCarry = zitem.replace(".zip",".mht")
                if cfile.endswith(".csv"):
                  zitemCarry = zitem.replace(".zip", ".csv")
                os.rename(cfile, zitemCarry)


  # Position File Loading
  f_extension = ".csv"
  os.chdir(directory_out_str)

  for ffile in os.listdir(directory_out_str):
    if ffile.endswith(f_extension):

      # Position File Loading
      if ffile.startswith("Allostery-LK_PositionExtract"):
        df = pd.read_csv(ffile, skiprows=1)
        df = df.reset_index()
        bAsOfDate = False 
        for index, row in df.iterrows():
            
          if bAsOfDate == False:            
            dtAsOfDate = row.iloc[2]
            dtAsOfDate = dtAsOfDate.replace("=", "")
            dtAsOfDate = dtAsOfDate.replace('"','')
            dtAsOfDate = dtAsOfDate.replace("/", "")
              
            iYear = int(dtAsOfDate[-4:])
            iMonth = int(dtAsOfDate[:2])
            iDay = int(dtAsOfDate[2:-4])            
            dtDate = dt.date(iYear, iMonth, iDay)
            dtDate = dtDate.strftime(r'%m/%d/%y')
            print("".join(["Captured AsOfDate ", dtDate, " for file ", ffile,  " in ETL job..."]))
            bAsOfDate = True
            ClearAsOfAdminPositionData(dtDate)
        # Load rows/positions from file
          LoadRowsToDatabase(row.iloc[1].replace("=", "").replace('"',''), 
                              dtDate, 
                              row.iloc[3].replace("=", "").replace('"',''), 
                              row.iloc[4].replace("=", "").replace('"',''), 
                              row.iloc[5].replace("=", "").replace('"',''), 
                              row.iloc[6].replace("=", "").replace('"',''), 
                              row.iloc[7].replace("=", "").replace('"',''), 
                              row.iloc[8].replace("=", "").replace('"',''), 
                              row.iloc[9].replace("=", "").replace('"',''), 
                              row.iloc[10].replace("=", "").replace('"',''), 
                              row.iloc[11].replace("=", "").replace('"',''), 
                              row.iloc[12].replace("=", "").replace('"',''), 
                              row.iloc[13].replace("=", "").replace('"',''), 
                              row.iloc[14], 
                              row.iloc[15], 
                              row.iloc[16], 
                              row.iloc[17], 
                              row.iloc[18], 
                              row.iloc[19], 
                              row.iloc[20], 
                              row.iloc[21], 
                              row.iloc[22], 
                              row.iloc[23], 
                              row.iloc[24], 
                              row.iloc[25], 
                              row.iloc[26], 
                              row.iloc[27], 
                              row.iloc[28], 
                              row.iloc[29].replace("=", "").replace('"',''), 
                              row.iloc[30].replace("=", "").replace('"',''), 
                              row.iloc[31].replace("=", "").replace('"',''), 
                              row.iloc[32].replace("=", "").replace('"',''), 
                              row.iloc[33].replace("=", "").replace('"',''), 
                              row.iloc[34].replace("=", "").replace('"',''), 
                              row.iloc[35].replace("=", "").replace('"',''), 
                              row.iloc[36].replace("=", "").replace('"',''), 
                              row.iloc[37].replace("=", "").replace('"',''), 
                              row.iloc[38].replace("=", "").replace('"',''), 
                              row.iloc[39].replace("=", "").replace('"',''), 
                              row.iloc[40].replace("=", "").replace('"',''), 
                              row.iloc[41].replace("=", "").replace('"',''), 
                              row.iloc[42].replace("=", "").replace('"',''), 
                              row.iloc[43].replace("=", "").replace('"',''))
      
      # Return File Loading
      if "LK_ReturnHist" in ffile:
        dr = pd.read_csv(ffile, skiprows=1)
        dr = dr.reset_index()
        bAsOfDate = False 
        for index, row in dr.iterrows():
          
          if row.iloc[3] != 0 and row.iloc[4] != 0 and row.iloc[5] != 0:

            if bAsOfDate == False:            
              dtAsOfDate = row.iloc[2]
              dtAsOfDate = dtAsOfDate.replace("=", "")
              dtAsOfDate = dtAsOfDate.replace('"','')
              dtAsOfDate = dtAsOfDate.replace("/", "")
                
              iYear = int(dtAsOfDate[-4:])
              iMonth = int(dtAsOfDate[:2])
              iDay = int(dtAsOfDate[2:-4])            
              dtDate = dt.date(iYear, iMonth, iDay)
              dtDate = dtDate.strftime(r'%m/%d/%y')
              print("".join(["Captured AsOfDate ", dtDate, " for file ", ffile,  " in ETL job..."]))
              bAsOfDate = True

          # LOAD STANDARD FUND RETURN
            LoadPerformanceData(dtDate, "AMF", row.iloc[3]/100)

          # LOAD LONG MV FUND RETURN
            LoadPerformanceData(dtDate, "AMF LMV", row.iloc[4]/100)

          # LOAD SHORT MV FUND RETURN
            LoadPerformanceData(dtDate, "AMF SMV", row.iloc[5]/100)

          # LOAD RETURN CONTRIB LONG
            LoadPerformanceData(dtDate, "AMF RET CONTRIB LONG", row.iloc[9]/100)

          # LOAD RETURN CONTRIB SHORT
            LoadPerformanceData(dtDate, "AMF RET CONTRIB SHORT", row.iloc[10]/100)

          # LOAD RETURN CONTRIB NET
            LoadPerformanceData(dtDate, "AMF RET CONTRIB NET", row.iloc[11]/100)

          # LOAD AMF LONG MARKET VALUE
            LoadFundAssetData(dtDate, "AMF LONG MARKET VALUE", row.iloc[6])

          # LOAD AMF SHORT MARKET VALUE
            LoadFundAssetData(dtDate, "AMF SHORT MARKET VALUE", row.iloc[7])

          # LOAD AMF NET MARKET VALUE
            LoadFundAssetData(dtDate, "AMF NET MARKET VALUE", row.iloc[8])

          # LOAD NET ASSET VALUE
            LoadFundAssetData(dtDate, "AMF NAV", row.iloc[12])
      
    # ARCHIVE THE FILE 
    if ffile != "archive":
      if os.path.exists(directory_arc_file + ffile):
        x = ''.join(random.choices(string.ascii_letters + string.digits, k=16))        
        os.rename(ffile, ffile + '.' + x)
        ffile = ffile + '.' + x
      src_path = os.path.join(directory_out_str, ffile)
      dst_path = os.path.join(directory_arc_file, ffile)
      os.rename(src_path, dst_path)
      if not ffile.endswith(".mht"):
        SendMsgToDb("AMF data warehouse has loaded MSFS file: "  + ffile, "3", "MSFS File ETL", dt.datetime.now())
    
  SendMsgToDb("Scheduled Task [MSFS File ETL] has completed.", "3", "MSFS File ETL", dt.datetime.now())
