# -*- coding: utf-8 -*-
'''
Created on 5 Apr 2016

@author: Kévin Bienvenu

Module containing functions extracting informations from the
file 'cameliaBalAG.csv' 

All tests in module preprocessTest must pass to properly use the function
of this module. These tests include the functions from the modules:
- FTPTools : for the downloading and importing
- DrawingTools : for the export of graphs and histograms
- Utils : for other minor functions

=== Part I : Import of the data
        Imports and functions that get files and extract dataframe out of csv files

=== Part II : Cleaning Functions
        Functions that clean the BalAG file according to different columns

=== Part III - Analysing Functions
        Functions that analyze and compute statistics and graphs of the BalAG file

=== Part IV - Analysing functions using other files
        Functions that use the files 'Score' and 'Etab' to perform compared analysis

=== Part V - Creating 2d histograms
        Functions that computes useful crossed-analysis
        
=== Part VI - Scripts and Global Functions
        Functions and pipelines of global analysis function
'''
from numpy.core.operand_flag_tests import inplace_add
from compiler.ast import Const
from preprocess import Constants


''' I - Import of the data  '''

import datetime
import math
import os
import time

import Constants
import DrawingTools
import FTPTools
import Utils
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from Constants import bclnIdIntFormat, bclnIdMinimalBillsNumber, \
    clnIdMinimalBillsNumber, bclnIdMinimalIdValue, clnIdMinimalIdValue, \
    bclnIdMaximalIdValue, clnIdMaximalIdValue, bclnDatePieceFormat, \
    bclnDateEcheanceFormat, bclnDateDernierPaiementFormat, bclnDateInconsistent, \
    bclnDateMonthDiff, clnDateMonthDiff, bclnDateMinimalDate, clnDateMinimalDate, \
    bclnDateMaximalDate, clnDateMaximalDate, bclnMontantIntFormat, \
    bclnMontantNonNegativeValue, bclnMontantNonZeroValue, bclnMontMinimalValue, \
    clnMontMinimalValue, bclnMontMaximalValue, clnMontMaximalValue, \
    bclnMontantLitigeNonZero

def importCsv(filename = 'cameliaBalAG_extraitRandom.csv',sep='\t',usecols = None,dtype=None,addPaidBill = False):
    '''
    function that imports the content of the csv file. 
    The csv file must be stored stored locally.
    -- IN
    filename : name of the file, including the extension (string) default : 'cameliaBalAG_extraitRandom.csv'
    sep : separator for the pandas read_csv function (regexp) default : '\t'
    usecols : array containing the names of the column we want to import, 
        if None import all (string[] or None) default : None
    addPaidBill : boolean that settles if we add a column PaidBill (boolean) default : False
    -- OUT
    csvinput: the dataframe out of the csv file (pandas.Dataframe)
    returns None if an error occurs
    '''
    os.chdir(os.path.join("..",".."))
    # reading the local file
    try: 
        csvinput = pd.read_csv(filename,sep=sep,usecols = usecols,dtype=dtype)
    except:
        print "error : impossible to read the file"
        return None
    # adding a column to the dataframe with a boolean telling if the bill was paid
    if(addPaidBill):
        if(not 'dateDernierPaiement' in csvinput.columns):
            print "problem : impossible extraction of paidBill column"
        else:
            paidBill = []
            for row in csvinput['dateDernierPaiement'] :
                paidBill.append(not row == "0000-00-00")            
            csvinput['paidBill'] = pd.Series(paidBill, index=csvinput.index)
    os.chdir(os.path.join("src","preprocess"))
    return csvinput

def importFTPCsv(filename = 'cameliaBalAG.csv.gz',sep='\t',usecols = None,dtype = None,addPaidBill = False):
    '''
    function that imports the content of the csv file into the global
    variable csvinput. the file is downloaded from the remote ftp via
    one of the two functions in FTPTools according to the input
    -- IN
    filename : name of the file, including the extension (string) default : 'cameliaBalAG.csv.gz'
    sep : separator for the pandas read_csv function (regexp) default : '\t'
    usecols : array containing the names of the column we want to import, 
        if None import all (string[] or None) default : None
    addPaidBill : boolean that settles if we add a column PaidBill (boolean) default : False
    -- OUT
    returns the dataframe out of the csv file
    '''
    # importing the remote file
    csvinput = FTPTools.retrieveFtplib(filename, usecols=usecols, dtype=dtype, compression="gz")
    if csvinput is None:
        print "error : impossible to import the dataframe"
        return None
    # adding a column to the dataframe with a boolean telling if the bill was paid
    if(addPaidBill):
        if(not 'dateDernierPaiement' in csvinput.columns):
            print "problem : impossible extraction of paidBill column"
        else:
            paidBill = []
            for row in csvinput['dateDernierPaiement'] :
                paidBill.append(not row == "0000-00-00")            
            csvinput['paidBill'] = pd.Series(paidBill, index=csvinput.index)
    return csvinput

def getAndPreprocessCsvEtab(csvinput=None):
    '''
    function that imports the Etab file from the remote ftp server
    
    if csvinput is not None, it then deletes useless rows
    according to the entrep_id that must be in the csvinput file
    and the dates that must be in the proper date format "%Y%m%d"
    it keeps only one row by entreprise according to the following criterias:
    - 'DCREN' : we keep the minimal dateBilan for an entreprise over the whole set of etablissements
    - 'EFF_ENT' : we keep the maximal effectif for an entreprise over the whole set of etablissements
    - 'capital' : we keep the maximal capital for an entreprise over the whole set of etablissements

    Note that therefore the date aren't taken into account for the capital and the effectif
    If an entreprise grew a lot in a little time, there will be no difference between the two time-shifts.
    This choice was made in regard to the distribution of the entreprises in the cameliaBalAG file, 
    as most of the entreprises aren't present for more than three months; and knowing that joining
    a precise and denoised capital and effectif to a precise timestamp was not that easy according to the data.
    
    -- IN:
    csvEtab: the pandas dataframe containing etab file and at least column entrep_id (pandas.Dataframe)
    -- OUT:
    csvEtab: the cleaned pandas dataFrame containing only one row per concerned entreprises (pandas.Dataframe)
    returns None if an error occurs.
    '''

    print "Extracting the csvEtab dataframe"
    startTime = time.time()
    
    # setting parameters for the import of the dataframe
    usecols = ['entrep_id','capital','DCREN','EFF_ENT']
    dtype = {}
    dtype['entrep_id'] = "uint32"
    dtype['capital']   = "uint32"
    dtype['EFF_ENT']   = "uint32"
    dtype['DCREN']     = "string"
    # importing the file
    csvEtab = FTPTools.retrieveFtplib("ProcessedData/cameliaEtabKevin.csv.gz", compression="gz",
                                      usecols=usecols,dtype=dtype,toPrint=False,sep=";")
    print "   processing file",
    totalIni = len(csvEtab)
    # we remove wrong date input and convert the others

    csvEtab.loc[~csvEtab['DCREN'].str.contains(r"^[12][90][0-9][0-9]-[01][0-9]-[0-3][0-9]$",na=True)] = None
    csvEtab['DCREN'] = pd.to_datetime(csvEtab['DCREN'], format='%Y-%m-%d', errors='coerce')   
    csvEtab.dropna(axis=0, inplace=True)
    
    
    # if no parameter csvinput was given, we just return the whole Dataframe
    if csvinput is None:
        # counting errors and displaying results
        totalFin = len(csvEtab)
        print "... done"
        print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n" 
        Utils.printTime(startTime) 
        return csvEtab
    
    # otherwise, we clean the file according to the entrep_id in the csvinput Dataframe
    column = csvinput['entrep_id']
    # we remove lines of cvsEtab that are not concerning entreprises from csvinput
    csvEtab.loc[~csvEtab['entrep_id'].isin(column),['entrep_id']] = None    

#     # checking the validity of the result dataframe
#     if csvEtab is None or not (['entrep_id','capital','DCREN','EFF_ENT'] in csvEtab.columns):
#         print "error : invalid csvEtab file"
#         return None
    
    # grouping the csvEtab file according to 'entrep_id'
    grouped = csvEtab.groupby('entrep_id')
    # defining min() and max() lambda functions
    fmin = lambda x: pd.Series([x.min()]*len(x))
    fmax = lambda x: pd.Series([x.max()]*len(x))
    # merging the rows
    csvEtab['DCREN'] = grouped['DCREN'].transform(fmin)
    csvEtab['capital'] = grouped['capital'].transform(fmax)
    csvEtab['EFF_ENT'] = grouped['EFF_ENT'].transform(fmax)
    # keeping only one row per entreprise
    csvEtab = grouped.head(1)
    
    # counting errors and displaying results
    totalFin = len(csvEtab)
    print "... done"
    print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
    Utils.printTime(startTime)
    
    return csvEtab
      
def getAndPreprocessCsvScore(csvinput = None):  
    '''
    function that imports the Score file from the remote ftp server
    it then deletes useless or wrong formatted rows 
    the dates must be in the proper date format "%Y%m%d"
    it then only keeps the 'bilans1' rows
    -- IN:
    csvinput: the pandas dataframe containing BalAG file and at least column entrep_id in pos 0 (pandas.Dataframe)
    -- OUT:
    csvScore : pandas.Dataframe containing the file Score.
    '''
    print "Extracting the csvScore dataframe"
    startTime = time.time()
    
    # setting parameters for the import of the dataframe
    usecols = ['entrep_id','dateBilan','sourceModif','scoreSolv','scoreZ','scoreCH','scoreAltman']
    dtype= {}
    dtype['entrep_id'] = "uint32"
    dtype['dateBilan'] = "string"
    dtype['sourceModif'] = "string"
    dtype['scoreSolv'] = "float16"
    dtype['scoreZ'] = "float16"
    dtype['scoreCH'] = "float16"
    dtype['scoreAltman'] = "float16"
    csvScore = FTPTools.retrieveFtplib("cameliaScores.csv.bz2", compression = "bz2", 
                                       usecols=usecols, dtype = dtype, toPrint=False)
    print "   processing file",
    # cleaning according to the date column and converting remaning rows
    csvScore.loc[~csvScore.dateBilan.str.contains(r"^[12][90][0-9][0-9]-[01][0-9]-[0-3][0-9]$",na=True),['dateBilan']] = None
    csvScore['year'] = pd.to_datetime(csvScore['dateBilan'], format='%Y-%m-%d', errors='coerce').dt.year
    del csvScore['dateBilan']   
    totalIni = len(csvScore)
    csvScore.dropna(axis=0,inplace=True)
    
    # if the csvinput parameter was given in input, 
    # the file is cleaned further to match the input
    # if not, the file is returned this way
    if csvinput is None:
        # printing progress
        totalFin = len(csvScore)
        print "... done"
        print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
        Utils.printTime(startTime)
        return csvScore
    # keeping only the bilans1 rows
    csvScore = csvScore[csvScore.sourceModif=="bilans1"]
    # removing all years before 2010
    csvScore = csvScore[csvScore.year>=2010]
    column = csvinput['entrep_id']
    # we remove lines of cvsScore that are not concerning entreprises from csvinput
    csvScore.loc[~csvScore['entrep_id'].isin(column),['entrep_id']] = None
    
    # droping NaN rows and useless columns
    del csvScore['sourceModif']
    csvScore.dropna(axis=0, inplace=True)
    
    # printing final progress
    totalFin = len(csvScore)
    print "... done"
    print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
    Utils.printTime(startTime)
    
    
    return csvScore
    
''' II - Cleaning Functions '''
def cleaningEntrepId(csvinput):
    '''
    function that cleans the content of the first column 'entrep_id'
    according to the behaviors described in Constants and returns the cleaned dataframe
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toPrint : boolean that settles the display of computed informations (boolean) (default: False)
    -- OUT
    csvinput : the cleaned dataframe (dataframe)
    '''
    print "=== Starting Cleaning of entrep_id column ==="

    # checking if everything is all right with the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
        print ""
        return csvinput
    if not 'entrep_id' in csvinput.columns:
        print "error : No column to analyse - entrep_id"
        return csvinput

    # cleaning according to the 'entrep_id' int format
    # cleaning according to the minimal id
    # cleaning according to the maximal id
    test = ( Constants.bclnIdIntFormat & (csvinput.entrep_id<=0)
             | Constants.bclnIdMinimalIdValue & (csvinput.entrep_id<Constants.clnIdMinimalIdValue)
             | Constants.bclnIdMaximalIdValue & (csvinput.entrep_id>Constants.clnIdMaximalIdValue)
            )
    
    csvinput.loc[test] = None

    # cleaning according to the number of bills
    nbBills = csvinput.groupby('entrep_id').size()           
    if Constants.bclnIdMinimalBillsNumber:
        csvinput.loc[~nbBills[csvinput['entrep_id']]>Constants.clnIdMinimalBillsNumber] = None
    
    totalIni = len(csvinput)
    csvinput.dropna(axis=0,how='all',inplace=True)
    totalFin = len(csvinput)
    print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
        
    return csvinput

def cleaningDates(csvinput):
    '''
    function that cleans the content of the date columns
    'datePiece','dateEcheance','dateDernierPaiement'
    according to the behaviors described in Constants.
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toPrint : boolean that settles the display of computed informations (boolean) (default: False)
    -- OUT
    returns csvinput (dataframe)
    '''
        
    print "=== Starting Cleaning of date columns ==="
  
    # checking if everything is all right with the input
    if csvinput is None or len(csvinput)==0:
        print "error : No Data Remaining"
        print ""
        return csvinput
    if not 'datePiece' in csvinput.columns:
        print "error : No column to analyse - datePiece"
        return csvinput 
    if not 'dateEcheance' in csvinput.columns:
        print "error : No column to analyse - dateEcheance"
        return csvinput 
    if not 'dateDernierPaiement' in csvinput.columns:
        print "error : No column to analyse - dateDernierPaiement"
        return csvinput   

    
    # cleaning the data in one single test
    # criterias for dropping
    # - NaN date for datePiece (and bclnDatePieceFormat)
    # - NaN date for dateEcheance (and bclnDateEcheanceFormat)
    # - NaN date for dateDernierPaiement (and bclnDateDernierPaiementFormat)
    # - Echeance or DernierPaiement date anterior to Piece (and bclnDateInconsistent)
    # - Time gap between Echeance or DernierPaiement and Piece larger than threshold (and bclnDateMonthDiff)
    # - datePiece before minimal date (and bclnDateMinimalDate)
    # - datePiece after maximal date (and bclnDateMaximalDate)
    test = ((Constants.bclnDatePieceFormat & (csvinput.datePiece.isnull())) \
            |(Constants.bclnDateEcheanceFormat & (csvinput.dateEcheance.isnull())) \
            |(Constants.bclnDateDernierPaiementFormat & (csvinput.dateDernierPaiement.isnull())) \
            |(Constants.bclnDateInconsistent \
               & (csvinput.datePiece.notnull()) \
               & ((csvinput.dateDernierPaiement.notnull() \
                    & ((csvinput.dateDernierPaiement-csvinput.datePiece).astype('timedelta64[D]')<0)) \
                  |(csvinput.dateEcheance.notnull()  \
                    & ((csvinput.dateEcheance-csvinput.datePiece).astype('timedelta64[D]')<0))  \
                   )  \
              )  \
            | (Constants.bclnDateMonthDiff \
               & (csvinput.datePiece.notnull())  \
               & ((csvinput.dateDernierPaiement.notnull()  \
                   & ((csvinput.dateDernierPaiement-csvinput.datePiece).astype('timedelta64[M]')>Constants.clnDateMonthDiff))  \
                  |(csvinput.dateEcheance.notnull()  \
                    & ((csvinput.dateEcheance-csvinput.datePiece).astype('timedelta64[M]')>Constants.clnDateMonthDiff)))  \
               )  \
            | (Constants.bclnDateMinimalDate & (csvinput.datePiece.notnull()) & (csvinput.datePiece<Constants.clnDateMinimalDate)) \
            | (Constants.bclnDateMaximalDate & (csvinput.datePiece.notnull()) & (csvinput.datePiece>Constants.clnDateMaximalDate)) \
           ) 

    csvinput.loc[test] = None
    
    totalIni = len(csvinput)
    csvinput.dropna(axis=0,how='all',inplace=True)
    totalFin = len(csvinput)
    
    print csvinput.loc[csvinput.dateEcheance<Constants.clnDateMinimalDate]
    
    print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
    
    return csvinput

def cleaningMontant(csvinput):
    '''
    function that cleans the content of the column 'montantPieceEur'
    according to the behaviors described in Constants.
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toPrint : boolean that settles the display of computed informations (boolean) (default: False)
    -- OUT
    returns csvinput (dataframe)
    '''        
    print "=== Starting Cleaning of montantPieceEur columns ==="
    # validating the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
        print ""
        return csvinput
    if not 'montantPieceEur' in csvinput.columns:
        print "error : No column to analyse - montantPieceEur"
        return csvinput 
    
    # cleaning the data in one single test
    # criterias for dropping
    # - NaN int for montantPieceEur (and bclnMontantIntFormat)
    # - negative value for montantPieceEur (and bclnMontantNonNegativeValue)
    # - zero value for montantPieceEur (and bclnMontantNonZeroValue)
    # - montantPieceEur smaller than minimal montant (and bclnMontMinimalValue)
    # - montantPieceEur bigger than maximal montant (and bclnMontMaximalValue)
    
    test = ((Constants.bclnMontantIntFormat & csvinput.montantPieceEur.isnull())
            | (Constants.bclnMontantNonNegativeValue & (csvinput.montantPieceEur<0))
            | (Constants.bclnMontantNonZeroValue & (csvinput.montantPieceEur==0))
            | (Constants.bclnMontMinimalValue & (csvinput.montantPieceEur<Constants.clnMontMinimalValue))
            | (Constants.bclnMontMaximalValue & (csvinput.montantPieceEur>Constants.clnMontMaximalValue))
            )
    
    csvinput.loc[test] = None
    totalIni = len(csvinput)
    csvinput.dropna(axis=0,how='all',inplace=True)
    totalFin = len(csvinput)
    print "   ",str(totalIni-totalFin),"removed rows -",str(100.0*(totalIni-totalFin)/totalIni),"%\n"  
    
    
    return csvinput        
      
''' III - Analyzing Functions '''
def analyzingEntrepId(csvinput, toSaveGraph = False):
    '''
    function that analyses the content of the 'entrep_id' column
    it displays information about enterprises and the number of bills.
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toDrawGraph : boolean that settles the display of a graph displaying informations (boolean) (default: False)
    -- OUT
    returns nothing
    '''
    print "=== Starting Analysis of entrep_id column ==="
    print ""
    if csvinput is None:
        print "no result to analyse"
        print ""
        return
    if not('entrep_id' in csvinput.columns):
        print "wrong columns : 'entrep_id' is not present in the columns of the dataframe"
        print ""
        return
    if len(csvinput['entrep_id']) ==0 :
        print "no result to analyse"
        print ""
        return
    nbEntreprises = len(np.unique(csvinput['entrep_id']))
    nbEntries = len(csvinput['entrep_id'])
    
    print "enterprises number :" , nbEntreprises
    print "mean bills number by enterprise :", 1.0*nbEntries/nbEntreprises
    print "enterprise minimal id :", csvinput['entrep_id'].min()
    print "enterprise maximal id :", csvinput['entrep_id'].max()
    print ""
    
    nbBills = csvinput.groupby('entrep_id').size()         
       
    print "maximum bills number value: ",max(nbBills)
    print "minimum bills number value: ",min(nbBills)
    print ""    
    
    # about the repartition of ids
    idLog10 = lambda x : (int)(math.log10(x)*Constants.anaIdLogCoefficientBillNumber) if x>0 else 0
    repartitionIdArray = csvinput.entrep_id.apply(idLog10).value_counts(normalize=True,sort=False,ascending=False).values
    repartitionBillNumberArray = nbBills.apply(idLog10).value_counts(normalize=True,sort=False,ascending=False).values
    print repartitionIdArray
    print repartitionBillNumberArray
    
    if toSaveGraph:
        # I - Distribution of the Id
        nbStepGraph = len(repartitionIdArray)
        xlabel = [str("%.1e"%10**(1.0*i/Constants.anaIdLogCoefficientBillNumber)) for i in range(nbStepGraph)]
        DrawingTools.createHistogram(x=xlabel, y1=repartitionIdArray, typeyaxis='log',
                                     xlabel="Valeur de l'identifiant", ylabel="Nombre d'entreprises", 
                                     name="Distribution des identifiants", filename="01_IdIdentifiants")        
        # II - Distribution of the number of bills
        nbStepGraph = len(repartitionBillNumberArray)
        xlabel = [str("%.1e"%10**(1.0*i/Constants.anaIdLogCoefficientBillNumber)) for i in range(nbStepGraph)]
        DrawingTools.createHistogram(x=xlabel, y1=repartitionBillNumberArray, typeyaxis='log',
                                     xlabel="Valeur de l'identifiant", ylabel="Nombre de factures par entreprise", 
                                     name="Distribution du nombre de factures par entreprises", filename="02_IdNombreFactures")        
        
def analyzingDates(csvinput, toSaveGraph = False):
    '''
    function that analyses the content of the date columns
    'datePiece','dateEcheance','dateDernierPaiement'
    and displays information about them
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toSaveGraph : boolean that settles the creation and save of graphs displaying distribution and miscelleanous information (boolean) default: False
    toDrawGraphOld : boolean that settles the display of the version of graphs (boolean) default: False
    -- OUT
    returns nothing
    '''
    print ""
    print "=== Starting Analysis of date columns ==="
    print ""
    if csvinput is None:
        print "no result to analyse"
        print ""
        return
    if not('datePiece' in csvinput.columns) \
        or not('dateEcheance' in csvinput.columns) \
        or not('dateDernierPaiement' in csvinput.columns) \
        or not ('paidBill' in csvinput.columns):
        print "wrong columns"
        print ""
        return
    if len(csvinput) ==0 :
        print "no result to analyse"
        print ""
        return
    
    # analysis of the datePiece only for paid bills
    # columnDatePaid is the temporary vector with all converted dates ['datePiece', 'dateEcheance', 'dateDernierPaiement'] for paid bills
    print "Analysis of the paid bills"
    nbBills = len(csvinput)
    csvinputPaid = csvinput.loc[csvinput.paidBill==True,['datePiece','dateEcheance','dateDernierPaiement']]
    nbPaidBills = len(csvinputPaid)
    if nbPaidBills>0:
        # computing minimal and maximal dates
        minDatePaid = csvinputPaid.min(axis=0)
        maxDatePaid = csvinputPaid.max(axis=0)
        
        print ""
        print "    total amount of paid bills:", nbPaidBills,"-",100.0*nbPaidBills/nbBills,"%"
        print ""
        print "    DatePiece column:"
        # I especially want to apologize for those lines, 
        # but this string casting is the best way
        # I found to print only the date. Sorry...
        print "        min:", str(minDatePaid[0])[:10]
        print "        max:", str(maxDatePaid[0])[:10]
        print "    DateEcheance column:"
        print "        min:", str(minDatePaid[1])[:10]
        print "        max:", str(maxDatePaid[1])[:10]
        print "    DateDernierPaiement column:"
        print "        min:", str(minDatePaid[2])[:10]
        print "        max:", str(maxDatePaid[2])[:10]
        print ""
        print "    mean day number between Piece and Echeance :", ((csvinputPaid.dateEcheance-csvinputPaid.datePiece).astype('timedelta64[D]')).mean()
        print "    mean day number between Piece and DernierPaiement :", ((csvinputPaid.dateDernierPaiement-csvinputPaid.datePiece).astype('timedelta64[D]')).mean()
        print "    mean day number between Echeance and DernierPaiement :", ((csvinputPaid.dateDernierPaiement-csvinputPaid.dateEcheance).astype('timedelta64[D]')).mean()
        print ""
        print "    ratio of on-time paid bills :", 100.0*len(csvinputPaid[csvinputPaid.dateDernierPaiement<csvinputPaid.dateEcheance])/nbPaidBills,"%"
        print ""
    else:
        print "    no paid bills"

    # analysis of the datePiece only for unpaid bills
    csvinputUnpaid = csvinput.loc[csvinput.paidBill==False,['datePiece','dateEcheance']]
    print "Analysis of the unpaid bills"
    if len(csvinputUnpaid)>0:
        # computing minimal and maximal dates for unpaid bills
        minDateUnpaid = csvinputUnpaid.min(axis = 0)
        maxDateUnpaid = csvinputUnpaid.max(axis = 0)
        nbUnpaidBills = nbBills - nbPaidBills
        
        print ""
        print "    total amount of unpaid bills:", nbUnpaidBills,"-",100.0*nbUnpaidBills/nbBills,"%"
        print ""
        print "    DatePiece column:"
        print "        min:", str(minDateUnpaid[0])[:10]
        print "        max:", str(maxDateUnpaid[0])[:10]
        print "    DateEcheance column:"
        print "        min:", str(minDateUnpaid[1])[:10]
        print "        max:", str(maxDateUnpaid[1])[:10]
        print ""
        print "    mean day number between Piece and Echeance :", ((csvinputUnpaid.dateEcheance-csvinputUnpaid.datePiece).astype('timedelta64[D]')).mean()
        print ""
    else:
        print "    no unpaid bills"
    
    if toSaveGraph:
        # PRE PROCESSING 
        # computing the number of step for the bar-chart
        #    here by default Constants.anaDateStepMonthGraph = 12
        # array to store the number of entries by year
        numberByYear = csvinput['datePiece'].dt.year.value_counts(normalize=True,sort=False,ascending=False).values
        numberByMonth = csvinput['datePiece'].dt.month.value_counts(normalize=True,sort=False,ascending=False).values
        numberPaidByYear = csvinputPaid['datePiece'].dt.year.value_counts(normalize=True,sort=False,ascending=False).values
        numberPaidByMonth = csvinputPaid['datePiece'].dt.month.value_counts(normalize=True,sort=False,ascending=False).values
        delayDernierPaiementByYear = []
        delayEcheanceByYear = []
        delayDernierPaiementByMonth = []
        delayEcheanceByMonth = []
        labelByYear = []
        for year in range((Constants.clnDateMinimalDate).year,(Constants.clnDateMaximalDate).year+1):
            for month in range(1,13):
                # mean over the dernierpaiement column : only on the paid bills
                test = (csvinputPaid.datePiece.dt.month==month) & (csvinputPaid.datePiece.dt.year==year)
                csvtemp = csvinputPaid.loc[test]
                if len(csvtemp)>0:
                    delayDernierPaiementByYear.append(np.mean((csvtemp.dateDernierPaiement-csvtemp.datePiece).astype('timedelta64[D]').values))
                else:
                    delayDernierPaiementByYear.append(0)
                # mean over the echeance column : on all bills
                test = (csvinput.datePiece.dt.month==month) & (csvinputPaid.datePiece.dt.year==year)
                csvtemp = csvinput.loc[test]
                if len(csvtemp)>0:
                    delayEcheanceByYear.append(np.mean((csvtemp.dateEcheance-csvtemp.datePiece).astype('timedelta64[D]').values))
                else:
                    delayEcheanceByYear.append(0)
                labelByYear.append(str(month)+"-"+str(year)[2:])
        for month in range(1,13):
                # mean over the dernierpaiement column : only on the paid bills
            test = csvinputPaid.datePiece.dt.month==month
            csvtemp = csvinputPaid.loc[test]
            if len(csvtemp)>0:
                delayDernierPaiementByMonth.append(((csvtemp.dateDernierPaiement-csvtemp.datePiece).astype('timedelta64[D]')).mean())
            else:
                delayDernierPaiementByMonth.append(0)
                # mean over the echeance column : on all bills
            test = csvinput.datePiece.dt.month==month
            csvtemp = csvinput.loc[test]
            if len(csvtemp)>0:
                delayEcheanceByMonth.append(((csvtemp.dateEcheance-csvtemp.datePiece).astype('timedelta64[D]')).mean())
            else:
                delayEcheanceByMonth.append(0)
        labelByMonth = Constants.labelByMonth
        # DRAWING
        # III - distribution of entries over the years and over the months
        DrawingTools.createHistogram(x=labelByYear, y1=numberByYear,ylabel="Nombre de factures",
                                     name="Distribution des factures par année",filename="03_DateFacturesAnnees")
        DrawingTools.createHistogram(x=labelByMonth, y1=numberByMonth,ylabel="Nombre de factures",
                                     name="Distribution des factures par mois",filename="03b_DateFacturesMois")
        # IV - distribution of ratio of paid and ontimepaid over the years and the months
        DrawingTools.createHistogram(x=labelByYear,y1=numberPaidByYear,ylabel="Pourcentage de factures",
                                     name1="Pourcentage de factures payées",
                                     name="Ratio des factures payées par année",filename="04_RatioFacturesPayeesAnnees")
        DrawingTools.createHistogram(x=labelByMonth,y1=numberPaidByMonth,ylabel="Pourcentage de factures",
                                     name1="Pourcentage de factures payées",
                                     name="Ratio des factures payées par mois",filename="04b_RatioFacturesPayeesMois")
        # V - distribution of delays
        DrawingTools.createHistogram(x=labelByYear,y1=delayEcheanceByYear,y2=delayDernierPaiementByYear,ylabel="Jours de délais",
                                     name1="Durée de l'échéance",name2="Durée du temps de paiement",
                                     name="Durées des délais d'échéance et de paiement",filename="05_DelayFacturesAnnees")
        DrawingTools.createHistogram(x=labelByMonth,y1=delayEcheanceByMonth,y2=delayDernierPaiementByMonth,ylabel="Jours de délais",
                                     name1="Durée de l'échéance",name2="Durée du temps de paiement",
                                     name="Durées des délais d'échéance et de paiement",filename="05b_DelayFacturesMois")
                              
def analyzingMontant(csvinput, toSaveGraph = False):
    '''
    function that analyses the content of the column 'montantPieceEur'
    and displays information about it
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toDrawGraph : boolean that settles the display of a graph displaying informations (boolean) (default: False)
    -- OUT
    returns nothing
    '''
    print "=== Starting Analysis of montantPieceEur column ==="
    print ""
    if csvinput is None:
        print "no result to analyse"
        print ""
        return
    if not('montantPieceEur' in csvinput.columns):
        print "wrong columns"
        print ""
        return
    if len(csvinput) ==0 :
        print "no result to analyse"
        print ""
        return

    # displaying basic informations
    print "minimal bill value :", csvinput.montantPieceEur.min()
    print "maximal bill value :", csvinput.montantPieceEur.max()
    print "mean bill value :", csvinput.montantPieceEur.mean()
    print "median bill value :", csvinput.montantPieceEur.median()
    
    # about the log-repartition of ids
    idLog10 = lambda x : (int)(1.0*math.log10(1.0*x)*Constants.anaIdLogCoefficientMontants) if x>0 else 0
    serie = csvinput.montantPieceEur.apply(idLog10).value_counts(normalize=True,sort=False,ascending=False)
    xlabel = [str(10**(a/Constants.anaIdLogCoefficientMontants)) if a%Constants.anaIdLogCoefficientMontants==0 else "" for a in serie.index]
    repartitionMontantArray = serie.values
    print "repartition of the montants according to their log-10:"
    print repartitionMontantArray
    print ""
    
    # creating and saving graph
    if toSaveGraph:
        # PREPROCESSING
        # computing length of the array
        nbStepGraph = len(repartitionMontantArray)
        # computing labels
#         xlabel = ["e^"+str(1.0*i/Constants.anaIdLogCoefficientMontants) for i in range(nbStepGraph)]
        # DRAWING
        DrawingTools.createHistogram(x=xlabel, y1=repartitionMontantArray, 
                                     xlabel="Valeur des factures (euros)", ylabel="Nombre de factures", 
                                     name="Distribution des montants des factures", filename="06_Montants")

def analyzingOthers(csvinput):  
    '''
    function that analyses the content of the other columns 
    'montantLitige', 'devise', 'dateInsert'
    and displays information about them
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toDrawGraph : boolean that settles the display of a graph displaying informations (boolean) (default: False)
    -- OUT
    returns nothing
    '''  
    print "=== Starting Analysis of other columns ==="
    print ""
        
    if csvinput is None:
        print "no result to analyse"
        print ""
        return
    
    if not('montantLitige' in csvinput.columns) \
        or not('devise' in csvinput.columns) \
        or not('dateInsert' in csvinput.columns):
        print "wrong columns"
        print ""
        return
    
    # importing and displaying basic informations
    columnMontantLitige = np.array(csvinput['montantLitige'].values)
    columnDevise = np.array(csvinput['devise'].values)
    columnDateInsert = np.array(csvinput['dateInsert'].values)

    if len(columnDevise) ==0 :
        print "no result to analyse"
        print ""
        return

    # listing all other possibilities
    otherMontantLitige = []
    otherDevise = []
    otherDateInsert = []
    for m in columnMontantLitige:
        if m>0:
            otherMontantLitige.append(m)
    otherMontantLitige = np.unique(otherMontantLitige)
    for m in columnDevise:
        if m>0:
            otherDevise.append(m)
    otherDevise = np.unique(otherDevise)
    for m in columnDateInsert:
        if m>0:
            otherDateInsert.append(m)
    otherDateInsert = np.unique(otherDateInsert)
    
    nonZeroMontantLitige = len([1 for a in columnMontantLitige if a>0])
    
    print "ratio of non-zero montantLitige :",len([1 for a in columnMontantLitige if a>0]),"-",100.0*nonZeroMontantLitige/len(columnMontantLitige),"%"
    print ""
#     print "other possible values for montantLitige :", otherMontantLitige
    print "ratio of non-EUR devise :",100.0*len([1 for a in columnDevise if not a=="EUR"])/len(columnDevise),"%"
    print "   other possible values for devise :", otherDateInsert
    print ""
    print "ratio of non-standard dateInsert :",100.0*len([1 for a in columnDateInsert if not datetime.datetime.strptime(a[:10],"%Y-%m-%d").date()==Constants.anaOtherStandardDate])/len(columnDevise),"%"
    print "   other possible values for dateInsert :", otherDateInsert
        
def analyzingComplete(csvinput, toSaveGraph = False, toDrawGraphOld = False):
    '''
    function that analyses the content of the columns
    'entrep_id', 'datePiece', 'dateEcheance', 'dateDernierPaiement' and 'montantPieceEur'
    and displays information about the joined columns
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toDrawGraph : boolean that settles the display of a graph displaying informations (boolean) (default: False)
    -- OUT
    returns nothing
    '''  
    print "=== Starting Analysis of all columns ==="
    print ""
       
    if csvinput is None:
        print "no result to analyse"
        print ""
        return
    
    if not('montantLitige' in csvinput.columns) \
        or not('devise' in csvinput.columns) \
        or not('entrep_id' in csvinput.columns) \
        or not('datePiece' in csvinput.columns) \
        or not('dateEcheance' in csvinput.columns) \
        or not('dateDernierPaiement' in csvinput.columns) \
        or not('montantPieceEur' in csvinput.columns) \
        or not('dateInsert' in csvinput.columns):
        print "wrong columns"
        print ""
        return
    
    
    # importing column
    column = csvinput[['entrep_id','datePiece','dateEcheance','dateDernierPaiement','paidBill','montantPieceEur','montantLitige']]
    
    if len(column) ==0 :
        print "no result to analyse"
        print ""
        return
    
    # dictionary linking entrep_id to informations about it
    #     informations: [minBillDate, maxBillDate, nbBill, nbPaid, Delay, Montant]
    #     informations that will be added : [..., numberOfYearOfActivity, meanBillNumberPerYear]
    entrepriseDict = {}
    # dictionary linking years to montant values
    yearsToMontantDict = {}
    # dictionary linking number of bills to delay
    vectorDelay = [0]*((int)(Constants.anaCompletMaximalDelay/Constants.anaCompletStepSizeDelay+1))
    # vectors of starting and ending dates for entreprises
    vectorStartDateAll = []
    vectorEndDateAll = []
    vectorStartDate = []
    vectorEndDate = []
    # array keeping analysis about the montant values
    #     informations [delay (days), nbPaid, nbEntries]
    maxMontant = np.max(csvinput['montantPieceEur'])
    informationMontantArray = [[0,0,0] for i in range((int)(math.log10(maxMontant)+1))]
    maxDelay = 0
    minDate = 0
    maxDate =0
    # array about montantLitige
    numberMontantLitige = 0
    numberMontantLitigeAndDelay = 0
    numberMontantLitigeAndNotPaid = 0
    # filling the dictionaries
    for entry in column.values:
        d = datetime.datetime.strptime(entry[1],"%Y-%m-%d").date()
        if minDate==0 or d<minDate:
            minDate = d
        if maxDate==0 or d>maxDate:
            maxDate = d
        if entry[4]:
            d2 = datetime.datetime.strptime(entry[3],"%Y-%m-%d").date()
            delay = (d2-d).days
            if delay>maxDelay:
                maxDelay = delay
            vectorDelay[min(len(vectorDelay)-1,(int)(delay/Constants.anaCompletStepSizeDelay))] += 1
        if not entrepriseDict.has_key(entry[0]):
            # creating the enterprise field
            # informations: [minBillDate (date), maxBillDate (date), nbBill (int), nbPaid (int), Delay (int-days), Montants (array)]
            entrepriseDict[entry[0]] = [d, d, 1, 1 if entry[4] else 0, delay if entry[4] else 0, [entry[5]]]
        else:
            entrepriseDict[entry[0]][0] = min(d,entrepriseDict[entry[0]][0])
            entrepriseDict[entry[0]][1] = max(d,entrepriseDict[entry[0]][1])
            entrepriseDict[entry[0]][2] = entrepriseDict[entry[0]][2] + 1
            entrepriseDict[entry[0]][3] = entrepriseDict[entry[0]][3] + (1 if entry[4] else 0)
            entrepriseDict[entry[0]][4] = entrepriseDict[entry[0]][4] + (delay if entry[4] else 0)
            entrepriseDict[entry[0]][5].append(entry[5])
        if not d.year in yearsToMontantDict:
            yearsToMontantDict[d.year] = [entry[5]]
        else:
            yearsToMontantDict[d.year].append(entry[5])
        if entry[4]:
            informationMontantArray[(int)(math.log10(entry[5]))][1] += 1
            informationMontantArray[(int)(math.log10(entry[5]))][0] += (delay if entry[4] else 0)
        informationMontantArray[(int)(math.log10(entry[5]))][2] += 1
        try:
            if int(entry[6])>0:
                numberMontantLitige += 1
                if entry[4]:
                    d1 = datetime.datetime.strptime(entry[2],"%Y-%m-%d").date()
                    if d2>d1:
                        numberMontantLitigeAndDelay += 1
                else:
                    numberMontantLitigeAndNotPaid += 1
        except:
            continue
    # computing additional informations
    for entreprise in entrepriseDict.values():
        if entreprise[0]!=entreprise[1]:
            vectorStartDate.append(entreprise[0])
            vectorEndDate.append(entreprise[1])
        vectorStartDateAll.append(entreprise[0])
        vectorEndDateAll.append(entreprise[1])
        if not (entreprise[4]==0 or entreprise[3]==0):
            entreprise[4] = entreprise[4] / entreprise[3]
            entreprise[5] = [np.mean(entreprise[5]),np.std(entreprise[5])]
        else:
            entreprise[5] = [0,0]
        entreprise.append(1.0*(entreprise[1] - entreprise[0]).days/365.25)
        if not entreprise[6]==0:
            entreprise.append(1.0*entreprise[2]/entreprise[6])
        else:
            entreprise.append(1)
    yearMeanMontantArray = [0] * (maxDate.year+1-minDate.year)
    yearErrorMontantArray = [0] * (maxDate.year+1-minDate.year)
    yearNumberMontantArray = [0] * (maxDate.year+1-minDate.year)
    for year in range(minDate.year,maxDate.year+1):
        if not year in yearsToMontantDict:
            yearMeanMontantArray[year-minDate.year] = 0
            yearErrorMontantArray[year-minDate.year] = 0
            yearNumberMontantArray[year-minDate.year] = 0
        else:
            yearMeanMontantArray[year-minDate.year] = np.mean(yearsToMontantDict[year])
            yearErrorMontantArray[year-minDate.year] = np.std(yearsToMontantDict[year])
            yearNumberMontantArray[year-minDate.year] = len(yearsToMontantDict[year])
    for i in range(len(informationMontantArray)):
        if informationMontantArray[i][1] > 0:
            informationMontantArray[i][0] = informationMontantArray[i][0] / informationMontantArray[i][1]        
        
    vectorYearNumber = [entreprise[6] for entreprise in entrepriseDict.values()]
    vectorMeanMontant = [entreprise[5][0] for entreprise in entrepriseDict.values()]
    vectorVarMontant = [entreprise[5][1] for entreprise in entrepriseDict.values()]
    print "number of enterprises :", len(entrepriseDict)
    print ""
    print "mean number of years of activity :", np.mean(vectorYearNumber)
    print "min number of years of activity :", np.min(vectorYearNumber)
    print "max number of years of activity :", np.max(vectorYearNumber)
    print ""
    print "min mean montant of enterprise :", np.min(vectorMeanMontant)
    print "max mean montant of enterprise :", np.max(vectorMeanMontant)
    print ""
    print ""
    print "number of non-zero montantLitige :",numberMontantLitige,"-",100.0*numberMontantLitige/len(column),"%"
    if numberMontantLitige>0:
        print "    percentage of non-zero montantLitige and unpaid bills :",100.0*numberMontantLitigeAndNotPaid/numberMontantLitige,"%"
        print "    percentage of non-zero montantLitige and delay :",100.0*numberMontantLitigeAndDelay/numberMontantLitige,"%"
    print ""
    # creating and saving graphs
    if toSaveGraph:
        # creating vectors
        nbStepGraphNbEntreprisesByMonth = 12*maxDate.year+maxDate.month - 12*minDate.year - minDate.month + 1
        vectorNbEntreprisesByMonth = [0]*nbStepGraphNbEntreprisesByMonth
        nbStepGraphDelayOverMontant = len(informationMontantArray)
        # filling vectors
        print len(entrepriseDict)
        for entreprise in entrepriseDict.values():
            for i in range(12*entreprise[0].year+entreprise[0].month - 12*minDate.year - minDate.month, 
                           12*entreprise[1].year+entreprise[1].month - 12*minDate.year - minDate.month+1):
                vectorNbEntreprisesByMonth[i] += 1
        # creating labels
        xlabelNbEntreprisesByMonth = [str((minDate+i*(maxDate-minDate)/(nbStepGraphNbEntreprisesByMonth-1)).year) if i%12==0 else "" for i in range(nbStepGraphNbEntreprisesByMonth)]
        xlabelMontantOverYear = range(minDate.year,maxDate.year+1)
        xlabelDelayOverMontant = ['e^'+str(i) for i in range(nbStepGraphDelayOverMontant)]
        xlabelNbBillsDelay = [str(i*Constants.anaCompletStepSizeDelay) +("+"if i==Constants.anaCompletMaximalDelay-1 else "") for i in range(Constants.anaCompletMaximalDelay)]
        # drawing graphs
        # VII - Actives Enterprises over the years
        DrawingTools.createHistogram(x=xlabelNbEntreprisesByMonth,y1=vectorNbEntreprisesByMonth, 
                                     ylabel="Nombre d'entreprises", name="Entreprises actives selon les années", filename="07_CompleteEntreprisesActives")
        # VIII - Special 2D Monster of the incredible death of GdB
        DrawingTools.createHistogram2D(y0=vectorStartDateAll,y1=vectorEndDateAll,
                                       xlabel="Année de la première facture",ylabel="Année de dernière facture",
                                       name="Activité de toutes les entreprises", 
                                       filename="08_ActivityTimeAll")
        DrawingTools.createHistogram2D(y0=vectorStartDate,y1=vectorEndDate,
                                       xlabel="Année de la première facture",ylabel="Année de dernière facture",
                                       name="Activité des entreprises de plus de une facture", 
                                       filename="08b_ActivityTimeMoreThanOne")
                                       
        # IX - Distribution of the montant over the year
        DrawingTools.createHistogram(x=xlabelMontantOverYear,y1=yearMeanMontantArray,
                                     ylabel="Valeur moyenne des montants", name="Distribution des montants selon les années", filename="09_CompleteMontantSelonAnnees")
        # X - Distribution Delay over Montant
        DrawingTools.createHistogram(x=xlabelDelayOverMontant,y1=np.array(informationMontantArray).T[0],
                                     xlabel="Valeur du montant de la facture (euros)", ylabel="Délais de paiement (jours)",
                                     name="Distribution des délais de paiement en fonction du montant des factures",
                                     filename="10_CompleteDelaiSelonMontant")
        # XI - Distribution Delay
        DrawingTools.createHistogram(x=xlabelNbBillsDelay,y1=vectorDelay,
                                     xlabel="Délais de paiement (jours)", ylabel="Nombre de factures",
                                     name="Distribution des délais de paiement", filename="11_CompleteDistributionDelai")
        
    # dealing with the old graph displaying
    if toDrawGraphOld:
        # initializing graph displaying
        plt.figure(Constants.figureId, figsize = Constants.figsize)
        Constants.figureId += 1
        plt.suptitle("Complete Analysis", fontsize=16)
        Constants.graphId = 1
        
        ####################################################
        # Drawing the number of active enterprises by year #
        ####################################################
        # pre-processing
        nbStepGraph = 12*maxDate.year+maxDate.month - 12*minDate.year - minDate.month + 1
        vectorNbEntreprisesByMonth = [0 for _ in range(nbStepGraph)]
        for entreprise in entrepriseDict.values():
            for i in range(12*entreprise[0].year+entreprise[0].month - 12*minDate.year - minDate.month, 
                           12*entreprise[1].year+entreprise[1].month - 12*minDate.year - minDate.month+1):
                vectorNbEntreprisesByMonth[i] += 1
        xlabelYear = [str((minDate+i*(maxDate-minDate)/(nbStepGraph-1)).year) if i%12==0 else "" for i in range(nbStepGraph)]
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),vectorNbEntreprisesByMonth,color=Constants.colorOrange,edgecolor = "none")
        plt.title("number of active enterprises at a current time", fontsize=12)
        plt.xticks(range(nbStepGraph),xlabelYear,rotation = 70)
        ax1.set_yscale('log')
        plt.ylabel("number of enterprises", fontsize=10)
        
        ############################################################################
        # Drawing the repartition of mean and var bills values for the enterprises #
        ############################################################################
        # pre-processing
        sizeVector = max((int)(math.log10(max(vectorMeanMontant)+0.01)+1),(int)(math.log10(max(vectorVarMontant)+0.01)+1))
        repartitionMeanMontantArray = [0]*sizeVector
        repartitionVarMontantArray = [0]*sizeVector
        for e in vectorMeanMontant:
            if e>0:
                repartitionMeanMontantArray[(int)(math.log10((int)(e+1)))] += 1
        for e in vectorVarMontant:
            if e>0:
                repartitionVarMontantArray[(int)(math.log10((int)(e+1)))] += 1
        nbStepGraph = sizeVector
        labelArray = [0] * nbStepGraph
        for i in range(nbStepGraph):
            labelArray[i] = '%.0e' % 10**i
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),repartitionMeanMontantArray, Constants.barSpace, color=Constants.colorBlue)
        plt.bar(np.arange(nbStepGraph)+Constants.barSpace,repartitionVarMontantArray,Constants.barSpace,color=Constants.colorGreen)
        plt.title("repartition of the mean and var bills values of enterprises", fontsize=12)
        plt.xticks(range(nbStepGraph),labelArray,rotation=70)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorBlue, label='Mean bills values'),mpatches.Patch(color=Constants.colorGreen, label='Var bills values')], fontsize = 10)
        plt.ylabel("number of enterprises", fontsize=10)
                
        ##############################################################
        # Drawing the number and mean amount of bills over the years #
        ##############################################################
        # pre-processing
        nbStepGraph = len(yearMeanMontantArray)
        xlabelMonth = range(minDate.year,maxDate.year+1)
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        ax1.bar(range(nbStepGraph),yearMeanMontantArray,Constants.barSpace,color=Constants.colorBlue,edgecolor = "none",yerr = yearErrorMontantArray)
        plt.ylabel("Bills values", fontsize=10)
        ax1.set_yscale('log')
        plt.xticks(range(nbStepGraph),xlabelMonth,rotation = 70)
        ax2 = ax1.twinx()
        ax2.bar(np.arange(nbStepGraph)+Constants.barSpace,yearNumberMontantArray,Constants.barSpace,color=Constants.colorOrange,edgecolor = "none")
        ax2.set_yscale('log')
        plt.ylabel("Bills numbers", fontsize=10)
        plt.title("number and mean amount of bills over the years", fontsize=12)
        plt.xlabel("time", fontsize=10)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorBlue, label='Mean bills values'),
                              mpatches.Patch(color=Constants.colorOrange, label='Number bills values')], 
                   fontsize = 10)
        
        ###########################################
        # Drawing the repartition of bills values #
        ###########################################
        # pre-processing
        nbStepGraph = len(informationMontantArray)
        labelArray = [0] * nbStepGraph
        for i in range(nbStepGraph):
            labelArray[i] = '%.1e' % 10**i
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        ax1.bar(range(nbStepGraph),np.array(informationMontantArray).T[0],Constants.barSpace,color=Constants.colorOrange)
        plt.title("delay and ratio of paid bills over the montant values", fontsize=12)
#         plt.xticks(range(nbStepGraph),labelArray)
        plt.xlabel("value of bills", fontsize=10)
        plt.ylabel("delay of paid bills (days)", fontsize=10) 
        plt.xticks(range(nbStepGraph),labelArray,rotation=70)
        ax2 = plt.twinx(ax1)
        ax2.bar(np.arange(nbStepGraph)+Constants.barSpace,np.array(informationMontantArray).T[2],Constants.barSpace,color=Constants.colorGreen)
        ax2.bar(np.arange(nbStepGraph)+Constants.barSpace,np.array(informationMontantArray).T[1],Constants.barSpace,color=Constants.colorBlue)
        plt.ylabel("number of bills", fontsize=10) 
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorBlue, label='On-time Paid Bills'),
                              mpatches.Patch(color=Constants.colorGreen, label='Paid Bills'),
                              mpatches.Patch(color=Constants.colorOrange, label='Mean delay')], 
                   fontsize = 10)

def analyzingIdCorresponding(csvinput, fileToCompare):
    '''
    function that checks if all the entreprises references in csvinput are in 
    the other csv file 'CameliaEtab.csv.gz'
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    fileToCompare : name of the file on the remote ftp "CameliaBilans.csv.gz" or "CameliaEtab.csv.gz (string) 
    -- OUT 
    returns nothing
    '''
    print "=== Comparison of the entrep_id with", fileToCompare,"==="
    print ""
    columnIdEtab = pd.unique(importFTPCsv(fileToCompare, usecols = ['entrep_id'])['entrep_id'])
    columnIdBalAG = pd.unique(csvinput['entrep_id'])
    print "nombre d'entreprises dans CameliaBalAG.csv.gz :", len(columnIdBalAG)
    print "nombre d'entreprises dans",fileToCompare,":", len(columnIdEtab)
    nonAppearing = set(columnIdBalAG).difference(set(columnIdEtab))
    print "nombre d'entreprises communes :", len(columnIdBalAG) - len(nonAppearing)
    print "entreprises inconnues :",list(nonAppearing)
    print ""
     
''' IV - Analysing functions using other files '''

def analyzingEntrepScore():
    print "=== Starting Analysis of Entreprises Scores ==="
    print ""
    
    startTime = time.time()
    csvinput = importAndCleanCsv(True, ftp = True)
    column = csvinput[['entrep_id','datePiece']].values
    #initializing variables
    #    setting size variables
    nbEntreprises = len(np.unique(column))
    print "enterprises number :" , nbEntreprises
    # initializing variables
    #    dictionary linking entrep_id to concerned rows
    entrepriseToRowsDict = {}
    dicNbEntreprise = {}
    ind = 0
    for entreprise in column:
        # if the entreprise is not present in the dictionay we add it
        if not entrepriseToRowsDict.has_key(entreprise[0]):
            entrepriseToRowsDict[entreprise[0]] = []
        # we add the year of the bill to the dictionary
        entrepriseToRowsDict[entreprise[0]].append(entreprise[1][:4])
        ind += 1
    # we create another dictionary : dicNbEntreprise
    # keys : entreprises
    # values : [entreprise, nbBill, minYear, maxYear]
    for entreprise in entrepriseToRowsDict:
        dicNbEntreprise[entreprise] = [entreprise,len(entrepriseToRowsDict[entreprise]),min(entrepriseToRowsDict[entreprise]),max(entrepriseToRowsDict[entreprise])]
    # clearing the column
    column = []
    del entrepriseToRowsDict
    # creating a dataframe out of the previous dictionary
    entrepriseData = pd.DataFrame.from_dict(dicNbEntreprise, orient='index')
    entrepriseData.columns = ['entrep_id','nbBills','dateMin','dateMaxi']

    ## IMPORTING SCORE DATA
    print "importing the Score csv",
    csvScore = getAndPreprocessCsvScore()
    print "... done:",
    interval = time.time() - startTime
    print interval, 'sec'
    print ""
    
    # CLEANING AND MERGING THE DATAFRAME
    print "cleaning the dataframe",
    # keeping only bilans1 in the score file
    csvScore = csvScore[csvScore.sourceModif=="bilans1"]
    # removing all years before 2010
    csvScore = csvScore[csvScore.dateBilan.str[2:4]>=10]
    # droping lines with empty dates
    csvScore = csvScore[csvScore.dateBilan!="0000-00-00"]
    # droping the useless columns
    del csvScore['sourceModif']
    # merging both the dataframes according to the entrep_id field
    entrepriseData = entrepriseData.merge(csvScore, on="entrep_id")
    print "... done"
    print ""
    
    # final entrepriseData fields:
    # ['entrep_id', 'nbBills', 'dateMin', 'dateMax', 'dateBilan', 'scoreSolv','scoreZ','scoreCH','scoreAltman']
    # [    0      ,     1    ,     2    ,     3    ,     4      ,      5     ,   6    ,    7    ,     8

    ## ANALYZING DATES CONSISTENCE
    print "analyzing dates consistence"
    # adding a new field to the dictionary
    # values: list of dates, 0
    for entreprise in dicNbEntreprise.keys():
        dicNbEntreprise[entreprise].append(0)
    for line in entrepriseData.values:
        # for each entry, if the dateBilan is between dateMin-1 and dateMax we increase the counter
        # we allow the bilan of year N counting for year N+1 and N
        if line[4][:4]<=line[3] and line[4][:4]>=int(line[2])-1:
            dicNbEntreprise[int(line[0])][-1]+=1
    # computing the percentage of years present in the file
    # for instance if a company has dateMin=2011, dateMax=2015 and three entries : 2010, 2012, 2014
    # all three of them are valid, we have 3 years over 5, so the score for this company is 60%
    #
    # we assume here that each year is present at most one time
    for entreprise in dicNbEntreprise.keys():
        dicNbEntreprise[entreprise][-1] = 100.0*dicNbEntreprise[entreprise][-1]/(int(dicNbEntreprise[entreprise][3])+1-int(dicNbEntreprise[entreprise][2]))
    print "   percentage of scores:", 1.0*sum([a[-1] for a in dicNbEntreprise.values()])/len(dicNbEntreprise)
    print ""
    print ""
    del dicNbEntreprise
    
    ## CREATING HISTOGRAMS AND PRINTING STATS
    print "computing histogram"
    print "number of lines to analyze :", len(csvScore)
    dicScore = {0:'scoreSolv', 1:'scoreZ',2:'scoreCH',3:'scoreAltman'}
    maxisMerged = np.max(entrepriseData,axis=0)
    minisMerged = np.min(entrepriseData,axis=0)
    maxisGlobal = np.max(csvScore,axis=0)
    print "max computed -",
    minisGlobal = np.min(csvScore,axis=0)
    print "min computed"
    dicX = {}
    dicY = {}
    dicError = {}
    nbStepHistogram = 50
    # getting ready to plot histograms
    for dic in dicScore.values():
        print dic
        # getting min and max values
        print "   max merged:",maxisMerged[dic],
        print "  max global:",maxisGlobal[dic]
        print "   min merged:",minisMerged[dic],
        print "  min global:",minisGlobal[dic]
        print ""
        if dic=="scoreZ":
            minisGlobal[dic] = -20
            maxisGlobal[dic] = 20
        if dic=="scoreAltman":
            minisGlobal[dic] = 0
            maxisGlobal[dic] = 20
        # creating dic to store histograms values
        dicX[dic] = range(int(minisGlobal[dic]),int(maxisGlobal[dic])+1,max(1,int((maxisGlobal[dic]+1-minisGlobal[dic])/nbStepHistogram)))
        dicY[dic] = [0] * nbStepHistogram
        dicY[dic+"global"] = [0] * nbStepHistogram
        dicError[dic] = 0
        dicError[dic+"global"] = 0
    # ANALYZING MERGED FILE
    p = 0
    percent = 10
    total = len(entrepriseData)
    print "analyzing merged file"
    for line in entrepriseData.values:
        # printing progress
        p+=1
        if 100.0*p/total>percent:
            print percent,'%',
            percent+=10
        # getting through the scores
        for dic in dicScore.keys():
            i=0
            try:
#                 print len(dicX[dicScore[dic]])-1, dicX[dicScore[dic]][i+1], line[5+dic]
                while i<len(dicX[dicScore[dic]])-1 and dicX[dicScore[dic]][i+1]<line[5+dic]:
                    i+=1
                dicY[dicScore[dic]][i]+=1   
            except:
                dicError[dicScore[dic]]+=1 
    del entrepriseData
    print ""
    print ""
    # ANALYZING GLOBAL FILE
    print "analyzing global file"
    p = 0
    percent = 10
    total = len(csvScore)
    for line in csvScore.values:
        # printing progress
        p+=1
        if 100.0*p/total>percent:
            print percent,'%',
            percent+=10
        # getting through the scores
        for dic in dicScore.keys():
            i=0
            try:
                while i<len(dicX[dicScore[dic]])-1 and dicX[dicScore[dic]][i+1]<line[2+dic]:
                    i+=1
                dicY[dicScore[dic]+"global"][i]+=1   
            except:
                dicError[dicScore[dic]+"global"]+=1
        
    print ""
    print "errors:", dicError
    print ""   
    
    prepareInput("scoreFile")
    for dic in dicScore.values():
        DrawingTools.createHistogram(x=dicX[dic], y1=dicY[dic], name1=dic+" merged", y2=dicY[dic+"global"], name2=dic+" global",xlabel="score", ylabel="number of entreprises", percent=True, name="Repartition of "+dic, filename="repartition_"+dic)   
    
    interval = time.time() - startTime
    print "done: ", interval, 'sec'

def analyzingEntrepEtab():
    '''
    function that analyzes the etab file.
    1. it imports the paiementfile and extract the entreprises id.
    2. it imports the etab file and computes statistics about entreprises
    3. it prints statistics and creates histograms
    -- IN:
    the function takes no argument
    -- OUT:
    the function returns nothing
    '''
    print "=== Starting Analysis of Entreprises Etablissment ==="
    print ""
    startTime = time.time()
    
    # importing Etab file
    print "retrieving csvEtab file",
    csvEtab = getAndPreprocessCsvEtab()
    print "... done:",
    interval = time.time() - startTime
    print interval, 'sec'
    print ""
    
    column = csvEtab['entrep_id'].values
    # initializing dicEntreprise, dic which will contain info usefull for stats
    dicEntreprise = {}
    for entreprise in column:
        if not dicEntreprise.has_key(entreprise):
            # array containing ['entrep_id',nbEtab,array(capital),array(dateCreation),array(effectif), isInFile(boolean)]
            dicEntreprise[entreprise] = [entreprise,0,[],[],[],False]
    nbEntreprisesGlobal = len(dicEntreprise)      
      
    # importing the BalAG file
    print "retrieving csvBalAG file",
    csvinput = importAndCleanCsv(toPrint=False, ftp = True)
    print "... done:",
    interval = time.time() - startTime
    print interval, 'sec'
    print ""
    
    # joining files
    column = csvinput['entrep_id'].values
    nbEntreprises = len(np.unique(column))
    nbMissingEntreprises = 0
    for entreprise in column:
        if not dicEntreprise.has_key(entreprise):
            # adding the entreprise to te file, and counting the missing entreprises
            dicEntreprise[entreprise] = [entreprise,0,[],[],[]]
            nbMissingEntreprises+=1
        else:
            # putting the boolean to True, the entreprise is in the BalAG file
            dicEntreprise[entreprise][-1] = True
        
    # printing about missing entreprises
    print ""
    print "global entreprises number :", nbEntreprisesGlobal
    print "enterprises number :" , nbEntreprises
    print ""
    print "missing entreprises :",nbMissingEntreprises,"-",100.0*nbMissingEntreprises/nbEntreprises,"%"
    print ""
    
    del csvinput
    
    ## ANALYSIS AND JOINING
    print "Analysing the file"
    nbNanCapital = 0
    nbNanCapitalGlobal = 0
    nbNanDate = 0
    nbNanDateGlobal = 0
    nbNanEffectif = 0
    nbNanEffectifGlobal = 0
    nbLineBalAG = 0
    total = len(csvEtab)
    print total
    percent = 1
    i = 0
    for line in csvEtab.values:
        # printing progress
        i+=1
        if 100.0*i/total>percent:
            print percent,"%",
            percent+=1
            if int(percent)%10==0:
                print ""
        # line = ['entrep_id','capital','DCREN','EFF_ENT'] 
        nbLineBalAG += (1 if dicEntreprise[line[0]][5] else 0)
        # increasing number of Etablissment
        dicEntreprise[line[0]][1]+=1
        # adding info about capital
        if str(line[1]).lower()!="nan":
            dicEntreprise[line[0]][2].append(int(line[1]))
        else:
            nbNanCapitalGlobal+=1
            nbNanCapital+= (1 if dicEntreprise[line[0]][5] else 0)
        # adding info about dateCreation
        if str(line[2]).lower()!="nan":
            dicEntreprise[line[0]][3].append(line[2])
        else:
            nbNanDateGlobal+=1
            nbNanDate+= (1 if dicEntreprise[line[0]][5] else 0)
        # adding info about effectif
        if str(line[3]).lower()!="nan":
            dicEntreprise[line[0]][4].append(int(line[3]))
        else:
            nbNanEffectifGlobal+=1
            nbNanEffectif+= (1 if dicEntreprise[line[0]][5] else 0)
    print "...done"
    print ""
    print "PAIEMENT FILE STATS"
    print "number of Nan capital:",nbNanCapital, "-",100.0*nbNanCapital/nbLineBalAG,"%"
    print "number of Nan date:",nbNanDate, "-",100.0*nbNanDate/nbLineBalAG,"%"
    print "number of Nan effectif:",nbNanEffectif, "-",100.0*nbNanEffectif/nbLineBalAG,"%"
    print ""
    print "GLOBAL FILE STATS"
    print "number of Nan capital:",nbNanCapitalGlobal, "-",100.0*nbNanCapitalGlobal/len(csvEtab),"%"
    print "number of Nan date:",nbNanDateGlobal, "-",100.0*nbNanDateGlobal/len(csvEtab),"%"
    print "number of Nan effectif:",nbNanEffectifGlobal, "-",100.0*nbNanEffectifGlobal/len(csvEtab),"%"
    print ""
    
    ## ANALYSING CONSISTENCY
    nbNoInfo = 0
    nbInconsistentCapital = 0
    nbInconsistentDate = 0
    nbInconsistentEffectif = 0
    nbConsistencyError = 0
    print "Consistency analysis",
    for entreprise in dicEntreprise.keys():
        try:
            if dicEntreprise[entreprise][1] == 0:
                nbNoInfo+=1
                continue
            if len(np.unique(dicEntreprise[entreprise][2]))>1:
                nbInconsistentCapital+=1
            dicEntreprise[entreprise][2] = np.max(dicEntreprise[entreprise][2])
            if len(np.unique(dicEntreprise[entreprise][3]))>1:
                nbInconsistentDate+=1
            dicEntreprise[entreprise][3] = np.unique(dicEntreprise[entreprise][3])[0]
            if len(np.unique(dicEntreprise[entreprise][4]))>1:
                nbInconsistentEffectif+=1
            dicEntreprise[entreprise][4] = np.max(dicEntreprise[entreprise][4])
        except:
            nbConsistencyError+=1
            pass
    print "...done"
    print "   nb of problems:",nbConsistencyError
    print "   nb of missing informations:",100.0*nbNoInfo/len(dicEntreprise),"%"
    print "   nb of inconsistent capital:",100.0*nbInconsistentCapital/len(dicEntreprise),"%"
    print "   nb of inconsistent date:",100.0*nbInconsistentDate/len(dicEntreprise),"%"
    print "   nb of inconsistent effectif:",100.0*nbInconsistentEffectif/len(dicEntreprise),"%"
    print ""
    
    print "computing analysis",
    # initializing arrays
    dateX = range(1950,2016)
    dateY = [0]*len(dateX)
    dateYGlobal = [0]*len(dateX)
    capitalX = range(0,12)
    capitalY = [0]*len(capitalX)
    capitalYGlobal = [0]*len(capitalX)
    effectifX = range(0,6)
    effectifY = [0]*len(effectifX)
    effectifYGlobal = [0]*len(effectifX)
    nbDateError = 0
    for entreprise in dicEntreprise.keys():
        # handling capitals
        i=0
        capital = dicEntreprise[entreprise][2]
        while i<len(capitalX)-1 and capital>10**(capitalX[i]):
            i+=1
        capitalYGlobal[i]+=1
        capitalY[i]+=(1 if dicEntreprise[entreprise][5] else 0)
        # handling dates
        i=0
        try:
            date = dicEntreprise[entreprise][3][:4]
#             print date
            date = int(date)
            while i<len(dateX)-1 and date>dateX[i]:
                i+=1
            dateYGlobal[i]+=1
            dateY[i]+=(1 if dicEntreprise[entreprise][5] else 0)
        except:
            nbDateError+=1
        # handling effectifs
        i=0
        effectif = dicEntreprise[entreprise][4]
        while i<len(effectifX)-1 and effectif>10**(effectifX[i]):
            i+=1
        effectifYGlobal[i]+=1
        effectifY[i]+=(1 if dicEntreprise[entreprise][5] else 0)
    # handling output 
    prepareInput("etabFile")
    DrawingTools.createHistogram(x=capitalX,y1=capitalY,y2=capitalYGlobal,
                                 name1 = "BalAG stats", name2 = "Global stats", percent=True,
                                 xlabel="capitals",ylabel="number of entreprise (%)",
                                 name="Repartition of capitals",
                                 filename="01_repartitionCapital")
    DrawingTools.createHistogram(x=dateX,y1=dateY,y2=dateYGlobal,
                                 name1 = "BalAG stats", name2 = "Global stats", percent=True,
                                 xlabel="dates",ylabel="number of entreprise (%)",
                                 name="Repartition of the dates",
                                 filename="01_repartitionDate")
    DrawingTools.createHistogram(x=effectifX,y1=effectifY,y2=effectifYGlobal,
                                 name1 = "BalAG stats", name2 = "Global stats", percent=True,
                                 xlabel="effectifs",ylabel="number of entreprise (%)",
                                 name="Repartition of the effectifs",
                                 filename="01_repartitionEffectif")
    print "...done"
    print "nb Date Error:",nbDateError
    print ""
    
    interval = time.time() - startTime
    print "done: ", interval, 'sec'
      
''' V - Creating 2d histograms '''
      
def AnalyzingEcheanceOverMontant(csvinput):
    '''
    function that computes 2d hist seeing how variables are correlated.
    -- IN:
    csvinput : the BalAG file cleaned with at least the columns 'entrep_id','montantPieceEur','datePiece','dateEcheance'
#     csvetab : the Etab file with at least the columns 'entrep_id','DCREN','effectif','capital'
#     csvscore : the Score file with at least the columns 'entrep_id','dateModif','scoreZ','scoreCH','scoreSolv','scoreAltman'
    -- OUT:
    the function returns nothing
    '''    
    print "analyzing montant over echeance"
    # setting the array for drawing the histogram
    y0 = []
    y1 = []
    ya = []
    yb = []
    nbError = 0
    nbLine = len(csvinput)
    print nbLine
    percent = 10
    i = 0
    for line in csvinput[['montantPieceEur','datePiece','dateEcheance']].values:
        i+=1
        if 100.0*i/nbLine>percent:
            print percent,"%",
            percent+=10 
        try :
            d0 = datetime.datetime.strptime(line[1], '%Y-%m-%d').date()
            d1 = datetime.datetime.strptime(line[2], '%Y-%m-%d').date()
            a = line[0]
            b = (d1-d0).days
            y0.append(a)
            y1.append(b)
            if a < 100000 and b < 100:
                ya.append(a)
                yb.append(b)
        except:
            nbError += 1
    print ""
    print "... done"
    print ""
    print "nb errors :",100.0*nbError/nbLine
    print ""
    DrawingTools.createHistogram2D(y0=y0, y1=y1, xlabel="montant facture", ylabel="nombre de jours d'échéance", name="Echéance selon Montant", filename="01_montant_echeance")
    DrawingTools.createHistogram2D(y0=ya, y1=yb, xlabel="montant facture", ylabel="nombre de jours d'échéance", name="Echéance selon Montant", filename="01_montant_echeance_zoom")
            
def AnalyzingEffectifOverCapital(csvetab):
    '''
    function that computes 2d hist seeing how variables are correlated.
    -- IN:
    csvetab : the Etab file with at least the columns 'entrep_id','DCREN','effectif','capital'
    -- OUT:
    the function returns nothing
    '''    
    y0 = []
    y1 = []
    ya = []
    yb = []
    nbError = 0
    percent = 10
    i=0
    total = len(csvetab)
    for line in csvetab[['EFF_ENT','capital']].values:
        i+=1
        if 100.0*i/total>percent:
            print percent,"%",
            percent+=10
        try:
            a = int(line[1])
            b = int(line[0])
            y0.append(a)
            y1.append(b)
            if a>0 and b>0:
                ya.append(a)
                yb.append(b)
        except:
            nbError += 1
    print ""
    print "... done"
    print ""
    print "nbError :",100.0*nbError/total
    print ""
    DrawingTools.saveHistogram2D(y0=y0, y1=y1, xlabel="capital", ylabel="effectif", name="Effectif selon capital (complet)", filename="02_effectif_over_capital")
    DrawingTools.saveHistogram2D(y0=ya, y1=yb, xlabel="capital", ylabel="effectif", name="Effectif selon capital (nettoyé)", filename="02_effectif_over_capital_clean")
          
''' VI - Scripts and Global Functions '''
def importAndCleanCsv(toPrint = False, ftp = False, toSave = False):
    '''
    Function that process the data:
    importing, cleaning and analysing and eventually save the dataframe
    -- IN
    toPrint : boolean to show the log of the cleaning process (boolean) default: True
    ftp : boolean to choose between local and remote data (boolean) default: False
    toSave : boolean that settles if the final dataframe must be saved (boolean) default: False
    -- OUT 
    csvinput : the cleaned dataframe (pandas.Dataframe)
    '''
    print "Extracting the BalAG dataframe"
    startTime = time.time()
    # importing the csv file and creating the datframe
    if(ftp):
        csvinput = importFTPCsv(addPaidBill=True,dtype=Constants.dtype)
    else:
        csvinput = importCsv(addPaidBill=True,dtype=Constants.dtype)
    # prepricessing of the dates
    csvinput['datePiece'] = pd.to_datetime(csvinput['datePiece'], format='%Y-%m-%d', errors='coerce') 
    csvinput['dateEcheance'] = pd.to_datetime(csvinput['dateEcheance'], format='%Y-%m-%d', errors='coerce') 
    csvinput['dateDernierPaiement'] = pd.to_datetime(csvinput['dateDernierPaiement'], format='%Y-%m-%d', errors='coerce')   
    # preprocess the dataframe
    csvinput = cleaningDates(csvinput)
    csvinput = cleaningMontant(csvinput)
    csvinput = cleaningEntrepId(csvinput)
    Utils.printTime(startTime)
    print ""
    if toSave:
        # saving the resulting Dataframe
        os.chdir(os.path.join("..",".."))
        with open("cameliaBalAGKevin.csv","w") as fichier:
            fichier.write(csvinput.to_csv(path_or_buff = None ,sep="\t"))
        print "file saved =>",
        Utils.printTime(startTime)
        print ""
    return csvinput

def importAndAnalyseCsv(toPrint = False, toDrawGraph = True, ftp = False):
    '''
    Function that process the data:
    importing, cleaning and analyzing
    and returns the dataframe
    -- IN
    toPrint : boolean to show the log of the cleaning process (boolean) default: False
    toDrawGraph : boolean to export the graphs of the analysis process (boolean) default: True
    ftp : boolean to choose between local and remote data (boolean) default: False
    -- OUT 
    csvinput : the cleaned dataframe (pandas.Dataframe)
    '''
    print "Extracting the BalAG dataframe"
    startTime = time.time()
    # importing the csv file and creating the datframe
    if(ftp):
        csvinput = importFTPCsv(addPaidBill=True,dtype=Constants.dtype)
    else:
        csvinput = importCsv(addPaidBill=True,dtype=Constants.dtype) 
    # prepricessing of the dates
    csvinput['datePiece'] = pd.to_datetime(csvinput['datePiece'], format='%Y-%m-%d', errors='coerce') 
    csvinput['dateEcheance'] = pd.to_datetime(csvinput['dateEcheance'], format='%Y-%m-%d', errors='coerce') 
    csvinput['dateDernierPaiement'] = pd.to_datetime(csvinput['dateDernierPaiement'], format='%Y-%m-%d', errors='coerce')   
    # preprocess the dataframe
    csvinput = cleaningDates(csvinput)
    csvinput = cleaningMontant(csvinput)
    csvinput = cleaningEntrepId(csvinput)
    if toDrawGraph:
        prepareInput()
    # analysing the dateframe
    analyzingDates(csvinput, toSaveGraph=toDrawGraph)
    analyzingEntrepId(csvinput, toSaveGraph=toDrawGraph)
    analyzingMontant(csvinput, toSaveGraph=toDrawGraph)
#     analyzingOthers(csvinput)
#     analyzingComplete(csvinput, toSaveGraph=toDrawGraph)
    # ploting the graphs
    plt.show()
    Utils.printTime(startTime)
    os.chdir("..")
    print ""
    return csvinput

def prepareInput(filename="analysis"):
    """
    functions that prepare the directory to export the graph during the analyzing steps.
    -- IN
    filename : name of the directory to create (string) default : analysis
    -- OUT
    returns nothing
    """
    os.chdir(os.path.join("..","..",filename))
    n = len(os.listdir("."))
    os.mkdir(str(n)+"_"+time.strftime('%d-%m-%y_%H-%M',time.localtime()))
    os.chdir(str(n)+"_"+time.strftime('%d-%m-%y_%H-%M',time.localtime())) 
    
def printLastGraphs(filename = "analysis"):
    """
    function that seek for all .txt files in the last folder of export and transform them
    into graph through plotly and export them in png.
    -- IN
    takes no argument
    -- OUT
    returns nothing
    """
    print "Printing graphs"
    os.chdir(os.path.join("..","..",filename))
    lastdir = os.listdir(".")[-1]
    print lastdir
    os.chdir(lastdir)
    dirs = os.listdir("../"+lastdir)
    for direct in dirs:
        tab = direct.split(".")
        if tab[1]=="txt":
            print direct,
            DrawingTools.drawHistogramFromFile(tab[0]) 
            print "...done"
        elif tab[1]=="hist2d":
            print direct,
#             DrawingTools.drawHistogramFromFile(tab[0],typeHist="2d") 
            DrawingTools.drawLargeHistogram2D(tab[0])
            print "...done"

def printConfiguration(globalConfig = False):
    '''
    function that print the configuration of the launch according to the constants in
    the Constants python module.
    -- IN
    globalConfig : booleanthat settles if the functions displays all the booleans (True)
                    or just the changes from the default launch configuration (False) (boolean) default : False
    -- OUT
    returns nothing
    '''
    ### CLEANING ID
    print "CLEANING ID"
    # clean according to being an int
    if globalConfig or not bclnIdIntFormat:
        print "clean according to being an int :",bclnIdIntFormat
    # clean according to the number of bills
    if globalConfig or bclnIdMinimalBillsNumber:
        print "clean according to the number of bills :",bclnIdMinimalBillsNumber,
        if bclnIdMinimalBillsNumber:
            print ":",clnIdMinimalBillsNumber
        else:
            print ""
    # clean according to the value of the ID
    if globalConfig or not bclnIdMinimalIdValue or clnIdMinimalIdValue!=1:
        print "clean according to the minimal value of the ID :",bclnIdMinimalIdValue,
        if bclnIdMinimalIdValue:
            print ":",clnIdMinimalIdValue
        else:
            print ""
    if globalConfig or bclnIdMaximalIdValue:
        print "clean according to the maximl value of the ID :",bclnIdMaximalIdValue,
        if bclnIdMaximalIdValue:
            print ":",clnIdMaximalIdValue
        else:
            print ""
    
    
    ### CLEANING DATES
    print "CLEANING DATES"
    # clean according to the date format
    if globalConfig or not bclnDatePieceFormat:
        print "clean according to being a proper date for DatePiece :",bclnDatePieceFormat
    if globalConfig or not bclnDateEcheanceFormat:
        print "clean according to being a proper date for DateEcheance :",bclnDateEcheanceFormat
    if globalConfig or bclnDateDernierPaiementFormat:
        print "clean according to being a proper date for DateDernierPaiement :",bclnDateDernierPaiementFormat
    # clean according to the consistence of the dates (piece < echeance and piece < dernierpaiement)
    if globalConfig or not bclnDateInconsistent:
        print "clean according to the consistence of the dates :",bclnDateInconsistent
    # clean according to a maximal gap in month between dates
    if globalConfig or bclnDateMonthDiff:
        print "clean according to a maximal gap in month between dates :",bclnDateMonthDiff,
        if bclnDateMonthDiff:
            print ":",clnDateMonthDiff
        else:
            print ""
    # clean according to a minimal date
    if globalConfig or bclnDateMinimalDate:
        print "clean according to a minimal date :",bclnDateMinimalDate,
        if bclnDateMinimalDate:
            print ":",clnDateMinimalDate
        else:
            print ""
    # clean according to a maximal date
    if globalConfig or bclnDateMaximalDate:
        print "clean according to a maximal date :",bclnDateMaximalDate,
        if bclnDateMaximalDate:
            print ":",clnDateMaximalDate
        else:
            print ""
    
    
    ### CLEANING MONTANTS
    print "CLEANING MONTANTS"
    # clean according to being an int
    if globalConfig or not bclnMontantIntFormat:
        print "clean according to being an int :",bclnMontantIntFormat
    # clean according to being a positive value
    if globalConfig or not bclnMontantNonNegativeValue:
        print "clean according to being a positive value :",bclnMontantNonNegativeValue
    # clean according to being an non-zero value
    if globalConfig or not bclnMontantNonZeroValue:
        print "clean according to being an non-zero value :",bclnMontantNonZeroValue
    # clean according to a minimal value of the montant
    if globalConfig or bclnMontMinimalValue:
        print "clean according to a minimal value of the montant :",bclnMontMinimalValue,
        if bclnMontMinimalValue:
            print ":",clnMontMinimalValue
        else:
            print ""
    # clean according to a maximal value of the montant
    if globalConfig or bclnMontMaximalValue:
        print "clean according to a maximal value of the montant :",bclnMontMaximalValue,
        if bclnMontMaximalValue:
            print ":",clnMontMaximalValue
        else:
            print ""
    
    
    ### CLEANING MONTANT LITIGE
    print "CLEANING MONTANT LITIGE"
    # clean according to a zero-valued montantLitige
    if globalConfig or bclnMontantLitigeNonZero:
        print "clean according to a zero-valued montantLitige :",bclnMontantLitigeNonZero
      

