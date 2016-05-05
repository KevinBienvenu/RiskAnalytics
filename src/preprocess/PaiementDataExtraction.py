# -*- coding: utf-8 -*-
'''
Created on 5 Apr 2016

@author: Kévin Bienvenu

Module containing functions extracting informations from the
file 'cameliaBalAG.csv' 

The module is divided in three parts :

=== Part I : Importation of the data
importCsv(filename,sep,usecols,addPaidBill) : import a local csv file into a pandas.Dataframe 
importFTPCsv(filename,sep,function,usecols,addPaidBill) : import a remote csv.gz file into a pandas.Dataframe

=== Part II : Cleaning Functions
cleaningEntrepId(csvinput, toPrint) : clean the dataframe according to the EntrepId column
cleaningDates(csvinput, toPrint) : clean the dataframe according to the dates columns
cleaningMontant(csvinput, toPrint) : clean the dataframe according to the montantPieceEur column
cleaningOther(csvinput, toPrint) : clean the dataframe according to the other columns

=== Part III - Analysing Functions
analyzingEntrepId(csvinput, toSaveGraph, toDrawGraphOld) : analyzes and draws graphs about the EntrepId column
analyzingDates(csvinput, toSaveGraph, toDrawGraphOld) : analyzes and draws graphs about the dates columns
analyzingMontant(csvinput, toSaveGraph, toDrawGraphOld) : analyzes and draws graphs about the montantPieceEur column
analysingOther(csvinput, toSaveGraph, toDrawGraphOld) : analyzes and draws graphs about the other columns
analysingComplete(csvinput, toSaveGraph, toDrawGraphOld) : analyzes and draws graphs about all the columns
analysingIdCorresponding(csvinput, fileToCompare) : compares the EntrepId column to another file of the database

=== PART IV - Analysing functions using other files


=== PART V - Scripts and Global Functions
importAndCleanCsv(toPrint, ftp) : imports the local or remote file and cleans it
importAndAnalyseCsv(toPrint, todoAnalysis, toDrawGraph, ftp) : imports, cleans and analyzes the csv file
prepareDirForGraphExport() : prepare the directories to export the graph from the analysis
printLastGraphs() : transforms the last export of graphs from .txt to .png
sideAnalysis(ftp) : compare the csv file to other files on the ftp server



'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import datetime
import math
import os
import time

import plotly.plotly as py
import plotly.graph_objs as go

import Utils
import FTPTools
import DrawingTools
import Constants
from Constants import *



''' I - Importation of the data  '''
def importCsv(filename = 'cameliaBalAG_extraitRandom.csv',sep='\t',usecols = None,addPaidBill = False):
    '''
    function that imports the content of the csv file into the global
    variable csvinput. The csv file must be stored stored locally.
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
        csvinput = pd.read_csv(filename,sep=sep,usecols = usecols)
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

def importFTPCsv(filename = 'cameliaBalAG.csv.gz',sep='\t',function="ftplib",usecols = None,addPaidBill = False):
    '''
    function that imports the content of the csv file into the global
    variable csvinput. the file is downloaded from the remote ftp via
    one of the two functions in FTPTools according to the input
    -- IN
    filename : name of the file, including the extension (string) default : 'cameliaBalAG.csv.gz'
    sep : separator for the pandas read_csv function (regexp) default : '\t'
    function : name of the importation function to be used "ckftp" or "ftplib" (string) default : "ftplib"
    usecols : array containing the names of the column we want to import, 
        if None import all (string[] or None) default : None
    addPaidBill : boolean that settles if we add a column PaidBill (boolean) default : False
    -- OUT
    returns the dataframe out of the csv file
    '''
    # importing the remote file
    if(function=="ckftp"):
        csvinput = FTPTools.retrieveCKFtp(filename, usecols=usecols, dtype=Constants.dtype, compression="gz")
    elif(function=="ftplib"):
        csvinput = FTPTools.retrieveFtplib(filename, usecols=usecols, dtype=Constants.dtype, compression="gz")
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


''' II - Cleaning Functions '''
def cleaningEntrepId(csvinput, toPrint = True):
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
    if toPrint:
        print ""
    # checking if everything is all right with the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
        print ""
        return csvinput
    if not 'entrep_id' in csvinput.columns:
        print "error : No column to analyse - entrep_id"
        return csvinput
    # initializing variables
    #    dictionary linking entrep_id to concerned rows
    entrepriseToRowsDict = {}
    #    dictionary linking entrep_id to the number of bills
    entrepriseToBillNumber = {}
    entrepriseToDrop = []
    rowToDrop = []
        
    # filling the previous dictionaries
    ind = 0
    comptErrorFormat = 0
    for entreprise in csvinput['entrep_id'].values:
        if Constants.bclnIdIntFormat:
            if not Utils.checkIntFormat(entreprise, True, True):
                rowToDrop.append(ind)
                ind += 1
                comptErrorFormat += 1
                continue
        if not entrepriseToRowsDict.has_key(entreprise):
            entrepriseToRowsDict[entreprise] = []
        entrepriseToRowsDict[entreprise].append(ind)
        ind += 1
    for entry in entrepriseToRowsDict.keys():
        entrepriseToBillNumber[entry] = len(entrepriseToRowsDict[entry])
    if toPrint:
        print "total number of enterprises :", len(entrepriseToBillNumber)
        print ""
        if Constants.bclnIdIntFormat:
            print "enterprises IDs must be positive int"
            print "   number of errors :",comptErrorFormat,"-",100.0*comptErrorFormat/len(csvinput),"%" 
            print ""
        
    # preprocess according to the id
    if Constants.bclnIdMinimalIdValue or Constants.bclnIdMaximalIdValue:
        compt = 0
        for entreprise in entrepriseToBillNumber.keys():
            if Constants.bclnIdMinimalIdValue and entreprise<Constants.clnIdMinimalIdValue:
                entrepriseToDrop.append(entreprise)
                compt += 1
            if Constants.bclnIdMaximalIdValue and entreprise>Constants.clnIdMaximalIdValue:
                entrepriseToDrop.append(entreprise)
                compt += 1 
        if toPrint:
            s= "enterprises ids must be "+("above "+str('%.1e' % Constants.clnIdMinimalIdValue)+" " if Constants.bclnIdMinimalIdValue else "")
            s = s + ("and " if Constants.bclnIdMaximalIdValue and Constants.bclnIdMinimalIdValue else "")
            s = s + ("below "+str('%.1e' % Constants.clnIdMaximalIdValue)+" " if Constants.bclnIdMaximalIdValue else "")
            print s
            print "   incorrect Ids :",compt,"-",100.0*compt/len(entrepriseToRowsDict),"%"
            print ""
            
    #preprocess according to the number of bills
    if Constants.bclnIdMinimalBillsNumber:
        compt = 0
        for entreprise in entrepriseToBillNumber.keys():
            if entrepriseToBillNumber[entreprise] < Constants.clnIdMinimalBillsNumber:
                entrepriseToDrop.append(entreprise)
                compt += 1
        if toPrint:
            print "enterprises must have at least", Constants.clnIdMinimalBillsNumber,"bills"
            print "   enterprises without enough bills :",compt,"-",100.0*compt/len(entrepriseToRowsDict),"%"
            print ""
        
    #performing preprocess
    for entreprise in entrepriseToDrop:
        for row in entrepriseToRowsDict[entreprise]:
            rowToDrop.append(row)
    csvinput.drop(csvinput.index[rowToDrop], inplace = True)
    if toPrint:
        print "==> preprocess completed"
        print "   number of deleted rows :", len(rowToDrop)
        print "   new amount of rows :", len(csvinput)
        print ""
        print ""
    return csvinput

def cleaningDates(csvinput, toPrint = True):
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
    if toPrint:
        print ""
         
    # checking if everything is all right with the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
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
    # importing column
    column = np.array(csvinput[['datePiece','dateEcheance','dateDernierPaiement']].values)
    nbEntries = len(column.T[0])
    rowToDrop = []
    
    # counting problems
    compt = 0
    comptPiece = 0
    comptEcheance = 0
    comptDernierPaiement = 0
    comptInconsistent = 0
    comptMonthDiff = 0
    comptMinimal = 0
    comptMaximal = 0
    ind = 0
    for c in column:
        # validating the dates
        s = Utils.validateDate(c[0], c[1], c[2])      
        if len(s)>0:
            compt += 1
            rowToDrop.append(ind)
        if "pieceFormat" in s:
            comptPiece += 1
        if "echeanceFormat" in s:
            comptEcheance += 1
        if "dernierPaiementFormat" in s:
            comptDernierPaiement += 1
        if "inconsistentDates" in s:
            comptInconsistent += 1
        if "monthDiff" in s:
            comptMonthDiff += 1
        if "minimalDate" in s:
            comptMinimal += 1
        if "maximalDate" in s:
            comptMaximal += 1
        ind += 1
    if toPrint:
        print "date format errors:", compt, "-", 100.0*compt/(nbEntries),"%"
        if Constants.bclnDatePieceFormat:
            print "   datePiece format errors:", comptPiece,"-",100.0*comptPiece/nbEntries,"%"
        if Constants.bclnDateEcheanceFormat:
            print "   dateEcheance format errors:", comptEcheance,"-",100.0*comptEcheance/nbEntries,"%"
        if Constants.bclnDateDernierPaiementFormat:
            print "   dateDernierPaiement format errors:", comptDernierPaiement,"-",100.0*comptDernierPaiement/nbEntries,"%"
        if Constants.bclnDateInconsistent:
            print "   inconsistent dates errors:", comptInconsistent,"-",100.0*comptInconsistent/nbEntries,"%"
        if Constants.bclnDateMonthDiff:
            print "   months difference errors:", comptMonthDiff,"-",100.0*comptMonthDiff/nbEntries,"%"
        if Constants.bclnDateMinimalDate:
            print "   minimal dates errors:", comptMinimal,"-",100.0*comptMinimal/nbEntries,"%"
        if Constants.bclnDateMaximalDate:
            print "   maximal dates errors:", comptMaximal,"-",100.0*comptMaximal/nbEntries,"%"
        print ""
        
    
    # preprocess data
    csvinput.drop(csvinput.index[rowToDrop], inplace = True)
    if toPrint:
        print "==> preprocess completed"
        print "   number of deleted rows :", len(rowToDrop)
        print "   new amount of rows :", len(csvinput)
        print ""
        print ""
            
    return csvinput

def cleaningMontant(csvinput, toPrint = True):
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
    if toPrint:
        print ""
    # validating the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
        print ""
        return csvinput
    if not 'montantPieceEur' in csvinput.columns:
        print "error : No column to analyse - montantPieceEur"
        return csvinput 
    nbEntries = len(csvinput['montantPieceEur'])
    rowToDrop = [] 
    ind = 0
    nbMaxi = 0
    nbMini = 0
    nbError = 0
    for c in csvinput['montantPieceEur']:
        s = Utils.validateMontant(c)
        if len(s)>0:
            rowToDrop.append(ind)
        if s=="format":
            nbError +=1
        if s=="minimal":
            nbMini +=1
        if s=="maximal":
            nbMaxi +=1
        ind += 1
    if toPrint and Constants.bclnMontantIntFormat:
        print "the montant values must be integers", \
              "and non negative" if Constants.bclnMontantNonNegativeValue else "", \
              "and non zero values" if Constants.bclnMontantNonZeroValue else ""
        print "   number of invalid montants:", nbError,"-",100.0*nbError/nbEntries,"%"
    
    if toPrint and (Constants.bclnMontMaximalValue or Constants.bclnMontMinimalValue):
        s= "the value of montants must be "+("above "+str(Constants.clnMontMinimalValue)+" " if Constants.bclnMontMinimalValue else "")
        s = s + ("and " if Constants.bclnMontMaximalValue and Constants.bclnMontMinimalValue else "")
        s = s + ("below "+str(Constants.clnMontMaximalValue)+" " if Constants.bclnMontMaximalValue else "")
        print s
        if Constants.bclnMontMinimalValue:
            print "   number of montant value below the minimal value :",nbMini,"-",100.0*nbMini/nbEntries, "%"
        if Constants.bclnMontMaximalValue:
            print "   number of montant value above the maximal value :",nbMaxi,"-",100.0*nbMaxi/nbEntries, "%"
        print ""
        
    # preprocess data
    if(len(rowToDrop)>0):
        csvinput.drop(csvinput.index[rowToDrop], inplace = True)
    if toPrint:
        print "==> preprocess completed"
        print "  number of deleted rows :", len(rowToDrop)
        print "  new amount of rows :", len(csvinput)
        print ""
        print ""
            
    return csvinput        
    
def cleaningOther(csvinput, toPrint = True):
    '''
    function that cleans the content of the column 'montantLitige'
    according to the behavior described in Constants.
    -- IN
    csvinput : pandas dataframe containing the Payment csv file (dataframe)
    toPrint : boolean that settles the display of computed informations (boolean) (default: False)
    -- OUT
    returns csvinput (dataframe)
    '''        
    print "=== Starting Cleaning of montantLitige column ==="
    if toPrint:
        print ""
    # validating the input
    if csvinput is None or len(csvinput)==0:
        print "No Data Remaining"
        print ""
        return csvinput
    if not 'montantLitige' in csvinput.columns:
        print "error : No column to analyse - montantLitige"
        return csvinput 
    # importing column
    column = csvinput['montantLitige']
    nbEntries = len(column)
    rowToDrop = []
    
    if Constants.bclnMontantLitigeNonZero:
        ind = 0
        compt = 0
        for c in column:
            if c==0:
                compt += 1
                rowToDrop.append(ind)
            ind += 1
        if toPrint:
            print "montant litiges values must be non-zero values"
            print "   number of litiges that equals zero",compt,"-",100.0*compt/nbEntries,"%"
    
    # preprocess data
    if(len(rowToDrop)>0):
        csvinput.drop(csvinput.index[rowToDrop], inplace = True)
    if toPrint:
        print "==> preprocess completed"
        print "  number of deleted rows :", len(rowToDrop)
        print "  new amount of rows :", len(csvinput)
        print ""
        print ""
            
    return csvinput  
       
''' III - Analyzing Functions '''
def analyzingEntrepId(csvinput, toSaveGraph = False, toDrawGraphOld = False):
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
        print "wrong columns"
        print ""
        return
    
    # importing column
    column = csvinput['entrep_id'].values

    if len(column) ==0 :
        print "no result to analyse"
        print ""
        return
    
    # initializing variables
    #     setting size variables
    nbEntreprises = len(np.unique(column))
    nbEntries = len(column)
    #    array specifying the number of bills for an enterprise
    #    the i-th value is for the i-th enterprise in the dictionary
    print "enterprises number :" , nbEntreprises
    print "mean bills number by enterprise :", nbEntries/nbEntreprises
    print "enterprise minimal id :", min(column)
    print "enterprise maximal id :", max(column)
    print ""
    
    # initializing variables
    #    dictionary linking entrep_id to concerned rows
    entrepriseToRowsDict = {}
    #    dictionary linking entrep_id to the number of bills
    entrepriseToBillNumber = {}
    # filling the previous dictionaries
    ind = 0
    for entreprise in csvinput['entrep_id'].values:
        if not entrepriseToRowsDict.has_key(entreprise):
            entrepriseToRowsDict[entreprise] = []
        entrepriseToRowsDict[entreprise].append(ind)
        ind += 1
    for entry in entrepriseToRowsDict.keys():
        entrepriseToBillNumber[entry] = len(entrepriseToRowsDict[entry])
        
    print "maximum bills number value: ",np.max(entrepriseToBillNumber.values())
    print "minimum bills number value: ",np.min(entrepriseToBillNumber.values())
    print ""    
    
    # about the repartition of ids
    repartitionIdArray = [0]*(int)(math.log10(max(column))*Constants.anaIdLogCoefficientBillNumber+1)
    repartitionBillNumberArray = [0]*(int)(math.log10(max(entrepriseToBillNumber.values()))*Constants.anaIdLogCoefficientBillNumber+1)
    for e in entrepriseToBillNumber.keys():
        repartitionIdArray[(int)(math.log10(e+1)*Constants.anaIdLogCoefficientBillNumber)] += 1
        repartitionBillNumberArray[(int)(math.log10(entrepriseToBillNumber[e])*Constants.anaIdLogCoefficientBillNumber)] += 1
    if toSaveGraph:
        # I - Distribution of the Id
        nbStepGraph = len(repartitionIdArray)
        xlabel = ["e^" + str(1.0*i/Constants.anaIdLogCoefficientBillNumber) for i in range(nbStepGraph)]
        DrawingTools.createHistogram(x=xlabel, y1=repartitionIdArray, typeyaxis='log',
                                     xlabel="Valeur de l'identifiant", ylabel="Nombre d'entreprises", 
                                     name="Distribution des identifiants", filename="01_IdIdentifiants")        
        # II - Distribution of the number of bills
        nbStepGraph = len(repartitionBillNumberArray)
        xlabel = ["e^" + str(1.0*i/Constants.anaIdLogCoefficientBillNumber) for i in range(nbStepGraph)]
        DrawingTools.createHistogram(xlabel, y1=repartitionBillNumberArray, typeyaxis='log',
                                     xlabel="Nombre de factures", ylabel="Nombre d'entreprises", 
                                     name="Distribution du nombre de factures", filename="02_IdNombreFactures")
    # dealing with the old graph displaying
    if toDrawGraphOld:
        # initializing graph displaying
        plt.figure(Constants.figureId, figsize = Constants.figsize)
        Constants.figureId += 1
        plt.suptitle("Enterprises IDs Analysis", fontsize=16)
        Constants.graphId = 1
        
        #####################################################################
        # Drawing the repartition of enterprises according to a bill number #
        #####################################################################
        # pre-processing
        nbStepGraph = len(repartitionBillNumberArray)
        labelArray = [0] * nbStepGraph
        for i in range(nbStepGraph):
            labelArray[i] = "%.0f" % 10**(1.0*i/Constants.anaIdLogCoefficientBillNumber)
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeEnterprises+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),repartitionBillNumberArray,1.0,color=Constants.colorOrange)
        plt.title("repartition of the bill numbers", fontsize=12)
        ax1.set_yscale('log')
        plt.xticks(np.arange(nbStepGraph),labelArray,rotation = 70)
        plt.ylabel("number of enterprises", fontsize=10)
        
        ##############################################
        # Drawing the repartition of enterprises IDs #
        ##############################################
        # pre-processing
        nbStepGraph = len(repartitionIdArray)
        labelArray = [0] * nbStepGraph
        for i in range(nbStepGraph):
            labelArray[i] = "%.1e" %  10**(1.0*i/Constants.anaIdLogCoefficientBillNumber) if i%Constants.anaIdLogCoefficientIds==0 else ""
        # displaying
        ax1 = plt.subplot(Constants.subplotShapeEnterprises+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),repartitionIdArray,1.0,color=Constants.colorGreen)
        plt.title("repartition of the IDs values", fontsize=12)
        plt.xticks(np.arange(nbStepGraph),labelArray,rotation = 70)
        ax1.set_yscale('log')
        plt.xlabel("value of ID", fontsize=10)
        plt.ylabel("number of enterprises", fontsize=10)

def analyzingDates(csvinput, toSaveGraph = False, toDrawGraphOld = False):
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
    
    column = csvinput[['datePiece','dateEcheance','dateDernierPaiement','paidBill']].values
    
    if len(column) ==0 :
        print "no result to analyse"
        print ""
        return
    
    # analysis of the datePiece only for paid bills
    # columnDatePaid is the temporary vector with all converted dates ['datePiece', 'dateEcheance', 'dateDernierPaiement'] for paid bills
    print "Analysis of the paid bills"
    columnDatePaid = [[datetime.datetime.strptime(c[0], '%Y-%m-%d').date(),
                   datetime.datetime.strptime(c[1], '%Y-%m-%d').date(),
                   datetime.datetime.strptime(c[2], '%Y-%m-%d').date()] for c in column if c[3]]
    if len(columnDatePaid)>0:
        # computing minimal and maximal dates
        minDate = np.min(column, axis = 0)
        maxDate = np.max(column, axis = 0)
        minDatePaid = np.min(columnDatePaid, axis = 0)
        maxDatePaid = np.max(columnDatePaid, axis = 0)
        nbBills = len(column)
        nbPaidBills = len(columnDatePaid)
        
        print ""
        print "    total amount of paid bills:", nbPaidBills,"-",100.0*nbPaidBills/nbBills,"%"
        print ""
        print "    DatePiece column:"
        print "        min:", minDatePaid[0]
        print "        max:", maxDatePaid[0]
        print "    DateEcheance column:"
        print "        min:", minDatePaid[1]
        print "        max:", maxDatePaid[1]
        print "    DateDernierPaiement column:"
        print "        min:", minDatePaid[2]
        print "        max:", maxDatePaid[2]
        print ""
        print "    mean day number between Piece and Echeance :", (np.mean([(c[1]-c[0]).days for c in columnDatePaid]))
        print "    mean day number between Piece and DernierPaiement :", (np.mean([(c[2]-c[0]).days for c in columnDatePaid]))
        print "    mean day number between Echeance and DernierPaiement :", (np.mean([(c[2]-c[1]).days for c in columnDatePaid]))
        print ""
        print "    ratio of on-time paid bills :", 100.0*len([1 for c in columnDatePaid if c[2]<=c[1]])/nbPaidBills,"%"
        print "    mean delay between Echeance and DernierPaiement for late paid bills:", (np.mean([(c[2]-c[1]).days for c in columnDatePaid  if c[2]>c[1]]))
        print ""
    else:
        print "    no paid bills"
    
    # analysis of the datePiece only for unpaid bills
    columnDateUnpaid = [[datetime.datetime.strptime(c[0], '%Y-%m-%d').date(),datetime.datetime.strptime(c[1], '%Y-%m-%d').date()] for c in column if not c[3]]
    print "Analysis of the unpaid bills"
    if len(columnDateUnpaid)>0:
        # computing minimal and maximal dates for unpaid bills
        minDateUnpaid = np.min(columnDateUnpaid, axis = 0)
        maxDateUnpaid = np.max(columnDateUnpaid, axis = 0)
        nbUnpaidBills = nbBills - nbPaidBills
        
        print ""
        print "    total amount of unpaid bills:", nbUnpaidBills,"-",100.0*nbUnpaidBills/nbBills,"%"
        print ""
        print "    DatePiece column:"
        print "        min:", minDateUnpaid[0]
        print "        max:", maxDateUnpaid[0]
        print "    DateEcheance column:"
        print "        min:", minDateUnpaid[1]
        print "        max:", maxDateUnpaid[1]
        print ""
        print "    mean day number between Piece and Echeance :", (np.mean([(c[1]-c[0]).days for c in columnDateUnpaid]))
        print ""
    else:
        print "    no unpaid bills"
    
    if toSaveGraph:
        # PRE PROCESSING 
        # computing the number of step for the bar-chart
        #    here by default Constants.anaDateStepMonthGraph = 12
        nbStepGraphYear = Utils.nbMonthsBetweenDates(maxDate[0], minDate[0])/Constants.anaDateStepMonthGraph+1
        nbStepGraphMonth = 12
        # array to store the number of entries by year
        numberByYear = [0] * nbStepGraphYear
        numberPaidByYear = [0] * nbStepGraphYear
        numberOnTimePaidByYear = [0] * nbStepGraphYear
        delayDernierPaiementByYear = [0] * nbStepGraphYear
        delayEcheanceByYear = [0] * nbStepGraphYear
        # array to store the number of entries by month
        numberByMonth = [0] * nbStepGraphMonth
        numberPaidByMonth = [0] * nbStepGraphMonth
        numberOnTimePaidByMonth = [0] * nbStepGraphMonth
        delayDernierPaiementByMonth = [0] * nbStepGraphMonth
        delayEcheanceByMonth = [0] * nbStepGraphMonth
        # going through the column of paid data
        # => we use those columns because the strings are already converted into date
        for c in columnDatePaid:
            # computing concerned month and year
            year = (int)(Utils.nbMonthsBetweenDates(c[0], minDate[0])/Constants.anaDateStepMonthGraph)
            month = c[0].month-1
            # updating number
            numberByYear[year] += 1
            numberByMonth[month] += 1
            numberPaidByYear[year] += 1
            numberPaidByMonth[month] += 1
            if c[2]<c[1] :
                numberOnTimePaidByYear[year] += 1
                numberOnTimePaidByMonth[month] += 1
            # updating delays
            delayEcheance = (c[1]-c[0]).days
            delayEcheanceByYear[year] += delayEcheance
            delayEcheanceByMonth[month] += delayEcheance
            delayDernierPaiement = (c[2]-c[0]).days
            delayDernierPaiementByYear[year] += delayDernierPaiement
            delayDernierPaiementByMonth[month] += delayDernierPaiement
        # going through the column of unpaid data
        for c in columnDateUnpaid:
            # computing concerned month and year
            year = (int)(Utils.nbMonthsBetweenDates(c[0], minDate[0])/Constants.anaDateStepMonthGraph)
            month = c[0].month-1
            # updating number
            numberByYear[year] += 1
            numberByMonth[month] += 1
            # => we don't increase numberPaid nor numberOnTimePaid because the entry is not paid, neither paid on-time
            # updating delays
            delayEcheance = (c[1]-c[0]).days
            delayEcheanceByYear[year] += delayEcheance
            delayEcheanceByMonth[month] += delayEcheance
            # => we don't have the dernier paiement date, so we don't compute the dernierpaiement delay
        # computing ratio of numbers and means of delays
        for month in range(nbStepGraphMonth):
            if numberPaidByMonth[month]>0:
                delayDernierPaiementByMonth[month] /= numberPaidByMonth[month]
            if numberByMonth>0:
                delayEcheanceByMonth[month] /= numberByMonth[month]
                numberOnTimePaidByMonth[month] = 100.0*numberOnTimePaidByMonth[month]/numberByMonth[month]
                numberPaidByMonth[month] = 100.0*numberPaidByMonth[month]/numberByMonth[month]
        for year in range(nbStepGraphYear):
            if numberPaidByYear[year]>0:
                delayDernierPaiementByYear[year] /= numberPaidByYear[year]
            if numberByYear[year]>0:
                delayEcheanceByYear[year] /= numberByYear[year]
                numberOnTimePaidByYear[year] = 100.0*numberOnTimePaidByYear[year]/numberByYear[year]
                numberPaidByYear[year] = 100.0*numberPaidByYear[year]/numberByYear[year]
        # computing the label for the year
        labelByYear = [str((minDatePaid[0] + i*datetime.timedelta(days = Constants.anaDateStepMonthGraph*30.45)).year) for i in range(nbStepGraphYear)]
        labelByMonth = Constants.labelByMonth
        # DRAWING
        # III - distribution of entries over the years and over the months
        DrawingTools.createHistogram(x=labelByYear, y1=numberByYear,ylabel="Nombre de factures",
                                     name="Distribution des factures par année",filename="03_DateFacturesAnnees")
        DrawingTools.createHistogram(x=labelByMonth, y1=numberByMonth,ylabel="Nombre de factures",
                                     name="Distribution des factures par mois",filename="03b_DateFacturesMois")
        # IV - distribution of ratio of paid and ontimepaid over the years and the months
        DrawingTools.createHistogram(x=labelByYear,y1=numberPaidByYear,y2=numberOnTimePaidByYear,ylabel="Pourcentage de factures",
                                     name1="Pourcentage de factures payées",name2="Pourcentage de factures payées à temps",
                                     name="Ratio des factures payées par année",filename="04_RatioFacturesPayeesAnnees")
        DrawingTools.createHistogram(x=labelByMonth,y1=numberPaidByMonth,y2=numberOnTimePaidByMonth,ylabel="Pourcentage de factures",
                                     name1="Pourcentage de factures payées",name2="Pourcentage de factures payées à temps",
                                     name="Ratio des factures payées par mois",filename="04b_RatioFacturesPayeesMois")
        # V - distribution of delays
        DrawingTools.createHistogram(x=labelByYear,y1=delayEcheanceByYear,y2=delayDernierPaiementByYear,ylabel="Jours de délais",
                                     name1="Durée de l'échéance",name2="Durée du temps de paiement",
                                     name="Durées des délais d'échéance et de paiement",filename="05_DelayFacturesAnnees")
        DrawingTools.createHistogram(x=labelByMonth,y1=delayEcheanceByMonth,y2=delayDernierPaiementByMonth,ylabel="Jours de délais",
                                     name1="Durée de l'échéance",name2="Durée du temps de paiement",
                                     name="Durées des délais d'échéance et de paiement",filename="05b_DelayFacturesMois")
       
    if toDrawGraphOld:
        # initializing graph
        plt.figure(Constants.figureId, figsize = Constants.figsize)
        Constants.figureId += 1
        plt.suptitle("Dates Analysis", fontsize=16)
        Constants.graphId = 1
        
        #####################################
        # Drawing the delays over the years #
        #####################################
        # the following array are separated in time step depending on the constant file
        nbStepGraph = Utils.nbMonthsBetweenDates(maxDatePaid[0], minDatePaid[0])/Constants.anaDateStepMonthGraph+1
        #     arrays storing the delays
        if Constants.banaDateLargeAnalysis:
            delayByYear = [[] for i in range(nbStepGraph)]
            echeanceByYear = [[] for i in range(nbStepGraph)]
            # arrays storing advanced statistics
            meanDelayByYear = [0]*nbStepGraph
            varDelayByYear = [0]*nbStepGraph
#             medianDelayByYear = [0]*nbStepGraph
            meanEcheanceByYear = [0]*nbStepGraph
            varEcheanceByYear = [0]*nbStepGraph
#             medianEcheanceByYear = [0]*nbStepGraph
        else:
            delayByYear = [0]*nbStepGraph
            echeanceByYear = [0]*nbStepGraph
        #     array storing the total number of paid bills
        nbEntriesByYear = [0]*nbStepGraph
        #     array for the x-labels of the graph
        labelByYear = [0]*nbStepGraph
        for c in columnDatePaid:
            i = (int)(Utils.nbMonthsBetweenDates(c[0], minDatePaid[0])/Constants.anaDateStepMonthGraph)
            if Constants.banaDateLargeAnalysis:
                delayByYear[i].append((c[2]-c[0]).days)
                echeanceByYear[i].append((c[1]-c[0]).days)
            else:
                delayByYear[i] += (c[2]-c[0]).days
                echeanceByYear[i] += (c[1]-c[0]).days
            nbEntriesByYear[i] +=1
        somme = np.sum(nbEntriesByYear)
        # computing statistics
        for i in range(nbStepGraph):
            labelByYear[i] = str((minDatePaid[0] + i*datetime.timedelta(days = Constants.anaDateStepMonthGraph*30.45)).year)
            if nbEntriesByYear[i]>0:
                if Constants.banaDateLargeAnalysis:                
                    meanDelayByYear[i] = np.mean(delayByYear[i])
                    varDelayByYear[i] = np.std(delayByYear[i])
#                     medianDelayByYear[i] = np.median(delayByYear[i])
                    meanEcheanceByYear[i] = np.mean(echeanceByYear[i])
                    varEcheanceByYear[i] = np.std(echeanceByYear[i])
#                     medianEcheanceByYear[i] = np.median(echeanceByYear[i])
                else:
                    delayByYear[i] = 1.0*delayByYear[i]/nbEntriesByYear[i]
                    echeanceByYear[i] = 1.0*echeanceByYear[i]/nbEntriesByYear[i]
                    nbEntriesByYear[i] = 100.0*nbEntriesByYear[i]/somme
        # printing graph => two bars for delay and echeance by year
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        if Constants.banaDateLargeAnalysis:
            ax1.bar(range(nbStepGraph),meanDelayByYear, Constants.barSpace, color=Constants.colorBlue, yerr = varDelayByYear)
            ax1.bar(np.arange(nbStepGraph)+Constants.barSpace,meanEcheanceByYear, Constants.barSpace, color=Constants.colorGreen, yerr = varEcheanceByYear)
        else:
            ax1.bar(range(nbStepGraph),delayByYear, Constants.barSpace,color=Constants.colorBlue)
            ax1.bar(np.arange(nbStepGraph)+Constants.barSpace,echeanceByYear, Constants.barSpace,color=Constants.colorGreen)       
        ax1.set_ylabel('Mean delay (days)', color = Constants.colorBlue, fontsize=10)
        for tl in ax1.get_yticklabels():
            tl.set_color(Constants.colorBlue)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorBlue, label='Delay before last payment'),mpatches.Patch(color=Constants.colorGreen, label='Delay before Echeance')], fontsize = 10)
        ax1.axhline(y= 60, xmin=0, xmax=nbStepGraph,color='k',alpha = 0.5,linestyle = '--', label = "60 days")
        plt.xticks(np.arange(nbStepGraph)+Constants.barSpace,labelByYear,rotation=70)
        plt.title("Delays before last payment and echeance over the years for paid bills", fontsize = 12)
        
        ######################################
        # Drawing the delays over the months #
        ######################################     
        # the following array are separated in time step depending on the constant file
        #     arrays storing the delay
        if Constants.banaDateLargeAnalysis:
            delayByMonth = [[] for i in range(12)]
            echeanceByMonth = [[] for i in range(12)]
            meanDelayByMonth = [0] * 12
            varDelayByMonth = [0] * 12
            meanEcheanceByMonth = [0] * 12
            varEcheanceByMonth = [0] * 12
        else:
            delayByMonth = [0] * 12
            echeanceByMonth = [0] * 12
        #     array storing the total number of paid bills
        nbEntriesByMonth = [0] * 12
        #     array for the x-labels of the graph
        labelByMonth = Constants.labelByMonth
        for c in columnDatePaid:
            i = c[0].month-1
            if Constants.banaDateLargeAnalysis:
                delayByMonth[i].append((c[2]-c[0]).days)
                echeanceByMonth[i].append((c[1]-c[0]).days) 
            else:
                delayByMonth[i] += (c[2]-c[0]).days
                echeanceByMonth[i] += (c[1]-c[0]).days                 
            nbEntriesByMonth[i] += 1
        somme = np.sum(nbEntriesByMonth)
        for i in range(12):
            if nbEntriesByMonth[i]>0:
                if Constants.banaDateLargeAnalysis:
                    meanDelayByMonth[i] = np.mean(delayByMonth[i])
                    varDelayByMonth[i] = np.std(delayByMonth[i])
                    meanEcheanceByMonth[i] = np.mean(echeanceByMonth[i])
                    varEcheanceByMonth[i] = np.std(echeanceByMonth[i])
                else:
                    delayByMonth[i] = 1.0*delayByMonth[i]/nbEntriesByMonth[i]
                    echeanceByMonth[i] = 1.0*echeanceByMonth[i]/nbEntriesByMonth[i]
                    nbEntriesByMonth[i] = 100.0*(nbEntriesByMonth[i])/somme
        
        # printing graph
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        if Constants.banaDateLargeAnalysis:
            ax1.bar(range(12),meanDelayByMonth, Constants.barSpace,color = Constants.colorBlue, yerr = varDelayByMonth)
            ax1.bar(np.arange(12)+Constants.barSpace,meanEcheanceByMonth, Constants.barSpace,color = Constants.colorGreen, yerr = varEcheanceByMonth)
        else:
            ax1.bar(range(12),delayByMonth, Constants.barSpace,color = Constants.colorBlue)
            ax1.bar(np.arange(12)+Constants.barSpace,echeanceByMonth, Constants.barSpace,color = Constants.colorGreen)
        ax1.set_ylabel('Mean delay (days)', color=Constants.colorBlue, fontsize=10)
        for tl in ax1.get_yticklabels():
            tl.set_color(Constants.colorBlue)
        plt.xticks(np.arange(12)+Constants.barSpace,labelByMonth,rotation=70)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorBlue, label='Delay before last payment'),mpatches.Patch(color=Constants.colorGreen, label='Delay before Echeance')], fontsize = 10)
        plt.title("Delays before last payment and echeance over the months for paid bills", fontsize=12)
        
        #####################################
        # Drawing the ratios over the years #
        #####################################
        
        # the following array are separated in time step depending on the constant file
        nbStepGraph = (int)(Utils.nbMonthsBetweenDates(maxDate[0], minDate[0])/Constants.anaDateStepMonthGraph)+1
        #     array storing the total number of unpaid bills
        nbEntriesByYear = [0]*nbStepGraph
        nbOntimePaidByYear = [0]*nbStepGraph
        nbPaidByYear = [0]*nbStepGraph
        for c in column:
            # computing the period of interest
            i = (int)(Utils.nbMonthsBetweenDates(c[0], minDate[0])/Constants.anaDateStepMonthGraph)
            nbEntriesByYear[i] +=1
        for c in columnDatePaid:
            # computing the period of interest
            i = (int)(Utils.nbMonthsBetweenDates(c[0], minDate[0])/Constants.anaDateStepMonthGraph)
            nbPaidByYear[i] += 1
            if c[2]<=c[1]:
                nbOntimePaidByYear[i] += 1
        somme = np.sum(nbEntriesByYear)
        for i in range(nbStepGraph):
            if nbEntriesByYear[i]>0:
                nbPaidByYear[i] = 100.0*nbPaidByYear[i]/nbEntriesByYear[i]
                nbOntimePaidByYear[i] = 100.0*nbOntimePaidByYear[i]/nbEntriesByYear[i]
                nbEntriesByYear[i] = 100.0*nbEntriesByYear[i]/somme
        # printing graph
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),nbEntriesByYear,Constants.barSpace, color = Constants.colorOrange)
        plt.bar(np.arange(nbStepGraph)+Constants.barSpace,nbPaidByYear,Constants.barSpace, color = Constants.colorGreen)
        plt.bar(np.arange(nbStepGraph)+Constants.barSpace,nbOntimePaidByYear,Constants.barSpace, color = Constants.colorBlue)
        plt.ylabel('proportion (%)', color=Constants.colorBlue, fontsize=10)
        for tl in ax1.get_yticklabels():
            tl.set_color(Constants.colorBlue)
        plt.xticks(np.arange(nbStepGraph)+Constants.barSpace,labelByYear,rotation=70)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorOrange, label='Global proportion'),
                              mpatches.Patch(color=Constants.colorGreen, label='Proportion of on-time paid bills'),
                              mpatches.Patch(color=Constants.colorBlue, label='Proportion of paid bills')], 
                   fontsize = 10)
        plt.title("Analysis of the proportions of on-time paid and paid bills over the years", fontsize=12)
        
        ######################################
        # Drawing the ratios over the months #
        ######################################
        
        # the following array are separated in time step depending on the constant file
        nbStepGraph = 12
        #     array storing the total number of unpaid bills
        nbEntriesByMonth = [0]*nbStepGraph
        nbOntimePaidByMonth = [0]*nbStepGraph
        nbPaidByMonth = [0]*nbStepGraph
        for c in column:
            # computing the period of interest
            i = int(c[0][5:7])-1
            nbEntriesByMonth[i] +=1
        for c in columnDatePaid:
            # computing the period of interest
            i = c[0].month-1
            nbPaidByMonth[i] += 1
            if c[2]<=c[1]:
                nbOntimePaidByMonth[i] += 1
        somme = np.sum(nbEntriesByMonth)
        for i in range(nbStepGraph):
            if nbEntriesByMonth[i]>0:
                nbPaidByMonth[i] = 100.0*nbPaidByMonth[i]/nbEntriesByMonth[i]
                nbOntimePaidByMonth[i] = 100.0*nbOntimePaidByMonth[i]/nbEntriesByMonth[i]
                nbEntriesByMonth[i] = 100.0*nbEntriesByMonth[i]/somme
        # printing graph
        ax1 = plt.subplot(Constants.subplotShapeDates+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),nbEntriesByMonth,Constants.barSpace, color = Constants.colorOrange)
        plt.bar(np.arange(nbStepGraph)+Constants.barSpace,nbPaidByMonth,Constants.barSpace, color = Constants.colorGreen)
        plt.bar(np.arange(nbStepGraph)+Constants.barSpace,nbOntimePaidByMonth,Constants.barSpace, color = Constants.colorBlue)
        plt.ylabel('proportion (%)', color=Constants.colorBlue, fontsize=10)
        for tl in ax1.get_yticklabels():
            tl.set_color(Constants.colorBlue)
        plt.xticks(np.arange(nbStepGraph)+Constants.barSpace,labelByMonth,rotation=70)
        ax1.legend(handles = [mpatches.Patch(color=Constants.colorOrange, label='Global proportion'),
                              mpatches.Patch(color=Constants.colorGreen, label='Proportion of on-time paid bills'),
                              mpatches.Patch(color=Constants.colorBlue, label='Proportion of paid bills')], 
                   fontsize = 10)
        plt.title("Analysis of the proportions of on-time paid and paid bills over the months", fontsize=12)
                        
def analyzingMontant(csvinput, toSaveGraph = False, toDrawGraphOld = False):
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
    
    # importing column
    column = csvinput['montantPieceEur'].values
     
    if len(column) ==0 :
        print "no result to analyse"
        print ""
        return
    
    # displaying basic informations
    print "minimal bill value :", np.min(column)
    print "maximal bill value :", np.max(column)
    print "mean bill value :",np.mean(column)
    print "median bill value :",np.median(column)
    
    # about the log-repartition of ids
    repartitionMontantArray = [0]*(int)(math.log10(max(column))*Constants.anaIdLogCoefficientMontants+1)
    for e in column:
        if e>0:
            repartitionMontantArray[(int)(math.log10((int)(e))*Constants.anaIdLogCoefficientMontants)] += 1
    print "repartition of the montants according to their log-10:"
    print repartitionMontantArray
    print ""
    print "negative values :", len(column)-np.sum(repartitionMontantArray)
    print ""
    
    # creating and saving graph
    if toSaveGraph:
        # PREPROCESSING
        # computing length of the array
        nbStepGraph = len(repartitionMontantArray)
        # computing labels
        xlabel = ["e^"+str(1.0*i/Constants.anaIdLogCoefficientMontants) for i in range(nbStepGraph)]
        # DRAWING
        DrawingTools.createHistogram(x=xlabel, y1=repartitionMontantArray, 
                                     xlabel="Valeur des factures (euros)", ylabel="Nombre de factures", 
                                     name="Distribution des montants des factures", filename="06_Montants")
    # dealing with the old graph displaying
    if toDrawGraphOld:
        # initializing graph displaying
        plt.figure(Constants.figureId, figsize = Constants.figsize)
        Constants.figureId += 1
        plt.suptitle("Bills values 'montantPieceEur' Analysis", fontsize=16)
        Constants.graphId = 1
         
        ###########################################
        # Drawing the repartition of bills values #
        ###########################################
        # pre-processing
        nbStepGraph = len(repartitionMontantArray)
        labelArray = [0] * nbStepGraph
        for i in range(nbStepGraph):
            labelArray[i] = '%.1e' % 10**(1.0*i/Constants.anaIdLogCoefficientMontants) if i%Constants.anaIdLogCoefficientMontants==0 else ""
        # displaying
        plt.subplot(Constants.subplotShapeMontants+Constants.graphId)
        Constants.graphId += 1
        plt.bar(range(nbStepGraph),repartitionMontantArray,color=Constants.colorBlue)
        plt.title("repartition of the bills values", fontsize=12)
        plt.xticks(range(nbStepGraph),labelArray, rotation = 70)
        plt.xlabel("value of bills", fontsize=10)
        plt.ylabel("number of entries", fontsize=10)

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
    
def getCsvEtab():
    usecols = ['entrep_id','capital','DCREN','EFF_ENT']
    csvEtab = FTPTools.retrieveFtplib("ProcessedData/cameliaEtabKevin.csv.gz", compression="gz",usecols=usecols,toPrint=True,sep=";")
    return csvEtab

def getCsvScores():
    usecols = ['entrep_id','dateBilan','sourceModif','scoreSolv','scoreZ','scoreCH','scoreAltman']
    csvScore = FTPTools.retrieveFtplib("cameliaScores.csv.bz2", compression = "bz2", usecols=usecols, toPrint=False)
#     csvScore.set_index('entrep_id',inplace=True)
    return csvScore

def analyzingEntrepScore(toSaveGraph = False):
    print "=== Starting Analysis of Entreprises Scores ==="
    print ""
    
    startTime = time.time()
    csvinput = importAndCleanCsv(True, ftp = True)
    column = csvinput[['entrep_id','datePiece']].values
    # initializing variables
    #     setting size variables
    nbEntreprises = len(np.unique(column))
    print "enterprises number :" , nbEntreprises
    # initializing variables
    #    dictionary linking entrep_id to concerned rows
    entrepriseToRowsDict = {}
    dicNbEntreprise = {}
    ind = 0
    for entreprise in column:
        if not entrepriseToRowsDict.has_key(entreprise[0]):
            entrepriseToRowsDict[entreprise[0]] = []
        entrepriseToRowsDict[entreprise[0]].append(entreprise[1][:4])
        ind += 1
    for entreprise in entrepriseToRowsDict:
        dicNbEntreprise[entreprise] = [entreprise,len(entrepriseToRowsDict[entreprise]),min(entrepriseToRowsDict[entreprise]),max(entrepriseToRowsDict[entreprise])]
    column = []
    entrepriseData = pd.DataFrame.from_dict(dicNbEntreprise, orient='index')
    entrepriseData.columns = ['entrep_id','nbBills','dateMin','dateMaxi']
#     entrepriseData = []

    ## IMPORTING SCORE DATA
    print "importing the Score csv",
    csvScore = getCsvScores()
    print "... done:",
    interval = time.time() - startTime
    print interval, 'sec'
    print ""
    
    # CLEANING THE DATAFRAME
    print "cleaning the dataframe",
    entrepriseData = entrepriseData.merge(csvScore, on="entrep_id")
    entrepriseData = entrepriseData[entrepriseData.dateBilan.str[2:4]>=10]
    entrepriseData = entrepriseData[entrepriseData.sourceModif=="bilans1"]
    entrepriseData = entrepriseData[entrepriseData.dateBilan!="0000-00-00"]
    del entrepriseData['sourceModif']
    print "... done"
    print ""


    ## ANALYZING DATES CONSISTENCE
    print "analyzing dates consistence"
    for entreprise in dicNbEntreprise.keys():
        dicNbEntreprise[entreprise].append(0)
    for line in entrepriseData.values:
#         print line[4][:4], line[3], line[2], line[4][:4]<=line[3] and line[4][:4]>=line[2]
        if line[4][:4]<=line[3] and line[4][:4]>=int(line[2])-1:
            dicNbEntreprise[int(line[0])][-1]+=1
    for entreprise in dicNbEntreprise.keys():
        dicNbEntreprise[entreprise][-1] = 100.0*dicNbEntreprise[entreprise][-1]/(int(dicNbEntreprise[entreprise][3])+1-int(dicNbEntreprise[entreprise][2]))
    print "   percentage of scores:", 1.0*sum([a[-1] for a in dicNbEntreprise.values()])/len(dicNbEntreprise)
    print ""
    print ""
    
    ## CREATING HISTOGRAMS AND PRINTING STAT
    print "computing histogram"
    dicScore = {0:'scoreSolv', 1:'scoreZ',2:'scoreCH',3:'scoreAltman'}
    maxis = np.max(entrepriseData,axis=0)
    minis = np.min(entrepriseData,axis=0)
    means = np.mean(entrepriseData,axis=0)
    dicX = {}
    dicY = {}
    dicError = {}
    nbStepHistogram = 50
    for dic in dicScore.values():
        print dic
        print "   max:",maxis[dic]
        print "   min:",minis[dic]
        print "   mean:",means[dic]
        print ""
        if dic=="scoreZ":
            minis[dic] = -20
            maxis[dic] = 20
        if dic=="scoreAltman":
            minis[dic] = 0
            maxis[dic] = 20
        dicX[dic] = range(int(minis[dic]),int(maxis[dic])+1,max(1,int((maxis[dic]+1-minis[dic])/nbStepHistogram)))
        dicY[dic] = [0] * nbStepHistogram
        dicError[dic] = 0
    p = 0
    percent = 10
    total = len(entrepriseData)
    for line in entrepriseData.values:
        p+=1
        if 100.0*p/total>percent:
            print percent,'%',
            percent+=10
        for dic in dicScore.keys():
            i=0
            try:
#                 print len(dicX[dicScore[dic]])-1, dicX[dicScore[dic]][i+1], line[5+dic]
                while i<len(dicX[dicScore[dic]])-1 and dicX[dicScore[dic]][i+1]<line[5+dic]:
                    i+=1
                dicY[dicScore[dic]][i]+=1   
            except:
                dicError[dicScore[dic]]+=1 
    print ""
    print "errors:", dicError
    print ""   
    
    prepareInput("scoreFile")
    for dic in dicScore.values():
        DrawingTools.createHistogram(x=dicX[dic], y1=dicY[dic], name1=dic, xlabel="score", ylabel="nombre d'entreprise", percent=True, name="repartition du "+dic, filename="repartition_"+dic)   
    
    interval = time.time() - startTime
    print "done: ", interval, 'sec'

def analyzingEntrepEtab(toSaveGraph = False):
    print "=== Starting Analysis of Entreprises Etablissment ==="
    print ""
    startTime = time.time()
    csvinput = importAndCleanCsv(True, ftp = True)
    column = csvinput['entrep_id'].values
    # initializing variables
    #     setting size variables
    nbEntreprises = len(np.unique(column))
    print "enterprises number :" , nbEntreprises
    print ""
    # initializing variables
    #    dictionary linking entrep_id to concerned rows
    dicEntreprise = {}
    for entreprise in column:
        if not dicEntreprise.has_key(entreprise):
            # array containing ['entrep_id',nbEtab,array(capital),array(dateCreation),array(effectif)]
            dicEntreprise[entreprise] = [entreprise,0,[],[],[]]
    
    print "retrieving csvEtab file",
    csvEtab = getCsvEtab()
#     entrepriseData = []
    print "... done:",
    interval = time.time() - startTime
    print interval, 'sec'
    print ""
    
    ## ANALYSIS AND JOINING
    print "Analysing the file",
    nbNanCapital = 0
    nbNanDate = 0
    nbNanEffectif = 0
    total = len(csvEtab)
    percent = 1
    i = 0
    for line in csvEtab.values:
        i+=1
        if 100.0*i/total>percent:
            print percent,"%",
            percent+=1
            if percent%10==0:
                print ""
        # line = ['entrep_id','capital','DCREN','EFF_ENT','adr_dep'] 
        if line[0] not in dicEntreprise.keys():
            continue
        # increasing number of Etablissment
        dicEntreprise[line[0]][1]+=1
        # adding info about capital
        if str(line[1])!="nan":
            dicEntreprise[line[0]][2].append(int(line[1]))
        else:
            nbNanCapital+=1
        # adding info about dateCreation
        if str(line[2])!="nan":
            dicEntreprise[line[0]][3].append(line[2])
        else:
            nbNanDate+=1
        # adding info about effectif
        if str(line[3])!="nan":
            dicEntreprise[line[0]][4].append(int(line[3]))
        else:
            nbNanEffectif+=1
    print "...done"
    print ""
    print "number of Nan capital:",nbNanCapital
    print "number of Nan date:",nbNanDate
    print "number of Nan effectif:",nbNanEffectif
    
    ## ANALYSING CONSISTENCY
    nbNoInfo = 0
    nbInconsistentCapital = 0
    nbInconsistentDate = 0
    nbInconsistentEffectif = 0
    print "Consistency analysis",
    for entreprise in dicEntreprise.keys():
        if dicEntreprise[entreprise][1] == 0:
            nbNoInfo+=1
            continue
        if len(np.unique(dicEntreprise[entreprise][2]))>1:
            nbInconsistentCapital+=1
        dicEntreprise[entreprise][2] = np.max(dicEntreprise[entreprise][2])
        if len(np.unique(dicEntreprise[entreprise][3]))>1:
            nbInconsistentDate+=1
        dicEntreprise[entreprise][4] = np.min(dicEntreprise[entreprise][4])
        if len(np.unique(dicEntreprise[entreprise][4]))>1:
            nbInconsistentEffectif+=1
        dicEntreprise[entreprise][4] = np.max(dicEntreprise[entreprise][4])
    print "...done"
    print "   nb of missing entreprises:",100.0*nbNoInfo/len(dicEntreprise),"%"
    print "   nb of inconsistent capital:",100.0*nbInconsistentCapital/len(dicEntreprise),"%"
    print "   nb of inconsistent date:",100.0*nbInconsistentDate/len(dicEntreprise),"%"
    print "   nb of inconsistent effectif:",100.0*nbInconsistentEffectif/len(dicEntreprise),"%"
    print ""
    
    print "computing analysis",
    # initializing arrays
    dateX = range(1990,2016)
    dateY = [0]*len(dateX)
    capitalX = range(0,12)
    capitalY = [0]*len(capitalX)
    effectifX = range(0,6)
    effectifY = [0]*len(effectifX)
    nbDateError = 0
    for entreprise in dicEntreprise.keys():
        # handling capitals
        i=0
        capital = dicEntreprise[entreprise][2]
        while i<len(capitalX)-1 and capital>10**(capitalX[i]):
            i+=1
        capitalY[i]+=1
        # handling dates
        i=0
        try:
            date = int(dicEntreprise[entreprise][3][:4])
            print date
            while i<len(dateX)-1 and date>dateX[i]:
                i+=1
            dateY[i]+=1
        except:
            nbDateError+=1
            pass
        # handling effectifs
        i=0
        effectif = dicEntreprise[entreprise][4]
        while i<len(effectifX)-1 and effectif>10**(effectifX[i]):
            i+=1
        effectifY[i]+=1
    # handling output 
    prepareInput("etabFile")
    DrawingTools.createHistogram(x=capitalX,y1=capitalY,percent=True,
                                 xlabel="capitals",ylabel="number of entreprise (%)",
                                 name="Repartition of the capitals",
                                 filename="01_repartitionCapital")
    DrawingTools.createHistogram(x=dateX,y1=dateY,percent=True,
                                 xlabel="dates",ylabel="number of entreprise (%)",
                                 name="Repartition of the dates",
                                 filename="01_repartitionDate")
    DrawingTools.createHistogram(x=effectifX,y1=effectifY,percent=True,
                                 xlabel="effectifs",ylabel="number of entreprise (%)",
                                 name="Repartition of the effectifs",
                                 filename="01_repartitionEffectif")
    print "...done"
    print "nb Date Error:",nbDateError
    print ""
    
    interval = time.time() - startTime
    print "done: ", interval, 'sec'
    
    
    
             
''' V - Scripts and Global Functions '''
def importAndCleanCsv(toPrint = False, ftp = False):
    '''
    Function that process the data:
    importing, cleaning and analysing
    -- IN
    toPrint : boolean to show the log of the cleaning process (boolean) default: True
    ftp : boolean to choose between local and remote data (boolean) default: False
    -- OUT 
    csvinput : the cleaned dataframe (pandas.Dataframe)
    '''
    print "Extracting the BalAG dataframe"
    startTime = time.time()
    # importing the csv file and creating the datframe
    if(ftp):
        csvinput = importFTPCsv(addPaidBill=True)
    else:
        csvinput = importCsv(addPaidBill=True)
        
    # preprocess the dataframe
    csvinput = cleaningDates(csvinput, toPrint)
    csvinput = cleaningOther(csvinput, toPrint)
    csvinput = cleaningMontant(csvinput, toPrint)
    csvinput = cleaningEntrepId(csvinput, toPrint)
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
        csvinput = importFTPCsv(addPaidBill=True)
    else:
        csvinput = importCsv(addPaidBill=True)
        
    # preprocess the dataframe
    csvinput = cleaningDates(csvinput, toPrint)
    csvinput = cleaningOther(csvinput, toPrint)
    csvinput = cleaningMontant(csvinput, toPrint)
    csvinput = cleaningEntrepId(csvinput, toPrint)
    if toDrawGraph:
        prepareInput()
    # analysing the dateframe
    analyzingDates(csvinput, toSaveGraph=toDrawGraph)
    analyzingEntrepId(csvinput, toSaveGraph=toDrawGraph)
    analyzingMontant(csvinput, toSaveGraph=toDrawGraph)
#     analyzingOthers(csvinput)
    analyzingComplete(csvinput, toSaveGraph=toDrawGraph)
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
    os.chdir(lastdir)
    dirs = os.listdir("../"+lastdir)
    for direct in dirs:
        tab = direct.split(".")
        if(tab[1]=="txt"):
            print direct,
            DrawingTools.drawHistogramFromFile(tab[0]) 
            print "...done"
            #     os.chdir(os.path.join("..","src","preprocess"))

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
      
def sideAnalysis(ftp = True):
    """
    function that compare the local or remote file to other remote files
    of the database to see the differences in the entrep_id columns.
    -- IN
    ftp : boolean that settles if we compare the local or remote version of CameliaBalAG - 
        True for the remote version (boolean) default : True
    -- OUT
    returns nothing
    """
    # importing the csv file and creating the datframe
    if(ftp):
        csvinput = importFTPCsv()
    else:
        csvinput = importCsv(usecols=['entrep_id'])
    analyzingIdCorresponding(csvinput, "CameliaEtab.csv.gz")
#     analyzingIdCorresponding(csvinput, "CameliaBilans.csv.gz")

# path = Constants.path
# os.chdir(path)



# cleaningCsv(ftp = True)  

# printLastGraphs()

# sideAnalysis(True)

