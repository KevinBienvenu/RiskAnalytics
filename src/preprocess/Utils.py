# -*- coding: utf-8 -*-
'''
Created on 5 Apr 2016

@author: Kévin Bienvenu

Module containing useful functions used in other preprocess modules
those functions are static

=== functions :
validateDate(txtDate0, txtDate1, txtDate2) : checks if there are errors in the dates
validateDateError(txtDate) : checks if there are errors in the date format
nbMonthsBetweenDates(d0, d1) : computes the number of months between two dates
validateMontant(montant) : checks if there are errors in the montant value
checkIntFormat(i, isNonNegative, isNonZero) : checks several behaviour of the input
printTime(startTime) : print the time in HH:MM:SS format since startTime
drawArray(openfile, array, arrayName) : write in an openfile the values of an array

'''

import datetime
import Constants
import time

def validateDate(txtDate0, txtDate1, txtDate2):
    '''
    function that validates a row and its three dates Piece, Echeance and DernierPaiement
    according to the booleans in Constants.
    -- IN
    txtDate0 : date 0 (string)
    txtDate1 : date 1 (string)
    txtDate2 : date 2 (string)
    -- OUT
    s : result array of strings (string[])
        - "pieceFormat" error in the datePiece format
        - "echeanceFormat" error in the dateEcheance format
        - "dernierPaiementFormat" error in the dateDernierPaiement format
        - "inconsistentDates" error in the consistency: d0 > d1 or d0 > d2
        - "monthDiff" error in the difference of months between dates d1 > d0 + monthDiff or d2 > d0 + monthDiff
        - "minimalDate" error in the dates : d0 < dateMin
        - "maximalDate" error in the dates : d0 > dateMax
    '''
    result = []
    # checking dates format
    date0 = validateDateError(txtDate0)
    if date0 == None:
        result.append("pieceFormat")
    date1 = validateDateError(txtDate1)
    if date1 == None:
        result.append("echeanceFormat")
    date2 = validateDateError(txtDate2)
    if date2 == None and Constants.bclnDateDernierPaiementFormat :
        result.append("dernierPaiementFormat")
    if date0 == None or date1 == None:
        return result
    # checking consistency
    if(Constants.bclnDateInconsistent):
        if(date0 > date1 or (date2!=None and date0 > date2)):
            result.append("inconsistentDates")
    # checking month difference between dates
    if(Constants.bclnDateMonthDiff):
        monthDiff = Constants.clnDateMonthDiff
        deltamonth1 = nbMonthsBetweenDates(date0, date1)
        if date2 != None:
            deltamonth2 = nbMonthsBetweenDates(date0, date2)
        if(deltamonth1 > monthDiff or (date2!=None and deltamonth2 > monthDiff)):
            result.append("monthDiff")
    # checking minimal date
    if(Constants.bclnDateMinimalDate):
        dateMin = Constants.clnDateMinimalDate
        if(date0 < dateMin):
            result.append("minimalDate")
    # checking maximal date
    if(Constants.bclnDateMaximalDate):
        dateMax = Constants.clnDateMaximalDate
        if(date0 > dateMax):
            result.append("maximalDate")
    return result
        
def validateDateError(txtDate):
    '''
    function that returns the date if the txt is a valid date format and None otherwise
    -- IN
    txtDate : a string containing a date in format %Y-%m-%d (string)
    -- OUT
    d : the date converted in the right format (datetime.date)
        return None if an error occurs
    '''
    try:
        d = datetime.datetime.strptime(txtDate, '%Y-%m-%d').date()
        return d
    except ValueError:
        return None
        
def nbMonthsBetweenDates(d0, d1):
    '''
    function that returns the number of months between two dates
    the order doesn't matter, d0 can be larger than d1.
    -- IN
    d0 : date of start (str or datetime.datetime or datetime.date)
    d1 : date of end (str or datetime.datetime or datetime.date)
    -- OUT
    number of month between the dates (positive int)
    '''
    if isinstance(d0, str):
        d0 = datetime.datetime.strptime(d0, '%Y-%m-%d').date()
    if isinstance(d1, str):
        d1 = datetime.datetime.strptime(d1, '%Y-%m-%d').date()
    if isinstance(d0, datetime.datetime):
        d0 = d0.date()
    if isinstance(d1, datetime.datetime):
        d1 = d1.date()
    # the value of 30.45 is just because the exact calculation is a mess...
    # furthermore, it works pretty well like that :)
    return int(abs((d0-d1).days/30.45))

def validateMontant(montant):
    '''
    function that validates a montantPiece value
    according to the booleans in Constants.
    -- IN
    montant : value of the montant to analyse (int)
    -- OUT
    s : result array of strings (string[])
        - "pieceFormat" error in the datePiece format
        - "echeanceFormat" error in the dateEcheance format
        - "dernierPaiementFormat" error in the dateDernierPaiement format
        - "inconsistentDates" error in the consistency: d0 > d1 or d0 > d2
        - "monthDiff" error in the difference of months between dates d1 > d0 + monthDiff or d2 > d0 + monthDiff
        - "minimalDate" error in the dates : d0 < dateMin
        - "maximalDate" error in the dates : d0 > dateMax
    '''
    if Constants.bclnMontantIntFormat:
        if not checkIntFormat(montant, Constants.bclnMontantNonNegativeValue,Constants.bclnMontantNonZeroValue):
            return "format"
    if Constants.bclnMontMinimalValue:
        if montant<Constants.clnMontMinimalValue:
            return "minimal"
    if Constants.bclnMontMaximalValue:
        if montant>Constants.clnMontMaximalValue:
            return "maximal"
    return ""
            
def checkIntFormat(i, isNonNegative=False, isNonZero=False):
    """
    function that check the format of a number according to the boolean in input.
    the vanilla function only checks if the input i is an integer
    -- IN
    i : number to check (any type)
    isPositive : boolean that settles the checking of i>=0 (boolean)
    isNonZero : boolean that settles the checking of i!=0 (boolean)
    -- OUT
    b : the result of the check (boolean) return true if everything is ok
    """    
    try:
        j = int(i)
        b = True
        if isNonNegative:
            b = b and j>=0
        if isNonZero:
            b = b and j!=0
    except:
        b = False
    return b
        
def printTime(startTime):
    totalTime = (time.time()-startTime)
    hours = (int)(totalTime/3600)
    minutes = (int)((totalTime-3600*hours)/60)  
    seconds = (int)(totalTime%60)
    print "time : ",hours,':',minutes,':',seconds

def drawArray(openfile, array, arrayName):
    openfile.write(str(arrayName)+":"+str(array[0]))
    for i in array[1:len(array)]:
        openfile.write(","+str(i))
    openfile.write("\n")














    