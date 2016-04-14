'''
Created on 5 Apr 2016

@author: Kevin Bienvenu

Module containing utilies functions used in other modules
those functions are static
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
    function that returns a boolean set to True if the date is a valid one
    or False else. 
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
    d0 : date of start (str or datetime.date)
    d1 : date of end (str or datetime.date)
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














    