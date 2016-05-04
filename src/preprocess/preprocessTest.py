# -*- coding: utf-8 -*-
'''
Created on 19 avr. 2016

@author: Kévin Bienvenu
'''

import unittest
from FTPTools import *
from DrawingTools import *
from PaiementDataExtraction import *


class TestFTPTools(unittest.TestCase):
    '''
    tests for the FTPTools module
    '''

    def testLoginFilePresence(self):
        ''' tests if the file login_ftp.txt still exists '''
        self.assertTrue(os.path.isfile(os.path.join("..","..","login_ftp.txt")),"non-exisiting file : login_ftp.txt")
        
    def testGetAccountUserName(self):
        ''' tests if the username in the file is right'''
        self.assertEqual(getAccount()[0], "KevinBienvenu")
        
    def testGetAccountPort(self):
        ''' tests if the port in the file is an int'''
        int(getAccount()[3])
    
    def testFTPLibConnection(self):
        ''' tests if the ftplib connection is available with the FTP server '''
        ftp = FTP_TLS()
        (user, password, host, port) = getAccount()
        ftp.connect(host,port)
        ftp.login(user,password)
        
    def testCkFTPConnection(self):
        ''' tests if the chilkat connection is available with the FTP server '''
        ftp = CkFtp2()
        (user, password, host, port) = getAccount()
        # connecting and logging in
        ftp.put_Passive(True)
        ftp.put_Hostname(host)
        ftp.put_Port(port)
        ftp.put_Username(user)
        ftp.put_Password(password)
        ftp.put_AuthTls(True)
        ftp.put_Ssl(False)
        ftp.put_SslProtocol("TLS 1.2")
        self.assertTrue(ftp.Connect(), "connection impossible")
    
    def testWrongInputFilename(self):
        ''' tests if the function returns None if wrong file name is given'''
        # if filename is None
        self.assertEqual(retrieveFtplib(None),None)
        # if filename has no extension
        self.assertEqual(retrieveFtplib("cameliaBalAG"),None)
        # if filename is no gzip compressed
        self.assertEqual(retrieveFtplib("cameliaLiensLinks.csv.bz2"), None)
        # if filename is None
        self.assertEqual(retrieveFtplib(None),None)
        # if filename has no extension
        self.assertEqual(retrieveFtplib("cameliaBalAG"),None)
        # if filename is no gzip compressed
        self.assertEqual(retrieveFtplib("cameliaLiensLinks.csv.bz2"), None)

class TestDrawingTools(unittest.TestCase):
    '''
    tests for the DrawingTools module
    '''
    
class TestPaiementDataExtraction(unittest.TestCase):
    '''
    tests for the PaiementDataExtraction module

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
    
    === PART IV - Scripts and Global Functions
    importAndCleanCsv(toPrint, ftp) : imports the local or remote file and cleans it
    importAndAnalyseCsv(toPrint, todoAnalysis, toDrawGraph, ftp) : imports, cleans and analyzes the csv file
    prepareDirForGraphExport() : prepare the directories to export the graph from the analysis
    printLastGraphs() : transforms the last export of graphs from .txt to .png
    sideAnalysis(ftp) : compare the csv file to other files on the ftp server
    '''
    def testNoneInputCleaning(self):
        ''' tests if an input set to None raises no exception during the cleaning process '''
        csvinput = None
        cleaningEntrepId(csvinput)
        cleaningDates(csvinput)
        cleaningMontant(csvinput)
        cleaningOther(csvinput)
    
    def testNoneInputAnalysis(self):
        ''' tests if an input set to None raises no exception during the analysis process '''
        csvinput = None
        analyzingDates(csvinput)
        analyzingEntrepId(csvinput)
        analyzingMontant(csvinput)
        analyzingOthers(csvinput)
        analyzingComplete(csvinput)
        
    def testEmptyInputCleaning(self):
        ''' tests if an empty input raises no exception during the cleaning process '''
        csvinput = pd.DataFrame(['entrep_id','datePiece','dateEcheance','dateDernierPaiement',
                           'montantPieceEur','devise','montantLitige','dateInsert'])
        cleaningEntrepId(csvinput)
        cleaningDates(csvinput)
        cleaningMontant(csvinput)
        cleaningOther(csvinput)
        
    def testEmptyInputAnalysis(self):
        ''' tests if an empty input raises no exception during the analysis process '''
        csvinput = pd.DataFrame(['entrep_id','datePiece','dateEcheance','dateDernierPaiement',
                           'montantPieceEur','devise','montantLitige','dateInsert'])
        analyzingDates(csvinput)
        analyzingEntrepId(csvinput)
        analyzingMontant(csvinput)
        analyzingOthers(csvinput)
        analyzingComplete(csvinput)
        
    def testOnlyOneRowAnalyzingEntrepId(self):
        ''' tests if an input of one row raises no exception during the analysis of entrep_id '''
        db = pd.DataFrame(['entrep_id','datePiece','dateEcheance','dateDernierPaiement',
                           'montantPieceEur','devise','montantLitige','dateInsert'])
        db[0] = ['152','2012-02-03','2012-04-04','2012-05-05','2520','EUR','0','2015-05-05']
        analyzingEntrepId(db)
        analyzingDates(db)
        analyzingMontant(db)
        analyzingOthers(db)
        
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()