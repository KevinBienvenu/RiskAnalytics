# -*- coding: utf-8 -*-
'''
Created on 19 avr. 2016

@author: Kévin Bienvenu
'''

import unittest
from FTPTools import *
from DrawingTools import *
from preprocess.CameliaBalAGPreprocess import *
from Utils import *
import Constants


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
        
    def testWrongInputFilename(self):
        ''' tests if the function returns None if wrong file name is given'''
        # if filename is None
        self.assertEqual(retrieveFtplib(None),None)
        # if filename has no extension / doesn't exist
        self.assertEqual(retrieveFtplib("cameliaBalAG"),None)
    
    def testCsvFileNoEncoding(self):
        ''' tests if the file cameliaLiensEntites.csv is correctly downloadable'''
        db = FTPTools.retrieveFtplib("cameliaLiensEntites.csv")
        self.assertTrue(db is not None)
        self.assertTrue('id' in db.columns)
        self.assertTrue('entrep_id' in db.columns)
        self.assertTrue('cotationEnBourse' in db.columns)
    
    def testCsvFileGzipEncoding(self):
        ''' tests if the file cameliaAnnonces.csv.gz is correctly downloadable'''
        db = FTPTools.retrieveFtplib("cameliaAnnonces.csv.gz",compression="gz")
        self.assertTrue(db is not None)
        
    def testCsvFileBz2Encoding(self):
        ''' tests if the file cameliaLiensEntites.csv.bz2 is correctly downloadable'''
        db = FTPTools.retrieveFtplib("cameliaLiensEntites.csv.bz2",compression="bz2")
        self.assertTrue(db is not None)
        self.assertTrue(db is not None)
        self.assertTrue('id' in db.columns)
        self.assertTrue('entrep_id' in db.columns)
        self.assertTrue('cotationEnBourse' in db.columns)

class TestDrawingTools(unittest.TestCase):
    '''
    tests for the DrawingTools module
    '''
    
    def testConnectionToPlotLy(self):
        ''' tests if the plotly account is still available '''
        py.plotly.sign_in('KevinBienvenu','r8vjr5qj9n')
        
    def testNoneHistogram(self):
        ''' tests if the None case returns None and print an error '''
        self.assertTrue(DrawingTools.drawHistogram(x=None,y1=None) is None)
        self.assertTrue(DrawingTools.saveHistogram(x=None,y1=None) is None)
    
    def testSaveHistogram(self):
        ''' tests the creation of the file '''
        x = [1,2,3]
        y1 = [1,2,3]
        if "test.txt" in os.listdir("."):
            os.remove("test.txt")
        DrawingTools.saveHistogram(x=x,y1=y1,filename="test")
        self.assertTrue("test.txt" in os.listdir("."))
        with open("test.txt","r") as fichier:
            for line in fichier:
                if line[:2]=="x:":
                    self.assertTrue(line[2:-1]=="1,2,3")
                if line[:3]=="y1:":
                    self.assertTrue(line[3:-1]=="1,2,3")
        os.remove("test.txt")
    
    def testDrawFromFileHistogram(self):
        ''' tests the reading of the file '''
        DrawingTools.saveHistogram(x=[1,2,3],y1=[1,2,3],filename="test")
        DrawingTools.drawHistogramFromFile("test")
        self.assertTrue("test.png" in os.listdir("."))
        os.remove("test.txt")
        os.remove("test.png")

    def testSaveHistogram3Data(self):
        ''' tests the creation of the file'''
        x = [1,2,3]
        y1 = [1,2,3]
        y2 = [2,2,3]
        y3 = [3,2,3]
        if "test.txt" in os.listdir("."):
            os.remove("test.txt")
        DrawingTools.saveHistogram(x=x,y1=y1,y2=y2,y3=y3,
                                   name1 = "1",
                                   name2 = "2",
                                   name3 = "3",
                                   filename="test")
        self.assertTrue("test.txt" in os.listdir("."))
        nbTest = 0
        with open("test.txt","r") as fichier:
            for line in fichier:
                if line[:2]=="x:":
                    self.assertTrue(line[2:-1]=="1,2,3")
                    nbTest += 1
                if line[:3]=="y1:":
                    self.assertTrue(line[3:-1]=="1,2,3")
                    nbTest += 1
                if line[:3]=="y2:":
                    self.assertTrue(line[3:-1]=="2,2,3")
                    nbTest += 1
                if line[:3]=="y3:":
                    self.assertTrue(line[3:-1]=="3,2,3")
                    nbTest += 1
                if line[:6]=="name1:":
                    self.assertTrue(line[6:-1]=="1")
                    nbTest += 1
                if line[:6]=="name2:":
                    self.assertTrue(line[6:-1]=="2")
                    nbTest += 1
                if line[:6]=="name3:":
                    self.assertTrue(line[6:-1]=="3")
                    nbTest += 1
        self.assertTrue(nbTest>=7)
        os.remove("test.txt")

    def testSaveHistogram2D(self):
        ''' tests the creation of the file '''
        y0 = [1,2,3]
        y1 = [1,2,3]
        if "test.txt" in os.listdir("."):
            os.remove("test.txt")
        DrawingTools.saveHistogram2D(y0=y0,y1=y1,filename="test")
        self.assertTrue("test.hist2d" in os.listdir("."))
        with open("test.hist2d","r") as fichier:
            for line in fichier:
                if line[:3]=="y0:":
                    self.assertTrue(line[3:-1]=="1,2,3")
                if line[:3]=="y1:":
                    self.assertTrue(line[3:-1]=="1,2,3")
        os.remove("test.hist2d")
    
    def testDrawFromFileHistogram2D(self):
        ''' tests the reading of the file '''
        DrawingTools.saveHistogram2D(y0=[1,2,3],y1=[1,2,3],filename="test")
        DrawingTools.drawHistogramFromFile("test",typeHist="2d")
        self.assertTrue("test.png" in os.listdir("."))
        os.remove("test.hist2d")
        os.remove("test.png")

class TestUtils(unittest.TestCase):
    '''
    tests for the Utils module
    '''
    def testValidateDateError(self):
        ''' tests the validateDateError function '''
        # tests invalid examples
        self.assertIsNone(Utils.validateDateError(None))
        self.assertIsNone(Utils.validateDateError("example"))
        # tests a valid example
        self.assertEqual(validateDateError("2010-12-12"),datetime.datetime.strptime("2010-12-12", '%Y-%m-%d').date())
    
    def testValidateDate(self):
        ''' tests the validateDate function '''
        ## checking format behaviour
        d0 = None
        d1 = None
        d2 = None
        Constants.bclnDateDernierPaiementFormat = False
        result = Utils.validateDate(d0,d1,d2)
        # tests pieceFormat result
        self.assertTrue("pieceFormat" in result)
        # tests echeanceFormat result
        self.assertTrue("echeanceFormat" in result)
        # tests dernierPaiementFormat result (1/2)
        self.assertTrue(not("dernierPaiementFormat" in result))
        Constants.bclnDateDernierPaiementFormat = True
        result = Utils.validateDate(d0,d1,d2)
        # tests dernierPaiementFormat result (2/2)
        self.assertTrue("dernierPaiementFormat" in result)
        
        ## checking consistency behaviour
        d0 = "2012-01-01"
        d1 = "2012-02-01"
        d2 = "2012-03-01"
        Constants.bclnDateInconsistent = True
        result = Utils.validateDate(d0,d1,d2)
        # tests consistency (1/4) : everything is ok
        self.assertTrue(not("inconsistentDates" in result))
        d0 = "2013-01-01"
        d1 = "2012-02-01"
        d2 = "2012-03-01"
        result = Utils.validateDate(d0,d1,d2)
        # tests consistency (2/4) : inconsistent date 1
        self.assertTrue("inconsistentDates" in result)
        d0 = "2012-01-01"
        d1 = "2012-02-01"
        d2 = "2011-03-01"
        result = Utils.validateDate(d0,d1,d2)
        # tests consistency (3/4) : inconsistent date 2
        self.assertTrue("inconsistentDates" in result)
        Constants.bclnDateInconsistent = False
        result = Utils.validateDate(d0,d1,d2)
        # tests consistency (4/4) : boolean off
        self.assertTrue(not("inconsistentDates" in result))
        
        ## checking month difference behaviour
        Constants.bclnDateMonthDiff = True
        Constants.clnDateMonthDiff = 5
        d0 = "2012-01-01"
        d1 = "2012-02-01"
        d2 = "2012-04-01"
        result = Utils.validateDate(d0,d1,d2)
        # test month diff (1/4) : everything is ok
        self.assertTrue(not("monthDiff" in result))
        Constants.clnDateMonthDiff = 1
        result = Utils.validateDate(d0,d1,d2)
        # test month diff (2/4) : problem with date 2
        self.assertTrue("monthDiff" in result)
        Constants.clnDateMonthDiff = 5
        d1 = "2013-02-01"
        result = Utils.validateDate(d0,d1,d2)
        # test month diff (3/4) : problem with date 1
        self.assertTrue("monthDiff" in result)
        Constants.bclnDateMonthDiff = False
        result = Utils.validateDate(d0,d1,d2)
        # test month diff (4/4) : boolean off
        self.assertTrue(not("monthDiff" in result))
        
        ## checking minimal date behaviour
        Constants.bclnDateMinimalDate = True
        Constants.clnDateMinimalDate = datetime.datetime.strptime("2010-12-12", '%Y-%m-%d').date()
        d0 = "2013-01-01"
        d1 = "2013-01-01"
        d2 = None
        result = Utils.validateDate(d0,d1,d2)
        # test minimal date (1/3) : everything is ok
        self.assertTrue(not("minimalDate" in result))
        d0 = "2009-01-01"
        result = Utils.validateDate(d0,d1,d2)
        # test minimal date (2/3) : problem of minimal date
        self.assertTrue("minimalDate" in result)
        Constants.bclnDateMinimalDate = False
        result = Utils.validateDate(d0,d1,d2)
        # test minimal date (3/3) : boolean is off
        self.assertTrue(not("minimalDate" in result))
        
        ## checking maximal date behaviour
        Constants.bclnDateMaximalDate = True
        Constants.clnDateMaximalDate = datetime.datetime.strptime("2012-12-12", '%Y-%m-%d').date()
        d0 = "2012-01-01"
        d1 = "2012-01-01"
        d2 = None
        result = Utils.validateDate(d0,d1,d2)
        # test maximal date (1/3) : everything is ok
        self.assertTrue(not("maximalDate" in result))
        d0 = "2013-01-01"
        result = Utils.validateDate(d0,d1,d2)
        # test maximal date (2/3) : problem of maximal date
        self.assertTrue("maximalDate" in result)
        Constants.bclnDateMaximalDate = False
        result = Utils.validateDate(d0,d1,d2)
        # test maximal date (3/3) : boolean is off
        self.assertTrue(not("maximalDate" in result))

    def testNbMonthsBetweenDates(self):
        ''' tests the nbMonthsBetweenDates function'''
        # testing the fail cases
        d0 = "2010-01-10"
        d1 = None
        self.assertEqual(Utils.nbMonthsBetweenDates(d0, d1),-1)
        d1 = "roger"
        self.assertEqual(Utils.nbMonthsBetweenDates(d0, d1),-1)
        d1 = "01-22-2010"
        self.assertEqual(Utils.nbMonthsBetweenDates(d0, d1),-1)
        
        # testing basic cases
        d0 = "2010-01-10"
        d1 = "2010-03-10"
        self.assertTrue(abs(Utils.nbMonthsBetweenDates(d0,d1)-2)<=1)
        self.assertTrue(abs(Utils.nbMonthsBetweenDates(d1,d0)-2)<=1)
        d1 = "2011-01-10"
        self.assertTrue(abs(Utils.nbMonthsBetweenDates(d1,d0)-12)<=1)
   
    def testValidateMontant(self):
        ''' tests the validateMontant function '''
        Constants.bclnMontantIntFormat = True
        Constants.bclnMontantNonNegativeValue = True
        Constants.bclnMontantNonZeroValue = True
        Constants.bclnMontMinimalValue = False
        Constants.bclnMontMaximalValue = False
        montant = None
        self.assertEqual(Utils.validateMontant(montant), "format")
        montant = -1
        self.assertEqual(Utils.validateMontant(montant), "format")
        montant = 0
        self.assertEqual(Utils.validateMontant(montant), "format")
        Constants.bclnMontantNonZeroValue = False
        self.assertEqual(Utils.validateMontant(montant), "")
        Constants.bclnMontantNonNegativeValue = False
        montant = -1
        self.assertEqual(Utils.validateMontant(montant), "")
        Constants.bclnMontantNonNegativeValue = True
        Constants.bclnMontantNonZeroValue = True
        
        ## checking minimal value behaviour
        Constants.bclnMontMinimalValue = True
        Constants.clnMontMinimalValue = 30
        montant = 35 
        self.assertEqual(Utils.validateMontant(montant), "")
        montant = 15 
        self.assertEqual(Utils.validateMontant(montant), "minimal")
        Constants.bclnMontMinimalValue = False
        self.assertEqual(Utils.validateMontant(montant), "")
        
        ## checking maximal value behaviour
        Constants.bclnMontMaximalValue = True
        Constants.clnMontMaximalValue = 40
        montant = 35 
        self.assertEqual(Utils.validateMontant(montant), "")
        montant = 45 
        self.assertEqual(Utils.validateMontant(montant), "maximal")
        Constants.bclnMontMaximalValue = False
        self.assertEqual(Utils.validateMontant(montant), "")  
    
class TestPaiementDataExtraction(unittest.TestCase):
    '''
    tests for the PaiementDataExtraction module
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