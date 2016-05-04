# -*- coding: utf-8 -*-
'''
Created on 12 Apr 2016

@author: Kévin Bienvenu

Module that handles ftp connection to remote server.
The server security protocol must be TLS1.2
There are two librairies implemented in this module:
FTPLib and Chilkat.CkFTP2
As is the default python librairy FTPLib is set to be used by default
but with several configurations Chilkat must be use instead.

List of functions:
getAccount() : retrieve the information necessary to connect to the ftp server
retrieveFtplib(filename, compression, usecols, dtype) : download, decompress and extract a pandas dataframe using FTPLib
retrieveCKFtp(filename, compression, usecols, dtype) : download, decompress and extract a pandas dataframe using chilkat.CkFTP2
'''

import gzip
import bz2
import time
import os

import StringIO
from cStringIO import StringIO as cS

from ftplib import FTP_TLS
from chilkat import CkFtp2, CkByteData

import pandas as pd


def getAccount():
    """
    function that retrieves the informations about
    the ftp server that are stored on the hard disk.
    The file is 'login-ftp.txt' and is located at the root of the project
    -- IN
    the function takes no arguments
    -- OUT
    user : the user name for the connection (string)
    password : the password for the connection (string)
    host : the ip address where the ftp is located (string)
    port: the port to make the ftp connection. (int)
    returns a tuple of 4* (None) if an error occurs
    """
    filename = os.path.join("..","..","login_ftp.txt")
    try:
        with open(filename, "r") as fileToRead:
            for line in fileToRead:
                l = line.split(" ")
                if l[0] == "user":
                    user = l[1][:len(l[1])-1]
                if l[0] == "password":
                    password = l[1][:len(l[1])-1]
                if l[0] == "host":
                    host = l[1][:len(l[1])-1]
                if l[0] == "port":
                    port = (int)(l[1][:len(l[1])-1])
        return (user, password, host, port)
    except:
        print "error : coundn't read the account file"
        return (None, None, None, None)
    
    
def retrieveFtplib(filename, compression = None, usecols=None, dtype=None, toPrint = False):
    """
    function that connects to the remote FTP serveur and extract a pandas dataframe
    the downloaded file must be compressed through gzip and containing a csv file.
    -- IN
    filename : the filename with its extension to be downloaded from the remote ftp server (string)
    compression : string that specifies the encoding of the file (string in [None,"gz","bz2"] default: None
    usecols : an array containing the name of the column to extract (string[]) default: None
    dtype : a dictionary containing the name of the columns and the type to cast them ({string:string}) default: None
    toPrint : boolean that settles if the function should print its progress and results (boolean) default: False
    -- OUT
    db : a pandas dataframe containing the remote database (pandas.Dataframe)
    return None when an error occurs
    """
    startTime = time.time()
    if toPrint:
        print "==========================================="
        print "=== Connection to the remote FTP server ==="
        print "==========================================="
        print ""
        print "using ftplib"
        print "loading :",filename
        print "" 
    ftp = FTP_TLS()
    # retrieving information about account on ftp server
    (user, password, host, port) = getAccount()
    if user==None:
        print "error : coudn't read the account information"
        return None
    # connecting and logging in
    try:
        ftp.connect(host,port)
        ftp.login(user,password)
    except:
        print "error : unable to connect to the ftp server"
        return None
    # establishing the security protocol
    ftp.prot_p()
    if toPrint:
        print "connected to the FTP server"
    # retrieving the remote file as a binary file
    sio = StringIO.StringIO()
    def handle_binary(more_data):
        sio.write(more_data)
    try:
        ftp.retrbinary("RETR "+filename, callback=handle_binary)
    except:
        print "error : non-existing file :",filename
        return None
    # Go back to the start of the binary file
    sio.seek(0) 
    interval = time.time() - startTime
    if toPrint:
        print 'Data downloaded :', interval, 'sec'
    
    # Unziping the file
    if compression!=None:
        if compression=="gz":
            try:
                results = gzip.GzipFile(fileobj=sio)
            except:
                print "error : decompression impossible : not a gzip file"
                return None
            if toPrint:
                interval = time.time() - startTime
                print 'Decompression done :', interval, 'sec'
        elif compression=="bz2":
            results = StringIO.StringIO()
            a = bz2.decompress(sio.read())
            results.write(a)
            results.seek(0)
            try:
                pass
            except:
                print "error : decompression impossible : not a bz2 file"
                return None
            if toPrint:
                interval = time.time() - startTime
                print 'Decompression done :', interval, 'sec'
    else:
        results = sio
    # extracting the file into a pandas dataframe
    db = pd.read_csv(results,sep="\t", usecols = usecols)
    sio.close()
#     try:
#         db = pd.read_csv(results,sep="\t", usecols = usecols, dtype = dtype)
#     except:
#         print "error in the extraction of the dataframe"
#         return None
    interval = time.time() - startTime 
    if toPrint:
        print 'Dataframe created :', interval, 'sec'
    return db
    
def retrieveCKFtp(filename, compression = None, usecols=None, dtype=None, toPrint = False):
    """
    function that connects to the remote FTP serveur and extract a pandas dataframe
    the downloaded file must be compressed through gzip and containing a csv file.
    -- IN
    filename : the filename with its extension to be downloaded from the remote ftp server (string)
    compression : string that specifies the encoding of the file (string in [None,"gz","bz2"] default: None
    usecols : *optional* an array containing the name of the column to extract (string[]) default: None
    dtype : *optional* a dictionary containing the name of the columns and the type to cast them ({string:string}) default: None
    -- OUT
    db : a pandas dataframe containing the remote database (pandas.Dataframe)
    return None when an error occurs
    """
    if toPrint:
        print "==========================================="
        print "=== Connection to the remote FTP server ==="
        print "==========================================="
        print ""
        print "using chilkat.CkFTP2"
        print "loading :",filename
        print "" 
    startTime = time.time()
    # retrieving information about account on ftp server
    (user, password, host, port) = getAccount()
    if user==None:
        print "error : coudn't read the account information, abort download"
        return None
    ftp = CkFtp2()
    # connecting and logging in
    ftp.put_Passive(True)
    ftp.put_Hostname(host)
    ftp.put_Port(port)
    ftp.put_Username(user)
    ftp.put_Password(password)
    ftp.put_AuthTls(True)
    ftp.put_Ssl(False)
    ftp.put_SslProtocol("TLS 1.2")
    try:
        ftp.Connect()
    except:
        print "error : unable to connect to the ftp server"
        return None
    if toPrint:
        print "connected to the FTP server"
    # retrieving the remote file as a binary file
    data = CkByteData()
    try:
        ftp.GetRemoteFileBinaryData(filename,data)
    except:
        print "error : non-existing file :",filename
        return None
    interval = time.time() - startTime 
    if toPrint:
        print 'Data downloaded :', interval, 'sec'
    ftp.Disconnect()
    ftp.ClearDirCache()
    
    # Unziping the file
    if compression!=None:
        if compression=="gz":
            try:
                results = gzip.GzipFile(fileobj=cS(data.getData()))
            except:
                print "error : decompression impossible : not a gzip file"
                return None
            data.clear()
        elif compression=="bz2":
            try:
                results = bz2.BZ2File(fileobj=cS(data.getData()))
            except:
                print "error : decompression impossible : not a bz2 file"
                return None
            data.clear()
    else:
        results = cS(data.getData())
    # extracting the file into a pandas dataframe
    try:
        db = pd.read_csv(results,sep="\t",dtype = dtype, usecols = usecols)
    except:
        print "error in the extraction of the dataframe"
        return None
    interval = time.time() - startTime 
    if toPrint:
        print 'Dataframe created :', interval, 'sec'
    return db
    
