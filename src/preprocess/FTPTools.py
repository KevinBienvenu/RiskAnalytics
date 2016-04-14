'''
Created on 12 Apr 2016

@author: Kevin Bienvenu

Module that handles ftp connection to remote server.
The server security protocol must be TLS1.2
'''
import StringIO
from cStringIO import StringIO as cS
from ftplib import FTP_TLS
import gzip
import time

from chilkat import CkFtp2, CkByteData

import pandas as pd


def getAccount(filename = "login_ftp.txt"):
    with open(filename, "r") as fileToRead:
        for line in fileToRead:
            l = line.split(" ")
            if l[0] == "user":
                user = l[1]
            if l[0] == "password":
                password = l[1]
            if l[0] == "host":
                host = l[1]
            if l[0] == "port":
                port = (int)(l[1])
    return (user, password, host, port)


localfile = "../RiskAnalytics/cameliaLiensEntites.csv"

def connectFtplib(filename, usecols, dtype):
    start_time = time.time()
    ftp = FTP_TLS()
    (user, password, host, port) = getAccount()
#     ftp.passiveserver = True
    print ftp.connect(host,port)
    print ftp.login(user,password)
    print ftp.prot_p()

    sio = StringIO.StringIO()
    def handle_binary(more_data):
        sio.write(more_data)
    
    ftp.retrbinary("RETR "+filename, callback=handle_binary)
    interval = time.time() - start_time  
    print 'Data downloaded :', interval, 'sec'
    sio.seek(0) # Go back to the start
    results = gzip.GzipFile(fileobj=sio)
    db = pd.read_csv(results,sep="\t", usecols = usecols, dtype = dtype)
    interval = time.time() - start_time  
    print 'Dataframe created :', interval, 'sec'
    return db
    
def connectCKFtp(filename, usecols, dtype):

    print "==========================================="
    print "=== Connection to the remote FTP server ==="
    print "==========================================="
    print ""
    print "loading :",filename
    print "" 
    startTime = time.time()
    (user, password, host, port) = getAccount()
    ftp = CkFtp2()
    ftp.put_Passive(True)
    ftp.put_Hostname(host)
    ftp.put_Port(port)
    ftp.put_Username(user)
    ftp.put_Password(password)
    ftp.put_AuthTls(True)
    ftp.put_Ssl(False)
    ftp.put_SslProtocol("TLS 1.2")
    ftp.Connect()
    print "   => logged in"
    data = CkByteData()
    ftp.GetRemoteFileBinaryData(filename,data)
    print "   => data downloaded"
    ftp.Disconnect()
    ftp.ClearDirCache()
    results = gzip.GzipFile(fileobj=cS(data.getData()))
    print "   => decompression done"
    data.clear()
    db = pd.read_csv(results,sep="\t",dtype = dtype, usecols = usecols)
    print "   => dataframe created"
    print ""
    print "End of the import - time :", (time.time()-startTime),"secs"
    print ""
    return db
    