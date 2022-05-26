import os
from ftplib import FTP_TLS

from config import *

# Secret variables
ftp_user = os.environ.get('FTP_USER')
ftp_pass = os.environ.get('FTP_PASS')

_old_makepasv = FTP_TLS.makepasv
def _new_makepasv(self):
    host,port = _old_makepasv(self)
    host = self.sock.getpeername()[0]
    return host,port

def sos_fetch(ftp_address=ftp_address, ftp_user=ftp_user, ftp_pass=ftp_pass, ftp_directory=ftp_directory, ftp_file=return_zip):
    FTP_TLS.makepasv = _new_makepasv
    
    ftps = FTP_TLS()
    ftps.set_debuglevel(2)
    ftps.connect(ftp_address, 21)
    ftps.login(ftp_user, ftp_pass)
    ftps.prot_p()
    ftps.cwd(ftp_directory)
    
    print(f"Retrieving file {ftp_file}")
    local_file = open(ftp_file, 'wb')
    ftps.retrbinary('RETR ' + ftp_file, local_file.write)
    local_file.close()
            
    ftps.quit()
