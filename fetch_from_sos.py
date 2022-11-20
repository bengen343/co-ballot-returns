import os
from ftplib import FTP_TLS

from config import *

_old_makepasv = FTP_TLS.makepasv
def _new_makepasv(self):
    host,port = _old_makepasv(self)
    host = self.sock.getpeername()[0]
    return host,port

def sos_file_fetch(
    ftp_address: str, 
    ftp_user: str, 
    ftp_pass: str, 
    ftp_directory: str, 
    ftp_file: str
) -> str:
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

    return(f"Successfully downloaded {ftp_file}.")
