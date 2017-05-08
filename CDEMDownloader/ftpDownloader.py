from ftplib import FTP
from zipfile import ZipFile
from os import system

#Log into the geogratis ftp site
ftp = FTP('ftp.geogratis.gc.ca')
print ftp.login()

#Define the root directory for the CDEM data
root_directory = '/pub/nrcan_rncan/elevation/geobase_cded_dnec/50k_dem/'

#Get the list of DEM folders from the root directory
directory_list = ftp.nlst(root_directory)

#For each folder
for folder in directory_list[0:2]:
	
	#Switch into the folder
	ftp.cwd(folder)
	
	system('mkdir %s' % '/home/doopy/Documents/CDEMData' + folder[-4:])
	#Get the list of contents
	filenames = ftp.nlst()
	
	#For each file 
	for elem in filenames:
		
		#Check if it is a .zip file
		if elem[-4:] == '.zip':
			
			with open('/home/doopy/Documents/CDEMData' + folder[-4:] + '/' + elem,'w') as fileobj:
				
				ftp.retrbinary('RETR %s' % folder + '/' + elem, fileobj.write)
		

