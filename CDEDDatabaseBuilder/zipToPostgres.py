import os
import zipfile
import untangle
import psycopg2 as pg

data_path = '/home/doopy/Documents/CDEMData'

folders = os.listdir(data_path)

db_conn = pg.connect(host="localhost",database="CDED", user="postgres", password="postgres")
db_curr = db_conn.cursor()

folder_count = 0

for folder in folders:
	
	folder_count += 1
	
	zip_list = os.listdir(data_path + '/' + folder)
	
	zip_count = 1
	
	for elem in zip_list:
		
		try:
			zf = zipfile.ZipFile(data_path + '/' + folder + '/' + elem)
		
			os.system('clear')
			
			print 'Extracting folder ' + folder
			print str(folder_count) + '/' + str(len(folders))
			print 'Extracting zipfile ' + elem 
			print str(zip_count) + '/' + str(len(zip_list))
			zip_count +=1
	
			os.system('mkdir ' + data_path + '/' + folder + '/' + elem[0:6])
			zf.extractall(data_path + '/' + folder + '/' + elem[0:6])
			
			extract_list = os.listdir(data_path + '/' + folder + '/' + elem[0:6])
			
			metadata = {}
			
			for extract in extract_list:
				
				if extract[-4:] == '.xml':
					
					xml_file = untangle.parse(data_path + '/' + folder + '/' + elem[0:6] + '/' + extract)
					
					metadata['xml'] = extract
					
					metadata['north'] = xml_file.gmd_MD_Metadata.gmd_identificationInfo.gmd_MD_DataIdentification.gmd_extent.gmd_EX_Extent.gmd_geographicElement.gmd_EX_GeographicBoundingBox.gmd_northBoundLatitude.gco_Decimal.cdata
					metadata['east'] = xml_file.gmd_MD_Metadata.gmd_identificationInfo.gmd_MD_DataIdentification.gmd_extent.gmd_EX_Extent.gmd_geographicElement.gmd_EX_GeographicBoundingBox.gmd_eastBoundLongitude.gco_Decimal.cdata
					metadata['south'] = xml_file.gmd_MD_Metadata.gmd_identificationInfo.gmd_MD_DataIdentification.gmd_extent.gmd_EX_Extent.gmd_geographicElement.gmd_EX_GeographicBoundingBox.gmd_southBoundLatitude.gco_Decimal.cdata
					metadata['west'] = xml_file.gmd_MD_Metadata.gmd_identificationInfo.gmd_MD_DataIdentification.gmd_extent.gmd_EX_Extent.gmd_geographicElement.gmd_EX_GeographicBoundingBox.gmd_westBoundLongitude.gco_Decimal.cdata
				
				elif extract[-5:] == 'e.dem':
					metadata['img_east'] = extract
					
				elif extract[-5:] == 'w.dem':
					metadata['img_west'] = extract
				
			try:
				db_curr.execute("INSERT INTO terrain_models VALUES ('" + elem[0:6] + "'," + metadata['east'] + "," + metadata['west'] + "," + metadata['north']  + "," + metadata['south'] + ",'" + metadata['img_east'] + "','" + metadata['img_west'] + "','" + metadata['xml'] + "');")
							
			except pg.IntegrityError:
					
					'Record ' + elem[0:6] + ' already inserted into database'
		except IOError:
			
			'Directory Exists'
			
			xml_file = None
			del xml_file
		db_conn.commit()
		
db_conn.close()
