# -*- coding: utf-8 -*-

from __future__ import print_function
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file as oauth_file, client, tools

import os
import sys
import httplib2
import time
import mysql.connector
import json

"""
DROP TABLE IF EXISTS `documents`;
CREATE TABLE IF NOT EXISTS `documents` (
  `row_id` int(11) NOT NULL AUTO_INCREMENT,
  `doc_id` varchar(2500) NOT NULL,
  `title` varchar(500) NOT NULL,
  `current_folder` varchar(500) NOT NULL,
  `full_path` varchar(2500) NOT NULL,
  `mime` varchar(500) NOT NULL,
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""


#pip install mysql-connector-python-rf
#If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/drive'


def list_files(service):
    page_token = None
    while True:
        param = {}
        if page_token:
            param['pageToken'] = page_token
        files = service.files().list(**param).execute()
        for item in files['items']:
            yield item
        page_token = files.get('nextPageToken')
        if not page_token:
            break

def get_folder_name(drive_service, id, folders):

    parents = drive_service.parents().list(fileId=id).execute()
    for parent in parents['items']:
        #print('File ID: %s' % item.get('id'))
        #print('Parent: %s' % parent)

        time.sleep(0.1)
        
        data = drive_service.files().get(fileId=parent.get('id'), fields='id,title').execute()
        #print(data.get('title'))

        folders.append(data.get('title'))
        get_folder_name(drive_service, data.get('id'), folders)
        return folders #data.get('title') + '/' + get_folder_name(drive_service, data.get('id'))

    return folders


def download_files(mycursor, drive_service):

    mycursor.execute("SELECT replace(replace(concat(folder_path, '/', current_folder, '/'),'///','/'),'//','/') as full_path, export_links, title, mime, doc_id FROM `documents` inner join folders on parent_id = folders.folder_id where  mime like 'application/vnd.google-apps.%' and mime != 'application/vnd.google-apps.form'")

    myresult = mycursor.fetchall()

    for row in myresult:

        download_url = row[1]
        ext = ""

        if row[3] == 'application/vnd.google-apps.spreadsheet':
            ext = '.xlsx'
        if row[3] == 'application/vnd.google-apps.document':
            ext = '.docx'
        if row[3] == 'application/vnd.google-apps.drawing':
            ext = '.jpg'
        if row[3] == 'application/vnd.google-apps.presentation':
            ext = '.pptx'
            
        outfile = row[0] + row[2] + ext

        print(row[4])

        if not os.path.exists(os.path.dirname(outfile)):
            os.makedirs(os.path.dirname(outfile))
        
        if not os.path.exists(outfile):
            
            if len(download_url):
                print( "downloading %s" % outfile)
                resp, content = drive_service._http.request(download_url)
                if resp.status == 200:
                    if os.path.isfile(outfile):
                        print ("ERROR, %s already exist" % outfile)
                    else:
                        with open(outfile, 'wb') as f:
                            f.write(content)
                        print ("OK")
                else:
                    print ('ERROR downloading %s' % row[2])



def build_document_list(mycursor, drive_service):

##    mycursor.execute('DROP TABLE IF EXISTS `documents`; CREATE TABLE IF NOT EXISTS `documents` (  `row_id` int(11) NOT NULL AUTO_INCREMENT,  `doc_id` varchar(2500) NOT NULL,  `title` varchar(500) NOT NULL,  `current_folder_id` varchar(255) NOT NULL,  `current_folder` varchar(500) NOT NULL,  `full_path` varchar(2500) NOT NULL,  `mime` varchar(500) NOT NULL,  `export_links` varchar(5000) NOT NULL,  PRIMARY KEY (`row_id`)) ENGINE=InnoDB AUTO_INCREMENT=9255 DEFAULT CHARSET=utf8;', multi=True)
##    mydb.commit()

    for item in list_files(drive_service):

        if len(item['parents']) > 0:
            parent_id = item['parents'][0]['id']
        else:
            parent_id = 0
        
        exportLink = ""

        if 'mimeType' in item and 'application/vnd.google-apps.spreadsheet' in item['mimeType']:
            exportLink = item['exportLinks']['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
        elif 'mimeType' in item and 'aapplication/vnd.google-apps.document' in item['mimeType']:
            exportLink = item['exportLinks']['application/vnd.openxmlformats-officedocument.wordprocessingml.document']
        elif 'mimeType' in item and 'application/vnd.google-apps.drawing' in item['mimeType']:
            exportLink = item['exportLinks']['image/jpeg']
        elif 'mimeType' in item and 'application/vnd.google-apps.form' in item['mimeType']:
            exportLink = "???"
        elif 'mimeType' in item and 'application/vnd.google-apps.presentation' in item['mimeType']:
            exportLink = item['exportLinks']['application/vnd.openxmlformats-officedocument.presentationml.presentation']


        if 'downloadUrl' in item:
            exportLink = item['downloadUrl']

        if len(exportLink) > 0:
            sql = "INSERT INTO documents (doc_id, title, mime, export_links, parent_id) VALUES (%s, %s, %s, %s, %s)"
            
            val = (item.get('id'), item['title'], item['mimeType'], exportLink, parent_id)
            mycursor.execute(sql, val)

            mydb.commit()

##            print(mycursor.rowcount, "record inserted.")

def update_folder_paths_for_documents(mycursor, drive_service):

    mycursor.execute("select distinct parent_id from documents where parent_id not in (select folder_id from folders)")

    myresult = mycursor.fetchall()

    for row in myresult:

##        print(row)

        try:
            parents = drive_service.parents().list(fileId=row[0]).execute()
            for parent in parents['items']:
                

               fodlers = get_folder_name(drive_service, parent.get('id'), folders = [])
               fodlers.reverse()

               # add the current folder to the end of the array
               data = drive_service.files().get(fileId=parent.get('id'), fields='id,title').execute()
               fodlers.append(data.get('title'))

               path = '/'.join(fodlers) + '/'

               print(path)

               sql = "insert into folders (folder_id, folder_path) values(%s, %s)"

               val = (row[0], path)
               mycursor.execute(sql, val)

##               print(mycursor._executed)

               mydb.commit()
##               print(mycursor.rowcount, "record(s) affected")
        except:
            pass
            print("error id: " + str(row[0]))




def recreate_folder_structure(mycursor, drive_service):

    mycursor.execute("SELECT replace(replace(concat(folder_path, '/', current_folder, '/'),'///','/'),'//','/') as full_path FROM `documents` inner join folders on parent_id = folders.folder_id where mime like 'application/vnd.google-apps.%' group by replace(replace(concat(folder_path, '/', current_folder, '/'),'///','/'),'//','/') ")

    myresult = mycursor.fetchall()

    for row in myresult:

        print(row[0])
        if not os.path.exists(row[0]):
            os.makedirs(row[0])


def get_current_folder(mycursor, drive_service):

    mycursor.execute("SELECT doc_id, parent_id FROM `documents` where current_folder = '' and mime like 'application/vnd.google-apps.%'  ")

    myresult = mycursor.fetchall()

    for row in myresult:
        
        data = drive_service.files().get(fileId=row[1], fields='id,title').execute()

        sql = "update documents set current_folder = %s where doc_id = %s "

        val = (data.get('title'), row[0])
        mycursor.execute(sql, val)
        mydb.commit()
        

if __name__ == '__main__':


    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="root",
            database="google_docs",
            unix_socket="/Applications/MAMP/tmp/mysql/mysql.sock"
    )

    mycursor = mydb.cursor()

    store = oauth_file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secret_142671969192-7o7c3lamr5nbccsa4cuj4npuvgsh8nkt.apps.googleusercontent.com.json', SCOPES)
        creds = tools.run_flow(flow, store)


#    service = build('drive', 'v3', http=creds.authorize(Http()))

    drive_service = build('drive', 'v2', http=creds.authorize(Http()))


##    build_document_list(mycursor, drive_service)
##    get_current_folder(mycursor, drive_service)
##    update_folder_paths_for_documents(mycursor, drive_service)
    recreate_folder_structure(mycursor, drive_service)
    download_files(mycursor, drive_service)

    print('finished')
