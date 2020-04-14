import json
import os

from pydrive.auth import GoogleAuth, ServiceAccountCredentials
from pydrive.drive import GoogleDrive
from apiclient import errors

class GoogleDriveApiClient():
    def __init__(self):
        '''Creating a google drive authenticated object so that we
        can pass it around in google_upload.py to do various tasks. Need
        to have a json like object in environment variables that holds the
        service account credentials'''
        gauth = GoogleAuth()
        scope = ['https://www.googleapis.com/auth/drive']

        auth_json = json.loads(os.environ['GDRIVE_AUTH_PASSWORD'])
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(auth_json, scope)

        self.drive = GoogleDrive(gauth)

    def list_files_in_folder(self, folder_name):
        return self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(folder_name)})


    def upload_file(self, file_name, folder_id):
        """Uploads a file to google drive folder that you've specified

        Arguments:
            drive {Google Drive Object} -- Google Drive Object passed in from google_drive_utils.py
            file_name {String} -- name of the file
            folder_id {String} -- folder id that you get from visiting the webpage and the
            folder that you want. It'll be the last part of the URL.
            Ex: https://drive.google.com/drive/folders/1E-19lkZJJ055ApIhD_HMFBqMFJBj4DfQ
            the folder id is: 1E-19lkZJJ055ApIhD_HMFBqMFJBj4DfQ
        """
        bern = self.drive.CreateFile({
            'title': f'{file_name}',
            'mimeType':'text/csv',
            "parents": [
                {
                    "kind": "drive#fileLink",
                    "id": f'{folder_id}'
                }
            ]
        })

        bern.SetContentFile(f'./{file_name}')
        bern.Upload()

    def delete_files(self, to_delete):
        for file in to_delete:
            try:
                x = self.drive.CreateFile({'id': file['id']})
                print(f'deleting: {file["title"]}')
                x.Delete()
            except Exception:
                pass
