import pandas as pd
import requests
from StringIO import StringIO
import time, json
from flask.ext.babel import gettext
from .base import BulkTaskImport, BulkImportException
from werkzeug.datastructures import FileStorage
import io, time

class BulkTaskExcelImport(BulkTaskImport):

    """Class to import Excel tasks in bulk."""

    importer_id = "excel"

    def __init__(self, last_import_meta=None, **form_data):
        self.form_data = form_data

    def tasks(self):
        """Get tasks from a given URL/file."""
        filename = self.form_data['excel_filename']
        return self._get_excel_data(filename)

    def count_tasks(self):
        return len([task for task in self.tasks()])

    def _get_excel_data(self,filename):
        datapath = filename
        retry = 0
        f = None
        while retry<=10:
            try:
                f = io.BytesIO(FileStorage(io.open(datapath,'rb')).stream.read())
                break
            except Exception as e:
                time.sleep(2)
                retry = retry + 1
        if f is None:
            msg = ("Unable to load excel file for import, file {0}".format(datapath))
            raise BulkImportException(gettext(msg), 'error')
        data = pd.read_excel(f,header=None)
        return self._import_excel_tasks(data)


    def _import_excel_tasks(self,data):
        headers = data.iloc[0,:].dropna().tolist()
        if data.shape[1] != len(headers):
            msg = gettext('The file you uploaded has '
                          'Empty Headers')
            raise BulkImportException(msg)
            
        internal_fields = set(['state', 'quorum', 'calibration', 'priority_0',
                            'n_answers'])
        
        non_clash_headers = [header for header in headers if header not in internal_fields] 
        #if clash_headers:
        #    raise ValueError("{0} cannot be accepted in header field. please change them.".format(clash_headers))
        
        if len(set(headers)) != len(headers):
            msg = gettext('The file you uploaded has '
                          'multiple columns with same column name')
            raise BulkImportException(msg)
        else:
            data = data.iloc[1:,:]
            data.columns = headers
        data["info"] = data.loc[:,non_clash_headers].to_json(orient="records")
        data.drop(non_clash_headers,axis=1,inplace=True)
        return json.loads(data.to_json(orient='records'))
