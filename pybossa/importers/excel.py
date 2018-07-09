import pandas as pd
import requests
from StringIO import StringIO
import time, json
from flask.ext.babel import gettext
from .base import BulkTaskImport, BulkImportException
from werkzeug.datastructures import FileStorage

class BulkTaskExcelImport(BulkTaskImport):

    """Class to import CSV tasks in bulk."""

    importer_id = "Excel"

    def __init__(self, last_import_meta=None, **form_data):
        self.form_data = form_data

    def tasks(self):
        """Get tasks from a given URL."""
        datapath = self.form_data['Excel_filename']
        self.df = pd.read_excel(datapath)
        return self._import_excel_tasks()

    def count_tasks(self):
        return self.df.shape[0]

    def _import_excel_tasks(self):
        data = pd.DataFrame(self.df)
        headers = data.iloc[0,:].dropna().tolist()
        
        if data.shape[1] != len(headers):
            raise ValueError("Headers cannot be empty")
            
        internal_fields = set(['state', 'quorum', 'calibration', 'priority_0',
                            'n_answers'])
        
        non_clash_headers = [header for header in headers if header not in internal_fields] 
        #if clash_headers:
        #    raise ValueError("{0} cannot be accepted in header field. please change them.".format(clash_headers))
        
        if len(set(headers)) != len(headers):
            raise ValueError("Ensure that column names are different")
        else:
            data = data.iloc[1:,:]
            data.columns = headers
        data["info"] = data.loc[:,non_clash_headers].to_json(orient="records")
        data.drop(non_clash_headers,axis=1,inplace=True)
        return json.loads(data.to_json(orient='records'))