import pandas as pd
import requests
from StringIO import StringIO
import time, json
from flask.ext.babel import gettext
from .base import BulkTaskImport, BulkImportException
from werkzeug.datastructures import FileStorage

class BulkTaskExcelImport(BulkTaskImport):

    """Class to import Excel tasks in bulk."""

    importer_id = "excel"

    def __init__(self, last_import_meta=None, **form_data):
        self.form_data = form_data
        datapath = self.form_data['excel_filename']
        self.df = pd.read_excel(datapath)

    def tasks(self):
        """Get tasks from a given URL/file."""
        return self._import_excel_tasks()

    def count_tasks(self):
        return self.df.shape[0]

    def _import_excel_tasks(self):
        data = self.df
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
