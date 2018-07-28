import glob
import logging
import os
import re

import pandas as pd

logger = logging.getLogger(__name__)


def read(filename, **kwargs):
    for fn in glob.glob(filename + '*'):
        if fn.lower().endswith('dfn'):
            return GDF2(fn, **kwargs)
        elif fn.lower()[-3:] in ('dat', 'ddf', 'des', 'met'):
            for ext in ('DFN', 'dfn'):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2, **kwargs)
        else:
            for ext in ('DFN', 'dfn'):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2, **kwargs)
    raise OSError('No data package found at {}'.format(filename))



class GDF2(object):
    '''Load GDF2 data package.

    Arguments:
        dfn_filename (str)
        method (str): how to read the data file. Either assume it is
            delimited by whitespace (``'whitespace'``) or use the 
            field widths specified in the .dfn file (``'fixed-widths'``).

    '''
    def __init__(self, dfn_filename, method='whitespace', **kwargs):
        self._parse_dfn(dfn_filename)
        self._parse_dat(self._find_dat_files(), method)

    def _parse_dfn(self, dfn_filename, join_null_data_rts=True, **kwargs):
        self.record_types = {}
        with open(dfn_filename, 'r') as f:
            self.dfn_filename = dfn_filename
            self.dfn_contents = f.read()
        for i, line in enumerate(self.dfn_contents.splitlines()):
            if not line.startswith('DEFN'):
                logger.warning('line {} does not begin with DEFN: {}'.format(i, line))
                continue
            
            if not ';' in line:
                logger.debug('line {}: No field definitions: {}'.format(i, line))
                m = re.search('RT=(\w*)', line)
                if m:
                    rt = m.group(1)
                    logger.info('line {}: added record type RT={}'.format(i, rt))
                    self.record_types[m.group(1)] = {'fields': [], 'format': None}
            else:
                m = re.search('RT=(\w*)', line)
                if m:
                    rt = m.group(1)
                    if join_null_data_rts:
                        if rt == 'DATA':
                            rt = ''
                    if not rt in self.record_types:
                        logger.info('line {}: added record type RT={}'.format(i, rt))
                        self.record_types[rt] = {'fields': [], 'format': None}
                
                fields = line.split(';')[1:]
                logger.debug('line {}: looping through {} field definitions for RT={}'.format(i, len(fields), rt))
                for field in fields:
                    f = {
                        'name': '',
                        'format': '',
                        'unit': '',
                        'null': None,
                        'long_name': '',
                        'comment': '',
                    }
                    field = field.strip()
                    if field == 'END DEFN':
                        logger.debug('line {}: end of field definition for record type RT={}'.format(i, rt))
                        continue
                    f['name'], remaining = field.strip().split(':', 1)
                    if ':' in remaining:
                        f['format'], remaining = remaining.strip().split(':', 1)
                        for chunk in remaining.split(','):
                            chunk = chunk.strip()
                            
                            m = re.search('UNITS? *= *(.*)', chunk)
                            if m:
                                f['unit'] = m.group(1)
                                continue
                            
                            m = re.search('NAME *= *(.*)', chunk)
                            if m:
                                f['long_name'] = m.group(1)
                                continue
                            
                            m = re.search('NULL *= *(.*)', chunk)
                            if m:
                                f['null'] = m.group(1)
                                continue
                            
                            f['comment'] = chunk
                    else:
                        f['format'] = remaining
                    m = re.match('([0-9]*)([a-zA-Z]{1})([0-9]+).*', f['format'])
                    if m:
                        n = m.group(1)
                        try:
                            n = int(n)
                        except ValueError:
                            n = 1
                        f['cols'] = n
                        f['width'] = int(m.group(3))
                    else:
                        logger.critical('No field width found in format code {}'.format(f['format']))
                    
                    logger.info('line {}: adding field {} to record type RT={}'.format(i, str(f), rt))
                    self.record_types[rt]['fields'].append(f)
                    if f['name'] == 'RT':
                        self.record_types[rt]['format'] = f['format']

    def _find_dat_files(self):
        for ext in ('dat', 'DAT'):
            filename = self.dfn_filename[:-3] + ext
            if os.path.isfile(filename):
                return filename
        logger.error('No data file located.')
        return ''

    def _parse_dat(self, dat_filename, method='whitespace'):
        self._read_dat = {
            '': {
                'func': None,
                'args': [dat_filename, ],
                'kwargs': {
                    'names': self.column_names(''),
                    'header': None,
                }
            }
        }
        if method == 'fixed-widths':
            self._read_dat['']['func'] = pd.read_fwf
            self._read_dat['']['kwargs'].update({
                'widths': [f['width'] for f in self.record_types['']['fields']],
                'na_filter': False,
            })
        elif method == 'whitespace':
            self._read_dat['']['func'] = pd.read_table
            self._read_dat['']['kwargs'].update({
                'delimiter': '\s+',
            })

    def field_names(self, record_type=''):
        '''Return field names from the .dfn file.

        This may not correspond to columns in the data table / pd.DataFrame,
        because a field may be defined as having multiple values (== "columns")
        for each record (== "row"). See column_names() for an alternative.

        '''
        return [f['name'] for f in self.record_types[record_type]['fields']]

    def column_names(self, record_type=''):
        '''Provide a name for each column of the data table / pd.DataFrame
        object.

        This accounts for the fact that 2D fields are allowed e.g. a 
        single field "Con" in the .dfn file may account for 30 columns in the 
        .dat file with a format code of say 30F10.5.

        This method will expand the single field name "Con" into 30 field
        names, "Con0", "Con1", and so on.

        '''
        names = []
        for field in self.record_types[record_type]['fields']:
            if field['cols'] == 1:
                names.append(field['name'])
            else:
                for i in range(field['cols']):
                    names.append('{}[{:d}]'.format(field['name'], i))
        return names

    def df(self, record_type='', **kwargs):
        rt = self._read_dat[record_type]
        kws = dict(**rt['kwargs'])
        kws.update(kwargs)
        return rt['func'](*rt['args'], **kws)

    def df_chunked(self, record_type='', chunksize=200000, **kwargs):
        return self.df(record_type=record_type, chunksize=chunksize, **kwargs)

    def iterrows(self, *args, **kwargs):
        for chunk in self.df_chunked(*args, **kwargs):
            for row in chunk.itertuples():
                yield row._asdict()

    def get_field(self, field_name, record_type='', **kwargs):
        column_names = [n for n in self.column_names() if re.match(field_name + '(\[[0-9]+\])?', n)]
        data = self.df(record_type=record_type, usecols=column_names, **kwargs)
        if data.shape[1] == 1:
            return data.iloc[:, 0].values
        else:
            return data.values

    def get_field_chunked(self, field_name, record_type='', chunksize=200000, **kwargs):
        column_names = [n for n in self.column_names() if re.match(field_name + '(\[[0-9]+\])?', n)]
        for data in self.df_chunked(record_type=record_type, usecols=column_names, chunksize=chunksize, **kwargs):
            if data.shape[1] == 1:
                yield data.iloc[:, 0].values
            else:
                yield data.values
 