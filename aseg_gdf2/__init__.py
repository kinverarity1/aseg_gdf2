import glob
import logging
import os
import re

import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)


def read(filename):
    for fn in glob.glob(filename + '*'):
        if fn.lower().endswith('dfn'):
            return GDF2(fn)
        elif fn.lower()[-3:] in ('dat', 'ddf', 'des', 'met'):
            for ext in ('DFN', 'dfn'):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2)
        else:
            for ext in ('DFN', 'dfn'):
                fn2 = fn[:-3] + ext
                if os.path.isfile(fn2):
                    return GDF2(fn2)
    raise OSError('No data package found at {}'.format(filename))



class GDF2(object):
    def __init__(self, dfn_filename, **kwargs):
        self.parse_dfn(dfn_filename)
        self.read_dat(self.find_dat_files())

    def parse_dfn(self, dfn_filename, join_null_data_rts=True, **kwargs):
        self.record_types = {}
        with open(dfn_filename, 'r') as f:
            self.dfn_filename = dfn_filename
            for i, line in enumerate(f):
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
                        m = re.match('([0-9]*)([a-zA-Z]{1})([0-9]+)', f['format'])
                        if m:
                            n = m.group(1)
                            try:
                                n = int(n)
                            except ValueError:
                                n = 1
                            f['shape'] = (n, )
                        else:
                            f['shape'] = (1, )
                        
                        logger.info('line {}: adding field {} to record type RT={}'.format(i, str(f), rt))
                        self.record_types[rt]['fields'].append(f)
                        if f['name'] == 'RT':
                            self.record_types[rt]['format'] = f['format']

    def find_dat_files(self):
        for ext in ('dat', 'DAT'):
            filename = self.dfn_filename[:-3] + ext
            if os.path.isfile(filename):
                return filename
        logger.error('No data file located.')
        return ''

    def read_dat(self, dat_filename, method='delimited'):
        self.dat_filename = dat_filename
        self.dask_dfs = {}
        if method == 'delimited':
            self.dask_dfs[self.dat_filename] = dd.read_table(self.dat_filename, delimiter='\s+')

    @property
    def data(self):
        return {
            '': {key: self.dask_dfs[self.dat_filename][key].values for key in self.dask_dfs[self.dat_filename].columns}
        }

    @property
    def null_fields(self):
        return tuple([f['name'] for f in self.record_types['']['fields']])



class DAT(object):
    '''Represent GDF2 data file (.dat).

    Args:
        filename (str): path of .dat file
        method (str): either 'delimited' - others to come. If
            'delimited' then ``kws['delimiter'] = '\s+'`` will
            be included for pandas.read_table
        **kws: keyword arguments for pandas.read_table

    '''
    def __init__(self, filename, method='delimited', dfn=None, **kws):
        self.dfn = dfn
        self.funcs = {}
        self.filename = filename
        self.method = method
        if self.method == 'delimited':
            if not 'delimiter' in kws:
                kws['delimiter'] = '\s+'
            self.kws = kws
            self.refresh()

    def refresh(self):
        self.df = dd.read_table(self.filename, **self.kws)

    def null_field(self, name):
        for i, f in enumerate(self.dfn.record_types['']['fields']):
            s = sum([f['shape'][0] for f in self.dfn.record_types['']['fields'][:i]])
            if f['name'] == name:
                field_data = self.df.iloc[:, s: s + f['shape'][0]].copy()
                if field_data.shape[1] == 1:
                    return field_data.iloc[:, 0].values
                else:
                    return field_data.values
    
    def null_fields(self):
        for f in self.dfn.record_types['']['fields']:
            yield self.null_field(f['name'])

