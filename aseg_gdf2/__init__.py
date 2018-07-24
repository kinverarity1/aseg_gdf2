import logging
import os
import re

import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)


def read(filename):
    base, ext = os.path.splitext(filename)
    ext = ext.strip('.')
    if not ext:
        for dfn_ext in ('dfn', 'DFN'):
            dfn_fn = base + '.' + dfn_ext
            if os.path.isfile(dfn_fn):
                break
        for dat_ext in ('dat', 'DAT'):
            dat_fn = base + '.' + dat_ext
            if os.path.isfile(dat_fn):
                break
    elif ext in ('dfn', 'DFN'):
        dfn_fn = filename
        for dat_ext in ('dat', 'DAT'):
            dat_fn = base + '.' + dat_ext
            if os.path.isfile(dat_fn):
                break
    elif ext in ('dat', 'DAT'):
        dfn_fn = filename
        for dfn_ext in ('dfn', 'DFN'):
            dfn_fn = base + '.' + dfn_ext
            if os.path.isfile(dfn_fn):
                break
    logger.debug('dfn_fn {}: {}'.format(os.path.isfile(dfn_fn), dfn_fn))
    logger.debug('dat_fn {}: {}'.format(os.path.isfile(dat_fn), dat_fn))
    dfn = DFN(dfn_fn)
    return GDF2(dfn, dat_fn)



class GDF2(object):
    def __init__(self, dfn, dat_fn, **kws):
        self.dfn = dfn
        kws['dfn'] = dfn
        self.dat = DAT(dat_fn, **kws)
    
    

class DFN(object):
    def __init__(self, filename=None, content=None):
        if content is None and filename:
            content = open(filename, 'r').read()
        self.record_types = {}
        for i, line in enumerate(content.splitlines()):
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
                    # RT:A4
                    # COMMENTS:A76: NAME=Description
                    # EASTING: F12.2: UNIT=m, NAME=Easting, GDA94 / MGA zone 54
                    # TH-SCINT: I6: UNIT=cps, NULL=-9999, Thorium count in cps
                    # LINE:I6
                    # END DEFN
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

