import logging
import re
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm, colors
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection

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
    def __init__(self, dfn, dat_fn):
        self.dfn = dfn
        self.dat_fn = dat_fn
    



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
                    
                    logger.info('line {}: adding field {} to record type RT={}'.format(i, str(f), rt))
                    self.record_types[rt]['fields'].append(f)
                    if f['name'] == 'RT':
                        self.record_types[rt]['format'] = f['format']

