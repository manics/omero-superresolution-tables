#!/usr/bin/env python
# Convert a Superresolution particle file to an OMERO.table

import getpass
import os
import pandas as pd
import sys
import yaml

import omero
import omero.grid


NAMESPACE = '/test/superresolution/particletable'


SUPPORTED_COLUMNS = [
    (omero.grid.LongColumn, 'frame', 'Frame number'),
    (omero.grid.DoubleColumn, 'x', 'x position (nm)'),
    (omero.grid.DoubleColumn, 'y', 'y position (nm)'),

    (omero.grid.DoubleColumn, 'z', 'z position (nm)'),
    (omero.grid.LongColumn, 'id', 'id or index number'),
    (omero.grid.DoubleColumn, 'chi2', 'chi-square'),
    (omero.grid.LongColumn, 'photons', 'number of photons'),
    (omero.grid.DoubleColumn, 'intensity', 'intensity'),
    (omero.grid.DoubleColumn, 'precision', 'precision or uncertainty (nm)'),
    (omero.grid.StringColumn, 'channel', 'channel name', 64),
]

REQUIRED_COLUMNS = ('frame', 'x', 'y')
# Put known columns in a standard order
SUPPORTED_COLUMN_NAMES = [col[1] for col in SUPPORTED_COLUMNS]


def get_column_mapping(columnmap, dfcolnames):
    """
    Figure out the column mapping, and find the required re-ordering of
    the input data columns
    """
    revcolumnmap = {}
    othercolumns = []
    for dfname, omeroname in columnmap.iteritems():
        if omeroname:
            if omeroname in revcolumnmap:
                raise Exception(
                    'OMERO column name specified multiple times: %s' %
                    omeroname)
            if omeroname not in SUPPORTED_COLUMN_NAMES:
                raise Exception(
                    'Unknown OMERO column name: %s' % omeroname)

            revcolumnmap[omeroname] = dfname
        else:
            othercolumns.append(dfname)

    # Now figure out how to reorder the columns
    dfcolindexmap = dict(cnameidx for cnameidx in zip(
        dfcolnames, xrange(len(dfcolnames))))

    omerocolumns = []
    dfcolindex = []

    for supported in SUPPORTED_COLUMNS:
        if supported[1] in revcolumnmap:
            omerocolumns.append(supported)
            dfcolindex.append(dfcolindexmap[revcolumnmap[supported[1]]])

    for cname in othercolumns:
        omerocolumns.append((omero.grid.DoubleColumn, cname))
        dfcolindex.append(dfcolindexmap[cname])

    return omerocolumns, dfcolindex


def create_omero_table(session, name, parent, ns, omerocolumns):
    cols = []
    for colspec in omerocolumns:
        cols.append(colspec[0](*colspec[1:]))
    t = session.sharedResources().newTable(0, name)
    t.initialize(cols)

    if parent:
        otype, oid = parent.split(':')
        obj = getattr(omero.model, '%sI' % otype)(oid, False)
        link = getattr(omero.model, '%sAnnotationLinkI' % otype)()
        link.setParent(obj)

        child = omero.model.FileAnnotationI()
        child.setFile(t.getOriginalFile())
        link.setChild(child)
        session.getUpdateService().saveObject(link)

    return t


def store_data(t, df, dfcolindex):
    rowchunk = 1024
    cols = t.getHeaders()
    assert len(cols) == len(dfcolindex)
    for offset in xrange(0, df.shape[1], 1024):
        endoffset = min(offset + rowchunk, df.shape[1])
        print 'Storing rows %d:%d' % (offset, endoffset)
        for (col, dfi) in zip(cols, dfcolindex):
            col.values = df.iloc[offset:(offset + endoffset), dfi]
    t.addData(cols)


def read_sr_file(fname):
    df = pd.read_table(fname, dtype={'id': int, 'frame': int})
    return df


def connect(cfg):
    client = omero.client(cfg['host'])
    password = cfg.get('password')
    if not password:
        password = getpass.getpass()
    session = client.createSession(cfg['user'], password)
    return client, session


def run(fname):
    with open(fname) as f:
        cfg = yaml.load(f)

    df = read_sr_file(cfg['input'])

    omerocolumns, dfcolindex = get_column_mapping(cfg['columnmap'], df.columns)
    client, session = connect(cfg)

    try:
        t = create_omero_table(
            session, cfg['tablename'], cfg['parent'], NAMESPACE, omerocolumns)
        try:
            store_data(t, df, dfcolindex)
        finally:
            t.close()
    finally:
        client.closeSession()


def main():
    if len(sys.argv) != 2:
        raise Exception('Usage: %s input.yml' % os.path.basename(sys.argv[0]))
    return run(sys.argv[1])

if __name__ == '__main__':
    main()
