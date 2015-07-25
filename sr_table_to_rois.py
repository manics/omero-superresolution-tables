#!/usr/bin/env python
# Create point ROIs from a superresolution particle table

import getpass
from itertools import izip
import os
import sys
import yaml

import omero
from omero.rtypes import rint, rdouble


NAMESPACE = '/test/superresolution/particletable'


def create_roi(im, x, y, t):
    p = omero.model.PointI()
    p.setTheT(rint(t))
    p.setCx(rdouble(x))
    p.setCy(rdouble(y))
    roi = omero.model.RoiI()
    roi.addShape(p)
    roi.setImage(im)
    return roi


def create_rois_for_plane(session, table, iid, t, pszx, pszy):
    rows = table.getWhereList(
        'frame==%d' % (t + 1), None, 0, table.getNumberOfRows(), 0)
    data = table.readCoordinates(rows)
    assert data.columns[1].name == 'x'
    assert data.columns[2].name == 'y'
    im = omero.model.ImageI(iid, False)

    rois = [create_roi(im, x / pszx, y / pszy, t) for (x, y) in izip(
        data.columns[1].values, data.columns[2].values)]
    ids = session.getUpdateService().saveAndReturnIds(rois)
    return ids


def open_table(session, tableid):
    t = session.sharedResources().openTable(omero.model.OriginalFileI(tableid))
    assert t
    assert t.getHeaders()[0].name == 'frame'
    return t


def connect(cfg):
    client = omero.client(cfg['host'])
    password = cfg.get('password')
    if not password:
        password = getpass.getpass()
    session = client.createSession(cfg['user'], password)
    return client, session


def to_nm(u):
    return omero.model.LengthI(
        u, omero.model.enums.UnitsLength.NANOMETER).getValue()


def run(fname):
    with open(fname) as f:
        cfg = yaml.load(f)

    client, session = connect(cfg)

    try:
        table = open_table(session, cfg['tableid'])
        try:
            cs = session.getContainerService()
            im = cs.getImages('Image', [cfg['imageid']], None)[0]
            sizeT = im.getPrimaryPixels().getSizeT().val
            pszx = to_nm(im.getPrimaryPixels().getPhysicalSizeX())
            pszy = to_nm(im.getPrimaryPixels().getPhysicalSizeY())
            # sizeT = 2
            for t in xrange(sizeT):
                print 't=%d' % t
                ids = create_rois_for_plane(
                    session, table, cfg['imageid'], t, pszx, pszy)
                print '\t%d rois' % len(ids)
        finally:
            table.close()
    finally:
        client.closeSession()


def main():
    if len(sys.argv) != 2:
        raise Exception('Usage: %s rois.yml' % os.path.basename(sys.argv[0]))
    return run(sys.argv[1])


if __name__ == '__main__':
    # Don't run if inside ipython
    try:
        __IPYTHON__
    except NameError:
        main()
