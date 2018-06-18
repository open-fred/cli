""" Merges netCDF files covering the same month into one file.

Currently depends a lot on the input files having the right structure and
naming, i.e. a lot of assumptions are hardwired into this script. Maybe I'll
get to generalizing it by pulling information out of every file found, but for
now, this will have to do.

Parameters
----------

sys.argv[1]: directory
    The script will look into every tar file in this directory, an merge and
    netCDF file the tar file contains with the others, provided the name
    contains the right variables and date.
sys.argv[2]: path prefix
    The new netCDF file containing the merged data found will be written to
    the file "sys.argv[2]-YEAR_MONTH.nc" where YEAR_MONTH is pulled out of
    filenames considered for the merge.
sys.argv[3]: str, comma separated list of variables
    The argument is split by occurrences of "," and the results have any
    surrounding whitespace removed. The result is then treated as the list of
    variables eligible for a merge.
sys.argv[4]: regular expression
    A pattern to further limit the file names the script actually handles.
    Useful during development to not act on more files than absolutely
    necessary.
"""

from glob import glob, iglob
from itertools import chain
from pprint import pprint as pp
from tempfile import TemporaryDirectory as TD, mkdtemp
from subprocess import call
import os
import os.path as osp
import re
import sys
import tarfile

import xarray as xr

from add_dimension import add_dimension


def merge(variable, tar, store):
    with TD() as tmp:
        members = tar.getmembers()
        netcdfs = []
        for member in members:
            if not variable in member.name:
                continue
            print("Handling {}.".format(member.name))
            netcdfs.append(member.name)
            tar.extractall(tmp, members=[member])
            path = osp.join(tmp, member.name)
            fix_height = re.search("WSS_10M|WDIRlat_10M", member.name)
            if fix_height:
                print("Fixing height.")
                add_dimension(source=path, target=osp.join(tmp, "fixed"),
                        variable=fix_height.group(),
                        dimension="height",
                        position=2,
                        value=10.0,
                        new_name=fix_height.group()[:-4])
                call(["mv", osp.join(tmp, "fixed"), path])
        netcdfs = [osp.join(tmp, f) for f in netcdfs]
        print("Merging:")
        pp(netcdfs)
        target = osp.join(store, "{}.nc".format(variable))
        print("--> {}".format(target))
        current = netcdfs[0]
        for to_merge in netcdfs[1:]:
            print(to_merge)
            cds = xr.open_dataset(current, decode_cf=False)
            tds = xr.open_dataset(to_merge, decode_cf=False)
            mds = xr.merge((d[v] for d in [cds, tds] for v in d.data_vars))
            mds.to_netcdf(target + ".tmp", format='NETCDF4')
            for d in [cds, tds, mds]:
                d.close()
            call(["mv", target + ".tmp", target])
            current = target
    return target


if __name__ == "__main__":

    """ Variables:
    "WSS_zlevel", "T_zlevel", "P_zlevel", "Z0", "WDIRlat_zlevel",
    "ASWDIFD_S", "ASWDIR_S", "ASWDIR_NS2"
    """

    variables = [s.strip() for s in sys.argv[3].split(",")]
    with TD() as tmp:
        tars = list(tarfile.open(tar)
                for tar in iglob(osp.join(sys.argv[1], "*.tar"))
                if re.search(sys.argv[4], tar))
        for tar in tars:
            year = re.search("(\d\d\d\d_\d\d)\.tar", tar.name).groups()[0]
            merged = []
            for variable in variables:
                merged.append(merge(variable, tar, tmp))
            print("Merging:")
            pp(merged)
            mergetarget = osp.join(tmp, "merged.nc")
            print("--> {}".format(mergetarget))
            datasets = (xr.open_dataset(path, decode_cf=False) for path in merged)
            mds = xr.merge((d[v] for d in datasets for v in d.data_vars))
            mds.to_netcdf(mergetarget, format='NETCDF4')
            mds.close()
            call(["rm", "-r"] + merged)
            compresstarget = "{}-{}.nc".format(sys.argv[2], year)
            print("Compressing to '{}'.".format(compresstarget))
            ds = xr.open_dataset(mergetarget, decode_cf=False)
            for dv in ds.data_vars:
                if dv[0].isupper():
                    ds.data_vars[dv].encoding['least_significant_digit'] = 3
            ds.to_netcdf(compresstarget,
                    format='NETCDF4',
                    encoding={
                        v: {'complevel': 9, 'zlib': True}
                        for v in list(ds.variables)})
            ds.close()
            call(["rm", "-r", mergetarget])

