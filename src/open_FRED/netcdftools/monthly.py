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
sys.argv[5]: filename
    A single file containing all merged data fill be created using this name.
"""

from glob import iglob
from pprint import pprint as pp
from tempfile import TemporaryDirectory as TD
from subprocess import call
import os.path as osp
import re
import sys
import tarfile

from dask.diagnostics import ProgressBar
import xarray as xr

from add_dimension import add_dimension


""" Command lines:

python ../monthly.py '../' './no-height' 'ASWDIFD_S, ASWDIR_S, ASWDIR_NS2'
python ../monthly.py '../' './' \
                     'WSS_zlevel, T_zlevel, P_zlevel, WDIRlat_zlevel'

Special: Z0
(no height but weird time bounds due to mean/instantaneous measurement)

python ../../cli/code/monthly.py './' './import-test' \
                                 'WSS_zlevel, P_zlevel' \
                                 '2015' \
                                 '2015.WSS.P.nc'

"""


def merge(variable, tar, store):
    # chunks={"time": 12, "rlat": 11, "rlon": 11}
    chunks = {}
    with TD(dir="./_T_") as tmp:
        members = tar.getmembers()
        netcdfs = []
        for member in members:
            if variable not in member.name:
                continue
            print("Handling {}.".format(member.name))
            netcdfs.append(member.name)
            tar.extractall(tmp, members=[member])
            path = osp.join(tmp, member.name)
            fix_height = re.search("WSS_10M|WDIRlat_10M", member.name)
            if fix_height:
                print("Fixing height.")
                add_dimension(
                    source=path,
                    target=osp.join(tmp, "fixed"),
                    variable=fix_height.group(),
                    dimension="height",
                    position=2,
                    value=10.0,
                    new_name=fix_height.group()[:-4],
                )
                call(["mv", osp.join(tmp, "fixed"), path])
        netcdfs = [osp.join(tmp, f) for f in netcdfs]
        print("Merging:")
        pp(netcdfs)
        target = osp.join(store, "{}.nc".format(variable))
        print("--> {}".format(target))
        datasets = [
            xr.open_dataset(n, decode_cf=False, chunks=chunks) for n in netcdfs
        ]
        merged = xr.merge(
            d[v] for d in datasets for v in d.data_vars if v != "rotated_pole"
        )
        computation = merged.to_netcdf(target, format="NETCDF4", compute=False)
        with ProgressBar():
            computation.compute()
    return target


if __name__ == "__main__":

    """ Variables:
    "WSS_zlevel", "T_zlevel", "P_zlevel", "Z0", "WDIRlat_zlevel",
    "ASWDIFD_S", "ASWDIR_S", "ASWDIR_NS2"
    """

    variables = [s.strip() for s in sys.argv[3].split(",")]
    # chunks={"time": 12, "rlat": 11, "rlon": 11}
    chunks = {}
    with TD(dir="./_T_/") as tmp:
        tars = list(
            tarfile.open(tar)
            for tar in iglob(osp.join(sys.argv[1], "*.tar"))
            if re.search(sys.argv[4], tar)
        )
        everything = []
        for tar in tars:
            year = re.search(r"(\d\d\d\d_\d\d)\.tar", tar.name).groups()[0]
            merged = []
            for variable in variables:
                merged.append(merge(variable, tar, tmp))
            print("Merging/Compressing to:")
            pp(merged)
            mergetarget = "{}-{}.nc".format(sys.argv[2], year)
            print("--> {}".format(mergetarget))
            datasets = (
                xr.open_dataset(path, decode_cf=False, chunks=chunks)
                for path in merged
            )
            data_vars = [d[v] for d in datasets for v in d.data_vars]
            for dv in data_vars:
                if dv.name[0].isupper():
                    dv.encoding["least_significant_digit"] = 3
            ds = xr.merge(data_vars)
            computation = ds.to_netcdf(
                mergetarget,
                format="NETCDF4",
                compute=False,
                encoding={
                    v: {"complevel": 9, "zlib": True}
                    for v in list(ds.variables)
                },
            )
            with ProgressBar():
                computation.compute()
            call(["rm", "-r"] + merged)
            ds.close()
            everything.append(mergetarget)

        """
        print("Compressing to {}.".format(sys.argv[5]))
        computation = (
                xr.merge(
                    d[v]
                    for d in [
                        xr.open_dataset(p, chunks=chunks)
                        for p in everything]
                    for v in d.data_vars)
                .to_netcdf(sys.argv[5], format='NETCDF4',
                    encoding={
                        v: {'complevel': 9, 'zlib': True}
                        for v in list(ds.variables)},
                    compute=False))
        with ProgressBar():
            computation.compute()
        #call(["mv", tmpeverything, sys.argv[5]])
        """
    print("All done.")
