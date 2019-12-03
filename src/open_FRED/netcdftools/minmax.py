from glob import iglob
from tempfile import TemporaryDirectory as TD
import os
import os.path as osp
import tarfile

import xarray as xr


if __name__ == "__main__":

    bounds = {}

    with TD() as tmp:
        print(tmp)
        tars = list(tarfile.open(tar) for tar in iglob("*.tar"))
        for tar in tars:
            members = tar.getmembers()
            for member in members:
                tar.extractall(tmp, members=[member])
                ds = xr.open_dataset(osp.join(tmp, member.name))
                for dv in [
                    ds.data_vars[k] for k in ds.data_vars if k[0].isupper()
                ]:
                    new = (dv.min().data.min(), dv.max().data.max())
                    old = bounds.get(dv.name, (float("inf"), float("-inf")))
                    bounds[dv.name] = (
                        min(old[0], new[0]),
                        max(old[1], new[1]),
                    )
                ds.close()
                os.remove(osp.join(tmp, member.name))

    print("name;min;max")
    for k in sorted(bounds):
        print("{k};{bounds[0]};{bounds[1]}".format(k=k, bounds=bounds[k]))
