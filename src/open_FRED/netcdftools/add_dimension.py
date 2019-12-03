from collections import OrderedDict as OD

import click
import xarray as xr


"""
ALL = object()
SMALL = object()

heights = [10, 80, 100, 120, 140, 160, 200, 240]
small = heights[:3]
path_pattern = "oF_00625_MERRA2_expC17.WSS_{}M.2015_02.DEplus.nc"

wss = {h: xr.open_dataset(path_pattern.format(h)) for h in heights[2:]}

dims = list(wss[10]['WSS_10M'].dims)
dims.insert(1, 'height')

wss[10] = xr.DataArray(
        name='WSS',
        data=[[v] for v in wss[10]['WSS_10M'].values],
        coords=OD(
            (c, ([10] if c == 'height' else wss[10].coords[c]))
            for c in wss[80]['WSS'].dims),
        dims=wss[80]['WSS'].dims)

wss[ALL] = xr.merge([wss[h] for h in heights[2:]])
"""


def add_dimension(
    source, target, variable, dimension, position, value, new_name
):
    if position != 2:
        raise NotImplementedError(
            "Can only insert a new dimension at position 2 for now.\n"
            + "Got: {}".format(position)
        )
    if new_name is None:
        new_name = variable
    ds = xr.open_dataset(source)
    dimensions = list(ds[variable].dims)
    dimensions.insert(1, dimension)
    da = xr.DataArray(
        name=new_name,
        data=[[v] for v in ds[variable].values],
        coords=OD(
            (c, ([value] if c == dimension else ds[variable].coords[c].values))
            for c in dimensions
        ),
        dims=dimensions,
    )
    da.attrs.update(ds.attrs)
    merged = xr.merge([ds[v] for v in ds.data_vars if v != variable] + [da])
    merged.to_netcdf(target, format="NETCDF3_64BIT", unlimited_dims=("time",))


@click.command()
@click.argument("source", type=click.File("rb"), default="-")
@click.argument("target", type=click.File("wb"), default="-")
@click.option(
    "--name",
    "--variable",
    "-n",
    required=True,
    prompt=True,
    help=("Name of the variable you want to add a dimension to."),
)
@click.option(
    "--dimension",
    "-d",
    required=True,
    prompt=True,
    help=("The dimension you want to add to VARIABLE."),
)
@click.option(
    "--position",
    "-p",
    show_default=True,
    default=2,
    type=int,
    help=(
        "Position (1-based) at which you want to insert DIMENSION "
        + "into the already existing dimensions of VARIABLE."
    ),
)
@click.option(
    "--value",
    "-i",
    required=True,
    prompt=True,
    type=float,
    help=(
        "Value under which VARIABLE will be indexed along the new "
        + "DIMENSION. Will be treated as a float value."
    ),
)
@click.option(
    "--new-name",
    "-r",
    help=(
        "If supplied, VARIABLE will be renamed to the NEW-NAME in " + "TARGET."
    ),
)
def main(**kwargs):
    """ Adds a dimension to a variable in a netCDF file.

    Pulls VARIABLE out of SOURCE and writes it to TARGET indexed by VALUE along
    DIMENSION in addition to the dimensions originally present for it in
    SOURCE.
    Defaults to reading from STDIN and writing to STDOUT, which are specified
    with by supplying '-' as SOURCE and TARGET respectively.
    """
    return add_dimension(**kwargs)


if __name__ == "__main__":
    main()
