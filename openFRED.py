# TODO: Handle Timestamps and Metadata correctly.
#   * Names:
#     - "standard_name" is not allowed to contain whitespace
#     - not all variables have a "standard_name"
#   * Units: uses terms from the UDUNITS package
#     - has a Python binding?
#     - has what relation to GNU Units?

# TODO: Parallelize importing variables.
#       This shouldn't be too hard. While the session is currently created only
#       once and passed around a bunch of times, it doesn't have to be
#       that way. The session is only used when importing a specific
#       variable anyway, so one could simply factor out session creation
#       into a function and call that in the process importing each
#       variable. Also, this would force factoring the code importing a
#       single variable out of the function importing a single file,
#       which would finally lead to code being a bit less monolithic
#       again.

from contextlib import contextmanager
from collections.abc import MutableMapping as MM
from datetime import datetime as dt, timedelta as td, timezone as tz
from functools import reduce
from operator import mul as multiply
import itertools as it
import os

from alembic.migration import MigrationContext
from alembic.operations import Operations
from geoalchemy2 import WKTElement as WKT, types as geotypes
from numpy.ma import masked
from pandas import to_datetime
from sqlalchemy import (
    create_engine,
    BigInteger as BI,
    Column as C,
    DateTime as DT,
    Float,
    ForeignKey as FK,
    Integer as Int,
    Interval,
    JSON,
    MetaData,
    String as Str,
    Table,
    Text,
    UniqueConstraint as UC,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound as MRF
from sqlalchemy.inspection import inspect
from sqlalchemy.schema import AddConstraint, CheckConstraint, CreateSchema
import click
import xarray as xr

import oemof.db


class Keychanger(MM):
    """ A mapping that applies a function to the keys before lookup.
    """

    def __init__(self, data, transformer):
        self.data = data
        self.transform = transformer

    def __getitem__(self, key):
        return self.data.__getitem__(self.transform(key))

    def __setitem__(self, key, value):
        return self.data.__setitem__(self.transform(key), value)

    def __delitem__(self, key):
        return self.data.__delitem__(self.transform(key))

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()


class DimensionCache:
    """ Caches dimension values to speed up access later on.

    Iterates through the dimensions of the given variable and gets the
    corresponding objects from the database or creates them if they don't
    exist. This enables faster lookup, as the database doesn't have to be hit
    later on and it also enables talking about dimension values in terms of
    their primary key in the database, which is necessary for fast insertion of
    `Value`s into the database.
    """

    def __init__(self, ds, v, session, classes, time):
        d_index = {d: i for i, d in enumerate(ds[v].dims)}
        self.session = session
        height = (
            [
                float(ds[v])
                for v in ds.variables.keys()
                if v.startswith("height")
            ]
            + [0.0]
        )[0]

        click.echo("  Caching dimensions.")

        def timespan(index):
            assert time is not None, (
                "Trying to get timespan intervalls for a variable which seems"
                "\nconstant wrt. to time."
            )
            assert time in ["time_bnds", "time"], (
                "Trying to get timespan intevalls using an invalid argument"
                "value for `time`\n."
                'Valid argument values are `"time"` and `"time_bnds"`.\n\n'
                "  Got: {}"
            ).format("`None`" if time is None else time)
            bounds = (
                [
                    [to_datetime(bnd) for bnd in bnds]
                    for bnds in ds["time_bnds"][index].values
                ]
                if time == "time_bnds"
                else [
                    [to_datetime(moment)] * 2
                    for moment in ds["time"][index].values
                ]
            )
            intervals = [t[1] - t[0] for t in bounds]
            assert (
                len(set(intervals)) == 1
            ), "Nonuniform timeseries resolutions are not supported yet."
            return {
                "start": bounds[0][0],
                "stop": bounds[-1][1],
                "resolution": intervals[0],
                "segments": bounds,
            }

        self.timespans = Keychanger(
            data=(
                list(
                    self.cache(
                        [slice(None)],
                        "        Time:",
                        classes["Timespan"],
                        timespan,
                        idonly=True,
                        exclude={"segments"},
                    )
                )
                if "time" in ds.variables
                else [None]
            ),
            transformer=lambda indexes: 0,
        )

        def point(key):
            wkt = WKT(
                "POINT ({} {})".format(
                    ds["lon"].values[key], ds["lat"].values[key]
                ),
                srid=4326,
            )
            return {"point": wkt}

        location_index = list(
            it.product(
                *(
                    range(len(ds.variables.get(d, ())))
                    for d in ("rlat", "rlon")
                )
            )
        )
        self.locations = Keychanger(
            data=dict(
                zip(
                    location_index,
                    list(
                        self.cache(
                            location_index,
                            "    Location:",
                            classes["Location"],
                            point,
                            idonly=True,
                        )
                    ),
                )
            ),
            transformer=lambda indexes: tuple(
                indexes[d_index[d]] for d in ("rlat", "rlon")
            ),
        )
        self.heights = Keychanger(
            data=[height],
            transformer=lambda ixs: (
                0 if not d_index.get("height") else ixs[d_index["height"]]
            ),
        )

    def cache(self, indexes, label, cls, kwargs, idonly=False, exclude=set()):
        with click.progressbar(indexes, label=label) as bar:
            for index in bar:
                d = kwargs(index)
                o = self.session.query(cls).filter_by(
                    **{k: d[k] for k in d if k not in exclude}
                ).one_or_none() or cls(**d)
                self.session.add(o)
                self.session.flush()
                yield (o.id if idonly else o)


### Auxiliary functions needed by more than one command.


@contextmanager
def db_session(engine):
    """ Provide a session context to communicate with the database.
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def mapped_classes(metadata):
    """ Returns classes mapped to the openFRED database via SQLAlchemy.

    The classes are dynamically created and stored in a dictionary keyed by
    class names. The dictionary also contains the special entry `__Base__`,
    which an SQLAlchemy `declarative_base` instance used as the base class from
    which all mapped classes inherit.
    """

    Base = declarative_base(metadata=metadata)
    classes = {"__Base__": Base}

    def map(name, registry, namespace):
        namespace["__tablename__"] = "openfred_" + name.lower()
        namespace["__table_args__"] = namespace.get("__table_args__", ()) + (
            {"keep_existing": True},
        )
        if namespace["__tablename__"][-1] != "s":
            namespace["__tablename__"] += "s"
        registry[name] = type(name, (registry["__Base__"],), namespace)

    map(
        "Timespan",
        classes,
        {
            "id": C(BI, primary_key=True),
            "start": C(DT),
            "stop": C(DT),
            "resolution": C(Interval),
            "segments": C(ARRAY(DT, dimensions=2)),
            "__table_args__": (UC("start", "stop", "resolution"),),
        },
    )
    map(
        "Location",
        classes,
        {
            "id": C(BI, primary_key=True),
            "point": C(
                geotypes.Geometry(geometry_type="POINT", srid=4326),
                unique=True,
            ),
        },
    )
    # TODO: Handle units.
    class Variable(Base):
        __table_args__ = ({"keep_existing": True},)
        __tablename__ = "openfred_variables"
        id = C(BI, primary_key=True)
        name = C(Str(255), nullable=False, unique=True)
        # TODO: Figure out whether and where this is in the '.nc' files.
        type = C(Str(37))
        netcdf_attributes = C(JSON)
        description = C(Text)
        standard_name = C(Str(255))
        __mapper_args_ = {
            "polymorphic_identity": "variable",
            "polymorphic_on": type,
        }

    classes["Variable"] = Variable

    class Flags(Variable):
        __table_args__ = ({"keep_existing": True},)
        __tablename__ = "openfred_flags"
        id = C(BI, FK(Variable.id), primary_key=True)
        flag_ks = C(ARRAY(Int), nullable=False)
        flag_vs = C(ARRAY(Str(37)), nullable=False)
        __mapper_args_ = {"polymorphic_identity": "flags"}

        @property
        def flag(self, key):
            flags = dict(zip(self.flag_ks, self.flag_vs))
            return flags[key]

    classes["Flags"] = Flags

    class Series(Base):
        __tablename__ = "openfred_series"
        __table_args__ = (
            UC("height", "location_id", "timespan_id", "variable_id"),
            {"keep_existing": True},
        )
        id = C(BI, primary_key=True)
        values = C(ARRAY(Float), nullable=False)
        height = C(Float)
        timespan_id = C(BI, FK(classes["Timespan"].id), nullable=False)
        location_id = C(BI, FK(classes["Location"].id), nullable=False)
        variable_id = C(BI, FK(classes["Variable"].id), nullable=False)
        timespan = relationship(classes["Timespan"], backref="series")
        location = relationship(classes["Location"], backref="series")
        variable = relationship(classes["Variable"], backref="series")

    classes["Series"] = Series

    return classes


# TODO: The two functions below are prime examples of stuff that one can and
#       should write tests for.
def maybe(f, o):
    return None if o is None else f(o)


def chunk(iterable, n):
    """ Divide `iterable` into chunks of size `n` without padding.
    """
    xs = iter(iterable)
    return (it.chain((x,), it.islice(xs, n - 1)) for x in xs)


def import_nc_file(filepath, variables, schema, url):
    click.echo("Importing: {}".format(filepath))

    ds = xr.open_dataset(filepath, decode_cf=False)
    time = (
        ds["time"].attrs["bounds"]
        if "time" in ds.coords
        and "units" in ds[ds["time"].attrs["bounds"]].attrs
        else "time"
    )

    ds = xr.decode_cf(ds)

    vs = [v for v in variables if v in ds.variables.keys()]

    for name in vs:
        import_variable(name, ds, time, schema, url)


def import_variable(name, dataset, schema, url):

    classes = mapped_classes(MetaData(schema=schema))

    with db_session(create_engine(url)) as session:
        ncv = ds[name]
        if time != "time" and not "time:" in ncv.attrs.get("cell_methods", ""):
            time = None
        if hasattr(ncv, "flag_values"):
            variable = classes["Flags"]
            kws = {
                "flag_ks": [int(v) for v in ncv.flag_values],
                "flag_vs": ncv.flag_meanings,
            }
        else:
            variable = classes["Variable"]
            kws = {}
        if not time:
            assert not "time" in ncv.coords or (
                len(ds["time"]) == 1
                and not "time:" in ncv.attrs.get("cell_methods", "")
            ), (
                "Looks like we found a variable which is neither "
                "instantaneous nor "
                "constant wrt. time but also either doesn't have a "
                "`cell_methods` attribute\n"
                "or it has one that doesn't have `time` entry.\n\n"
                "  variable name: {}\n"
                "  cell_methods : {}"
            ).format(
                ncv.name,
                "`None`"
                if ncv.attrs.get("cell_methods") is None
                else ncv.attrs.get("cell_methods"),
            )
        if time == "time":
            assert not "time:" in ncv.attrs.get("cell_methods", ""), (
                "Looks like we found a variable which is instantaneous or "
                "constant wrt. time\n"
                "but also has a `cell_methods` attribute with a `time` entry."
                "\nThese things aren't compatible.\n\n"
                "  variable name: {}\n"
                "  cell_methods : {}"
            ).format(ncv.name, ncv.attrs.get("cell_methods"))
        # TODO: Assert that stuff in the file matches what's already there, if
        #       the variable already exists in the database.
        dbv = session.query(variable).filter_by(
            name=name
        ).one_or_none() or variable(
            name=name,
            standard_name=getattr(ncv, "standard_name", None),
            description=ncv.long_name,
            netcdf_attributes=ncv.attrs,
            **kws
        )
        session.add(dbv)
        session.commit()
        dbvid = dbv.id
        session.expunge(dbv)
        dcache = DimensionCache(ds, name, session, classes, time)
        session.commit()
        click.echo("  Importing variable(s).")
        length = reduce(
            multiply, (ds[d].size for d in ncv.dims if d != "time")
        )
        tuples = it.product(
            *(
                range(ds[d].size) if d != "time" else [slice(None)]
                for d in ncv.dims
            )
        )
        with click.progressbar(
            length=length, label="{: >{}}:".format(name, 4 + len("location"))
        ) as bar:
            mappings = (
                dict(
                    height=maybe(float, dcache.heights[indexes]),
                    values=(round(float(v), 3) for v in ncv.values[indexes]),
                    timespan_id=dcache.timespans[indexes],
                    location_id=dcache.locations[indexes],
                    variable_id=dbvid,
                )
                for indexes in tuples
            )
            mapper = classes["Series"]
            for c in chunk(mappings, 1):
                l = list(c)
                session.bulk_insert_mappings(mapper, l)
                bar.update(len(l))
        click.echo("  Committing.")
        session.commit()
    click.echo("     Done: {}\n".format(filepath))


### The commmands comprising the command line interface.
@click.group()
@click.pass_context
def openFRED(context):
    """ The openFRED command line toolbox.

    Contains useful commands to work with openFRED related/supplied data.
    """
    context.obj = {}


@click.group()
@click.pass_context
@click.option(
    "--configuration-file",
    "-c",
    type=click.Path(exists=True),
    help=(
        "Specifies an alternative configuration file " + "used by `oemof.db`."
    ),
)
@click.option(
    "--section",
    "-s",
    default="openFRED",
    show_default=True,
    help=(
        "The section in `oemof.db`'s configuration file from "
        + "which database parameters should be read."
    ),
)
def db(context, configuration_file, section):
    """ Commands to work with openFRED databases.
    """
    if configuration_file is not None:
        oemof.db.load_config(configuration_file)
    context.obj["db"] = {"cfg": configuration_file, "section": section}


@db.command()
@click.pass_context
@click.option(
    "--drop",
    "-d",
    type=click.Choice(["schema", "tables"]),
    help=("Drop the schema/tables prior to initializing the " + "database."),
)
def setup(context, drop):
    """ Initialize a database for openFRED data.

    Connect to the database specified in the `[openFRED]` section of oemof's
    configuration file and set the database up to hold openFRED data.
    This means that the configured schema is created if it doesn't already
    exists. The same holds for the tables necessary to store openFRED data
    inside the schema.
    """
    section = context.obj["db"]["section"]
    schema = oemof.db.config.get(section, "schema")
    engine = oemof.db.engine(section)
    inspector = inspect(engine)
    metadata = MetaData(schema=schema, bind=engine, reflect=(not drop))
    classes = mapped_classes(metadata)

    if drop == "schema":
        with engine.connect() as connection:
            connection.execute(
                "DROP SCHEMA IF EXISTS {} CASCADE".format(schema)
            )
    elif drop == "tables":
        classes["__Base__"].metadata.drop_all(engine)
    if not schema in inspector.get_schema_names():
        engine.execute(CreateSchema(schema))

    with engine.connect() as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology;")
    classes["__Base__"].metadata.create_all(engine)

    with db_session(engine) as session:
        timespan = classes["Timespan"]
        try:
            ts = (
                session.query(timespan)
                .filter_by(start=None, stop=None)
                .one_or_none()
            )
        except MRF as e:
            click.echo(
                "Multiple timestamps found which have no `start` "
                "and/or `stop` values.\nAborting."
            )
        ts = ts or classes["Timespan"]()
        session.add(ts)
        session.flush()

        context = MigrationContext.configure(session.connection())
        ops = Operations(context)
        ops.alter_column(
            table_name=str(classes["Series"].__table__.name),
            column_name="timespan_id",
            server_default=str(ts.id),
            schema=schema,
        )

        constraint_name = "singular_null_timestamp_constraint"
        if not [
            c
            for c in timespan.__table__.constraints
            if c.name == constraint_name
        ]:
            constraint = CheckConstraint(
                "(id = {}) OR ".format(ts.id)
                + "(start IS NOT NULL AND stop IS NOT NULL)",
                name=constraint_name,
            )
            timespan.__table__.append_constraint(constraint)
            session.execute(AddConstraint(constraint))

    return classes


@db.command("import")
@click.pass_context
@click.argument(
    "paths", type=click.Path(exists=True), metavar="PATHS", nargs=-1
)
@click.option(
    "--variables",
    "-V",
    metavar="VARIABLES",
    multiple=True,
    help=(
        "Specify the variable to import. Can be specified "
        "multiple times. If not specified, nothing is imported."
    ),
)
def import_(context, paths, variables):
    """ Import an openFRED dataset.

    For each path found in PATHS, imports the NetCDF files found under path.
    If path is a directory, it is traversed (recursively) and each NetCDF file,
    i.e. each file with the extension '.nc', found is imported.
    If path points to a file, it is imported as is.
    """
    filepaths = []
    for p in paths:
        if os.path.isfile(p):
            filepaths.append(p)
        elif os.path.isdir(p):
            for (path, _, files) in os.walk(p):
                for f in files:
                    if f[-3:] == ".nc":
                        filepaths.append(os.path.join(path, f))

    section = context.obj["db"]["section"]
    schema = oemof.db.config.get(section, "schema")
    url = oemof.db.url(section)

    for f in sorted(filepaths):
        import_nc_file(f, variables, schema, url)


if __name__ == "__main__":
    openFRED.add_command(db)
    openFRED()
