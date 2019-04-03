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
from math import floor
from operator import mul as multiply
from time import sleep
from traceback import TracebackException
import itertools as it
import multiprocessing as mp
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
from sqlalchemy.exc import SQLAlchemyError
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


class Dimensions:
    """ Caches dimension values to speed up access later on.

    Iterates through the dimensions of the given variable and gets the
    corresponding objects from the database or creates them if they don't
    exist. This enables faster lookup, as the database doesn't have to be hit
    later on and it also enables talking about dimension values in terms of
    their primary key in the database, which is necessary for fast insertion of
    `Value`s into the database.
    """

    def __init__(self, dataset, variable, session, classes, time):
        self.dataset = dataset
        self.variable = variable
        self.session = session
        self.classes = classes
        self.time = time
        self.index = {d: i for i, d in enumerate(dataset[variable].dims)}
        self.height = (
            [
                float(dataset[variable])
                for variable in dataset.variables.keys()
                if variable.startswith("height")
            ]
            + [0.0]
        )[0]

    def cache(self):
        def timespan(index):
            assert self.time is not None, (
                "Trying to get timespan intervalls for a variable which seems"
                "\nconstant wrt. to time."
            )
            assert self.time in ["time_bnds", "time"], (
                "Trying to get timespan intevalls using an invalid argument"
                "value for `time`\n."
                'Valid argument values are `"time"` and `"time_bnds"`.\n\n'
                "  Got: {}"
            ).format("`None`" if self.time is None else self.time)
            bounds = (
                [
                    [to_datetime(bnd) for bnd in bnds]
                    for bnds in self.dataset["time_bnds"][index].values
                ]
                if self.time == "time_bnds"
                else [
                    [to_datetime(moment)] * 2
                    for moment in self.dataset["time"][index].values
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

        while True:
            try:
                yield "Caching time dimension."
                self.timespans = Keychanger(
                    data=(
                        list(
                            self._cache(
                                [slice(None)],
                                self.classes["Timespan"],
                                timespan,
                                idonly=True,
                                exclude={"segments"},
                            )
                        )
                        if "time" in self.dataset.variables
                        else [None]
                    ),
                    transformer=lambda indexes: 0,
                )
            except SQLAlchemyError as e:
                yield "Caching time failed. Rolling back. Sleeping 7s."
                self.session.rollback()
                sleep(7)
            else:
                yield "Time cached."
                self.session.commit()
                break

        def point(key):
            wkt = WKT(
                "POINT ({} {})".format(
                    self.dataset["lon"].values[key],
                    self.dataset["lat"].values[key],
                ),
                srid=4326,
            )
            return {"point": wkt}

        location_index = list(
            it.product(
                *(
                    range(len(self.dataset.variables.get(d, ())))
                    for d in ("rlat", "rlon")
                )
            )
        )
        while True:
            try:
                yield "Caching locations."
                self.locations = Keychanger(
                    data=dict(
                        zip(
                            location_index,
                            list(
                                self._cache(
                                    location_index,
                                    self.classes["Location"],
                                    point,
                                    idonly=True,
                                )
                            ),
                        )
                    ),
                    transformer=lambda indexes: tuple(
                        indexes[self.index[d]] for d in ("rlat", "rlon")
                    ),
                )
            except SQLAlchemyError as e:
                yield "Caching locations failed. Rolling back. Sleeping 7s."
                self.session.rollback()
                sleep(7)
            else:
                yield "Locations cached."
                self.session.commit()
                break

        self.heights = Keychanger(
            data=[self.height],
            transformer=lambda ixs: (
                0
                if not self.index.get("height")
                else ixs[self.index["height"]]
            ),
        )

    def _cache(self, indexes, cls, kwargs, idonly=False, exclude=set()):
        for index in indexes:
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


def maybe(f, o):
    return None if o is None else f(o)


def chunks(iterable, n):
    """ Divide `iterable` into chunks of size `n` without padding.
    """
    xs = iter(iterable)
    return (it.chain((x,), it.islice(xs, n - 1)) for x in xs)


def import_nc_file(filepath, variables):

    # TODO: Specify whether a variable is constant wrt. a dimension via the
    #       command line.
    # It seems that the whole "cell_methods" and "units" heuristic doesn't
    # really work. So we're back to being simple again. If there's a
    # "cell_methods" attribute containing "time", the value is aggregatet,
    # otherwise it's istantaneous. Being constant wrt. time has to be specified
    # manually.

    dataset = xr.open_dataset(filepath, decode_cf=False)
    if (
        "time" in dataset.variables
        and not "units" in dataset[dataset["time"].attrs["bounds"]].attrs
    ):
        dataset["time_bnds"].attrs["units"] = dataset["time"].units
    dataset = xr.decode_cf(dataset)

    vs = [v for v in variables if v in dataset.variables.keys()]

    return [
        {
            "name": v,
            "dataset": dataset,
            "time": dataset["time"].attrs["bounds"]
            if "time:" in dataset[v].attrs.get("cell_methods", "")
            else "time",
        }
        for v in vs
    ]


def import_variable(dataset, name, schema, time, url):

    classes = mapped_classes(MetaData(schema=schema))

    with db_session(create_engine(url)) as session:
        ncv = dataset[name]
        if hasattr(ncv, "flag_values"):
            variable = classes["Flags"]
            kws = {
                "flag_ks": [int(v) for v in ncv.flag_values],
                "flag_vs": ncv.flag_meanings,
            }
        else:
            variable = classes["Variable"]
            kws = {}
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
        dimensions = Dimensions(dataset, name, session, classes, time)
        for message in dimensions.cache():
            yield message
        session.commit()
        yield "Importing variable(s)."
        tuples = it.product(
            *(
                range(dataset[d].size) if d != "time" else [slice(None)]
                for d in ncv.dims
            )
        )
        mappings = (
            dict(
                height=maybe(float, dimensions.heights[indexes]),
                values=(round(float(v), 3) for v in ncv.values[indexes]),
                timespan_id=dimensions.timespans[indexes],
                location_id=dimensions.locations[indexes],
                variable_id=dbvid,
            )
            for indexes in tuples
            # if ncv[indexes] is not masked
        )
        mapper = classes["Series"]
        for chunk in chunks(mappings, 74):
            session.bulk_insert_mappings(mapper, chunk)
        yield "Committing."
    yield "Done."


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
        "Specifies an alternative configuration file used by `oemof.db`."
    ),
)
@click.option(
    "--section",
    "-s",
    default="openFRED",
    show_default=True,
    help=(
        "The section in `oemof.db`'s configuration file from "
        "which database parameters should be read."
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
    help=("Drop the schema/tables prior to initializing the database."),
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


def monitor(paths):
    files = {
        os.path.abspath(os.path.join(directory, filename))
        for path in paths
        for (directory, _, files) in os.walk(path)
        for filename in files
        if filename[-3:] == ".nc"
    }
    return files


def message(job, message, time=None):
    # [:-7] strips milliseconds from timestamps.
    if not time:
        time = dt.now()
    return "{}, {}: {}".format(str(time)[:-7], job, message)


def wrap_process(job, messages, function, arguments):
    result = {"started": dt.now()}
    try:
        messages.put(
            message("{}, {}".format(*job), "Started.", result["started"])
        )
        for notification in function(**arguments):
            messages.put(message("{}, {}".format(*job), notification))
    except Exception as e:
        messages.put(
            message(
                "{}, {}".format(*job),
                "\n  "
                "  ".join(TracebackException.from_exception(e).format()),
            )
        )
        messages.put(message("{}, {}".format(*job), "Failed."))
        result["error"] = e
    finally:
        result["finished"] = dt.now()
        duration = result["finished"] - result["started"]
        hours = floor(duration.seconds / 3600)
        minutes = floor(duration.seconds / 60) - hours * 60
        seconds = duration.total_seconds() - hours * 3600 - minutes * 60
        messages.put(
            message(
                "{}, {}".format(*job),
                "Finished in {}{}{}".format(
                    "{}h, ".format(hours) if hours else "",
                    "{}m and ".format(minutes) if minutes or hours else "",
                    "{:.3f}s.".format(seconds),
                ),
                result["finished"],
            )
        )


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
@click.option(
    "--jobs",
    "-j",
    metavar="JOBS",
    default=1,
    show_default=True,
    help=(
        "The number parallel processes to start, in order to handle import jobs."
    ),
)
def import_(context, jobs, paths, variables):
    """ Import an openFRED dataset.

    For each path found in PATHS, imports the NetCDF files found under path.
    If path is a directory, it is traversed (recursively) and each NetCDF file,
    i.e. each file with the extension '.nc', found is imported. These directories
    are also continuously monitored for new files, which are imported too.
    If path points to a file, it is imported as is.
    """
    filepaths = {
        os.path.abspath(path) for path in paths if os.path.isfile(path)
    }

    section = context.obj["db"]["section"]
    schema = oemof.db.config.get(section, "schema")
    url = oemof.db.url(section)

    seen = set()
    manager = mp.Manager()
    messages = manager.Queue()
    pool = mp.Pool(jobs, maxtasksperchild=1)
    results = {"done": {}, "pending": {}}
    while True:
        filepaths.update(monitor(paths).difference(seen))
        results["pending"].update(
            {
                job: pool.apply_async(
                    wrap_process,
                    kwds={
                        "job": job,
                        "messages": messages,
                        "function": import_variable,
                        "arguments": dict(
                            arguments, **{"schema": schema, "url": url}
                        ),
                    },
                )
                for filepath in filepaths
                if not messages.put(
                    message(
                        "Main Process ({})".format(os.getpid()),
                        "Collecting {}.".format(filepath),
                    )
                )
                for arguments in import_nc_file(filepath, variables)
                for job in [(filepath, arguments["name"])]
            }
        )
        seen.update(filepaths)
        filepaths.clear()

        if not results["pending"] and messages.empty():
            break

        move = [
            (job, result)
            for job, result in results["pending"].items()
            if result.ready()
        ]
        for job, result in move:
            del results["pending"][job]
            results["done"][job] = result.get()
        while not messages.empty():
            click.echo(messages.get_nowait())
        sleep(1)


if __name__ == "__main__":
    openFRED.add_command(db)
    openFRED()
