from contextlib import contextmanager
from collections.abc import MutableMapping as MM
from datetime import datetime as dt, timedelta as td, timezone as tz
from functools import reduce
from operator import mul as multiply
import os
import itertools as it

from geoalchemy2 import WKTElement as WKT, types as geotypes
from geoalchemy2.shape import to_shape
from sqlalchemy import (Column as C, DateTime as DT, Float, ForeignKey as FK,
                        Integer as Int, MetaData, String as Str, Table, Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.inspection import inspect
from sqlalchemy.schema import CreateSchema
import click
import netCDF4 as nc

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
    def __init__(self, ds, v, session, classes):
        d_index = {d: i for i, d in enumerate(ds[v].dimensions)}
        self.session = session
        altitude = None
        height = list(filter(
            lambda v: v.startswith("height_") and v[-1] == "m",
            ds.variables.keys()))
        if height:
            # Will look like 'height_XYZm'.
            # Cut off prefix and suffix, convert, extract, and remove.
            altitude = int(height[0][len('height_'):][:-1])

        click.echo("  Caching dimensions.")
        epoch = dt(2002, 2, 1, tzinfo=tz.utc)

        def timestamp(index):
            bounds = [epoch + td(seconds=s) for s in ds['time_bnds'][index]]
            return {"start": bounds[0], "stop": bounds[1]}

        self.timestamps = Keychanger(
            data=list(self.cache(list(range(len(ds.variables.get('time', ())))),
                                 "        Time:",
                                 classes['Timestamp'],
                                 timestamp)),
            transformer=lambda indexes: indexes[d_index['time']])

        def point(key):
            wkt = WKT('POINT ({} {})'.format(ds['lon'][key], ds['lat'][key]),
                      srid=4326)
            return {"point": wkt}

        location_index = list(it.product(*(range(len(ds.variables.get(d, ())))
                                           for d in ('rlat', 'rlon'))))
        self.locations = Keychanger(
            data=dict(zip(location_index,
                          list(self.cache(location_index, "    Location:",
                               classes['Location'],
                               point)))),
            transformer=lambda indexes: tuple(indexes[d_index[d]]
                                              for d in ('rlat', 'rlon')))
        self.altitudes = Keychanger(
            data=ds.variables.get("altitude", [altitude]),
            transformer=lambda ixs: (0 if not d_index.get('altitude')
                                       else ixs[d_index['altitude']]))

    def cache(self, indexes, label, cls, kwargs):
        with click.progressbar(indexes, label=label) as bar:
            for index in bar:
                d = kwargs(index)
                o = (self.session.query(cls).filter_by(**d).one_or_none() or
                     cls(**d))
                self.session.add(o)
                yield(o)
            self.session.flush()

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

def mapped_classes(schema):
    """ Returns classes mapped to the openFRED database via SQLAlchemy.

    The classes are dynamically created and stored in a dictionary keyed by
    class names. The dictionary also contains the special entry `__Base__`,
    which an SQLAlchemy `declarative_base` instance used as the base class from
    which all mapped classes inherit.
    """

    Base = declarative_base(metadata=MetaData(schema=schema))

    def map(name, registry, namespace):
        namespace["__tablename__"] = "openfred_" + name.lower()
        if namespace["__tablename__"][-1] != 's':
            namespace["__tablename__"] += 's'
        registry[name] = type(name, (Base,), namespace)

    classes = {"__Base__": Base}
    map("Timestamp", classes, {
            "id": C(Int, primary_key=True),
            "start": C(DT, nullable=False),
            "stop": C(DT, nullable=False)})
    map("Location", classes, {
            "id": C(Int, primary_key=True),
            "point": C(geotypes.Geometry(geometry_type='POINT', srid=4326),
                       unique=True)})
    # TODO: Handle units.
    map("Variable", classes, {
            "name": C(Str(255), primary_key=True),
            # TODO: Figure out whether and where this is in the '.nc' files.
            "aggregation": C(Str(255)),
            "description": C(Text),
            "standard_name": C(Str(255))})
    map("Value", classes, {
            "id": C(Int, primary_key=True),
            "altitude": C(Float),
            "v": C(Float, nullable=False),
            "timestamp_id": C(Int, FK(classes["Timestamp"].id)),
            "timestamp": relationship(classes["Timestamp"], backref='values'),
            "location_id": C(Int, FK(classes["Location"].id)),
            "location": relationship(classes["Location"], backref='values'),
            "variable_id": C(Str(255), FK(classes["Variable"].name)),
            "variable": relationship(classes["Variable"], backref='values')})

    return classes

def maybe(f, o):
    return (None if o is None else f(o))

def import_nc_file(filepath, classes, session):
    click.echo("Importing: {}".format(filepath))
    ds = nc.Dataset(filepath)
    vs = list(it.takewhile(lambda x: x not in ['lat', 'altitude'],
                           ds.variables.keys()))
    # TODO: The `if` below weeds out variables which are constant with respect
    #       to time. Figure out how to handle and correctly save these.
    if len(vs) > 2: return
    for name in vs:
        ncv = ds[name]
        dbv = session.query(classes['Variable']).get(name) or \
              classes['Variable'](name=name,
                                  standard_name=getattr(ncv, "standard_name",
                                                        None),
                                  description=ncv.long_name)
        session.add(dbv)
        dcache = DimensionCache(ds, name, session, classes)
        click.echo("  Importing variable(s).")
        length = reduce(multiply, (ds[d].size for d in ncv.dimensions))
        with click.progressbar(length=length,
                               label="{: >{}}:".format(
                                   name, 4+len("location"))) as bar:
            ms = []
            for indexes, count in zip(
                    it.product(*(range(ds[d].size) for d in ncv.dimensions)),
                    it.count(1)):
                ms.append(dict(
                        altitude=maybe(float, dcache.altitudes[indexes]),
                        v=float(ncv[indexes]),
                        timestamp_id=dcache.timestamps[indexes].id,
                        location_id=dcache.locations[indexes].id,
                        variable_id=dbv.name))
                if count % 1000 == 0:
                    session.bulk_insert_mappings(classes['Value'], ms)
                    ms = []
                    bar.update(1000)
            session.bulk_insert_mappings(classes['Value'], ms)
    click.echo("     Done: {}\n".format(filepath))


### The commmands comprising the command line interface.
@click.group()
@click.pass_context
def openFRED(context):
    """ The openFRED command line toolbox.

    Contains useful commands to work with openFRED related/supplied data.
    """
    context.obj={}

@click.group()
@click.pass_context
@click.option('--configuration-file', '-c', type=click.Path(exists=True),
              help=('Specifies an alternative configuration file ' +
                    'used by `oemof.db`.'))
@click.option('--section', '-s', default='openFRED', show_default=True,
              help=("The section in `oemof.db`'s configuration file from " +
                    "which database parameters should be read."))
def db(context, configuration_file, section):
    """ Commands to work with openFRED databases.
    """
    if configuration_file is not None:
        oemof.db.load_config(configuration_file)
    context.obj['db'] = {'cfg': configuration_file, 'section': section}

@db.command()
@click.pass_context
def initialize(context):
    """ Initialize a database for openFRED data.

    Connect to the database specified in the `[openFRED]` section of oemof's
    configuration file and set the database up to hold openFRED data.
    This means that the configured schema is created if it doesn't already
    exists. The same holds for the tables necessary to store openFRED data
    inside the schema.
    """
    section = context.obj['db']['section']
    schema = oemof.db.config.get(section, 'schema')
    engine = oemof.db.engine(section)
    inspector = inspect(engine)

    if not schema in inspector.get_schema_names():
        engine.execute(CreateSchema(schema))

    classes = mapped_classes(schema)

    with engine.connect() as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology;")
    classes['__Base__'].metadata.create_all(engine)

    return classes


@db.command("import")
@click.pass_context
@click.argument('paths', type=click.Path(exists=True), metavar='PATHS',
                nargs=-1)
def import_(context, paths):
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
            for (path, dirs, files) in os.walk(p):
                for f in files:
                    if f[-3:] == '.nc':
                        filepaths.append(os.path.join(path, f))

    section = context.obj['db']['section']
    schema = oemof.db.config.get(section, 'schema')
    engine = oemof.db.engine(section)

    classes = mapped_classes(schema)

    with db_session(engine) as session:
        for f in filepaths:
            import_nc_file(f, classes, session)
        session.commit()

if __name__ == '__main__':
    openFRED.add_command(db)
    openFRED()

