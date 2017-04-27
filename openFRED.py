from contextlib import contextmanager
from datetime import datetime as dt, timedelta as td, timezone as tz
from functools import reduce
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

def import_nc_file(filepath, classes, session):
    click.echo("Importing: {}".format(filepath))
    ds = nc.Dataset(filepath)
    vs = list(it.takewhile(lambda x: x not in ['lat', 'altitude'],
                           ds.variables.keys()))
    # TODO: The `if` below weeds out variables which are constant with respect
    #       to time. Figure out how to handle and correctly save these.
    if len(vs) > 2: return
    altitude = None
    if (len(vs) == 2) and ('height' in vs[1]):
        # Will look like 'height_XYZm'.
        # Cut off prefix and suffix, convert, extract, and remove.
        altitude = int(vs[1][len('height_'):][:-1])
        vs = vs[:-1]
    # Generate a dictionary of grid points. This is for performance reasons.
    # Equality testing on WKT elements is really slow and we need to know
    # whether a grid point is already in the database or whether we have to
    # construct a new one.
    locations = session.query(classes["Location"]).all()
    grid = {(s.x, s.y): l for l in locations
                          for s in [to_shape(l.point)]}
    for name in vs:
        ncv = ds[name]
        dbv = session.query(classes['Variable']).get(name) or \
              classes['Variable'](name=name,
                                  standard_name=getattr(ncv, "standard_name",
                                                        None),
                                  description=ncv.long_name)
        session.add(dbv)
        dims = ncv.dimensions
        total_size = reduce(lambda x, y: x*y,
                            [ds[d].size for d in dims])
        d_index = {d: i for i, d in enumerate(dims)}
        def value_of(variable, indices, *dimensions):
            """ Returns the value of `variable` at `index` for `dimensions`.

            Uses `d_index` to determine which sub indices of `indices` (which
            should be a tuple of indices) belongs to `dimensions` and then uses
            those sub indices to return the appropriate value of `variable` at
            (the matching sub indices of) `indices`.
            If no `dimensions` are given, the `variable` name is used as the
            sole entry in `dimensions`.
            """
            dimensions = [variable] if not dimensions else dimensions
            return ds[variable][tuple(indices[d_index[d]] for d in dimensions)]
        def getset(d, k, v):
            """ Returns `d.get(k, v)` and stores `v` at `k` `if not k in d`.
            """
            d[k] = d.get(k, v)
            return d[k]
        epoch = dt(2002, 2, 1, tzinfo=tz.utc)
        with click.progressbar(length=total_size,
                               label="     Var.: " + name) as bar:
            for indexes, count in zip(
                    it.product(*(range(ds[d].size) for d in dims)),
                    it.count()):
                b = value_of('time_bnds', indexes, 'time')
                if 'altitude' in dims:
                    altitude = value_of('altitude', indexes)
                ts = (epoch + td(seconds=b[0]), epoch + td(seconds=b[1]))
                xy = tuple(value_of(variable, indexes, 'rlat', 'rlon')
                           for variable in ('lon', 'lat'))
                wkt = WKT('POINT ({} {})'.format(*xy), srid=4326)
                location = getset(grid, xy, classes['Location'](point=wkt))
                v = classes['Value'](
                        altitude=(float(altitude)
                                  if altitude is not None else None),
                        v=float(ncv[indexes]),
                        timestamp=classes['Timestamp'](start=ts[0], stop=ts[1]),
                        location=location,
                        variable=dbv)
                session.add(v)
                if count % 1000 == 0:
                    session.commit()
                    bar.update(1000)
    click.echo("     Done: {}\n".format(filepath))


### The commmands comprising the command line interface.
@click.group()
def openFRED():
    """ The openFRED command line toolbox.

    Contains useful commands to work with openFRED related/supplied data.
    """
    pass

@click.group()
def db():
    """ Commands to work with openFRED databases.
    """
    pass
openFRED.add_command(db)

@db.command()
def initialize():
    """ Initialize a database for openFRED data.

    Connect to the database specified in the `[openFRED]` section of oemof's
    `config.ini` and set the database up to hold openFRED data.
    This means that the configured schema is created if it doesn't already
    exists. The same holds for the tables necessary to store openFRED data
    inside the schema.
    """
    section = 'openFRED'
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
@click.argument('paths', type=click.Path(exists=True), metavar='PATHS',
                nargs=-1)
def import_(paths):
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

    section = 'openFRED'
    schema = oemof.db.config.get(section, 'schema')
    engine = oemof.db.engine(section)

    classes = mapped_classes(schema)

    with db_session(engine) as session:
        for f in filepaths:
            import_nc_file(f, classes, session)
        session.commit()

if __name__ == '__main__':
    openFRED()

