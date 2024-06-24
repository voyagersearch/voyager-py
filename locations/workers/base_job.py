# (C) Copyright 2014 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import json
import datetime
import copy
import math
import sys
import decimal
import zmq


class ObjectEncoder(json.JSONEncoder):
    """Support non-native Python types for JSON serialization."""
    def default(self, obj):
        text_chars = ''.join(map(chr, [7, 8, 9, 10, 12, 13, 27] + range(0x20, 0x100)))
        is_binary_string = lambda bytes: bool(bytes.translate(None, text_chars))

        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            # Format date to iso 8601
            try:
                if obj.tzinfo:
                    return obj.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
                else:
                    return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                # For dates before 1900.
                date_str = datetime.datetime.isoformat(obj) + 'Z'
                return date_str
            except Exception:
                return list(obj.timetuple())[0:6]
        elif isinstance(obj, memoryview):
            if not is_binary_string(obj.tobytes()):
                return str(obj)
            else:
                return None

        try:
            os.environ["NLS_LANG"] = ".AL32UTF8"
            import cx_Oracle
            if isinstance(obj, cx_Oracle.LOB):
                if not is_binary_string(cx_Oracle.LOB.read(obj, 1024)):
                    return str(obj)
                else:
                    return None
            elif isinstance(obj, cx_Oracle.CLOB):
                return str(obj)
        except ImportError:
            return


class Job(object):
    def __init__(self, job_file):
        self.job_file = job_file
        self.job = json.load(open(job_file, 'r'))

        self.drvr = None
        self.domains = {}  # For Geodatabase coded value domains
        self._sql_queries = []
        self.db_connection = None
        self.db_cursor = None
        self.db_query = None
        self.zmq_socket = None

        self.__sql_server_connection_str = ''
        self.__field_mapping = []
        self.__new_fields = []
        self.__joins = []
        self.__table_constraints = []
        self.__table_queries = []
        self.__get_domains()
        self.__related_tables = []
        self.__format = None

    def __del__(self):
        """Close open connections, streams, etc. after all references to Job are deleted."""
        try:
            if self.zmq_socket:
                self.zmq_socket.close()
            if self.db_connection:
                self.db_connection.close()
        except TypeError:
            pass

    @property
    def use_coded_value_descriptions(self):
        try:
            return self.job['location']['config']['convert_coded_values']
        except KeyError:
            return None

    @property
    def fields_to_keep(self):
        """List of fields to keep (may include wild card)."""
        try:
            return self.job['location']['config']['fields']['include']
        except KeyError:
            return ['*']

    @property
    def fields_to_skip(self):
        """List of fields to skip (may include a wild card)."""
        try:
            return self.job['location']['config']['fields']['exclude']
        except KeyError:
            return None

    @property
    def field_mapping(self):
        return self.__field_mapping

    @property
    def new_fields(self):
        return self.__new_fields

    @property
    def joins(self):
        return self.__joins

    @property
    def format(self):
        return self.__format

    @property
    def related_tables(self):
        return self.__related_tables

    @property
    def field_types(self):
        if self.drvr == 'Oracle':
            import cx_Oracle
            return {cx_Oracle.STRING: 'fs_',
                    'STRING': 'fs_',
                    cx_Oracle.FIXED_CHAR: 'fs_',
                    'FIXED_CHAR': 'fs_',
                    'NVARCHAR2': 'fs_',
                    cx_Oracle.NUMBER: 'ff_',
                    'NUMBER': 'ff_',
                    cx_Oracle.DATETIME: 'fd_',
                    'DATETIME': 'fd_',
                    cx_Oracle.TIMESTAMP: 'fd_',
                    'TIMESTAMP': 'fd_',
                    cx_Oracle.UNICODE: 'fs_'}
        else:
            import bson
            return {'STRING': 'fs_',
                    'UNICODE': 'fs_',
                    'FIXED_CHAR': 'fs_',
                    'NVARCHAR2': 'fs_',
                    'NUMBER': 'ff_',
                    'DATETIME': 'fd_',
                    'TIMESTAMP': 'fd_',
                    unicode: 'fs_',
                    long: 'fl_',
                    int: 'fl_',
                    float: 'fu_',
                    decimal.Decimal: 'fu_',
                    datetime.datetime: 'fd_',
                    bson.objectid.ObjectId: 'fl_',
                    "Date": "fd_",
                    "Double": "fu_",
                    "Guid": "meta_",
                    "Integer": "fl_",
                    "OID": "fl_",
                    "Single": "ff_",
                    "SmallInteger": "fi_",
                    "String": 'fs_',
                    'int': 'fl_',
                    'int identity': 'fl_',
                    'integer': 'fl_',
                    'smallint': 'fi_',
                    'bigint': 'fl_',
                    'char': 'fs_',
                    'nchar': 'fs_',
                    'nvarchar': 'fs_',
                    'varchar': 'fs_',
                    'numeric': 'fu_',
                    'date': 'fd_',
                    'smalldatetime': 'fd_',
                    'bit': 'fb_',
                    'float': 'ff_',
                    'text': 'fs_',
                    'ntext': 'fs_',
                    'decimal': 'ff_',
                    'esriFieldTypeOID': 'fl_',
                    'esriFieldTypeDate': 'fd_',
                    'esriFieldTypeDouble': 'fu_',
                    'esriFieldTypeString': 'fs_',
                    'esriFieldTypeInteger': 'fl_'}

    @property
    def table_constraints(self):
        return self.__table_constraints

    @property
    def table_queries(self):
        return self.__table_queries

    @property
    def action_type(self):
        """Returns the action type."""
        return 'ADD'

    @property
    def discovery_id(self):
        """Returns the discovery id."""
        return self.job['id']

    @property
    def location_id(self):
        """The location ID"""
        return self.job['location']['id']

    @property
    def generalize_value(self):
        """The value setting for generalizing the geometry.
        0   - do not generalize
        0.5 - generalize but maintain shape enough to depict (default)
        1   - most generalized (i.e. convex hull)
        """
        try:
            return self.job['location']['settings']['geometry']['generalize']
        except KeyError:
            return 0.5

    @property
    def multiprocess(self):
        try:
            if self.job['location']['config']['multiprocessing'] == 'true':
                return True
            else:
                return False
        except KeyError:
            return False

    @property
    def path(self):
        """Catalog path for esri data types."""
        try:
            return self.job['location']['config']['path']
        except KeyError:
            return ''

    @property
    def service_connection(self):
        """ArcGIS Server url Portal url."""
        if 'service_connection' in self.job['location']['config']:
            return self.job['location']['config']['service_connection']
        else:
            return ''

    @property
    def url(self):
        """URL for GDAL/OGR dataset."""
        try:
            return self.job['location']['config']['url']
        except KeyError:
            return ''

    @property
    def include_wkt(self):
        """Include well-known text (wkt) to geo information."""
        try:
            if self.job['location']['config']['wkt'] == 'true':
                return True
            else:
                return False
        except KeyError:
            return False

    @property
    def dynamodb_endpoint_url(self):
        """The DynamoDB endpoint URL"""
        try:
            return self.job['location']['config']['dynamodb']['endpoint_url']
        except KeyError:
            return ''

    @property
    def dynamodb_region(self):
        """The DynamoDB region."""
        try:
            return self.job['location']['config']['dynamodb']['region']
        except KeyError:
            return ''

    @property
    def mongodb_client_info(self):
        """The mongoDB client connection string."""
        try:
            return self.job['location']['config']['mongodb']['client']
        except KeyError:
            return ''

    @property
    def mongodb_database(self):
        try:
            return self.job['location']['config']['mongodb']['database']
        except KeyError:
            return ''

    @property
    def has_gridfs(self):
        try:
            if self.job['location']['config']['mongodb']['gridfs'] == 'true':
                return True
            else:
                return False
        except KeyError:
            return False

    @property
    def sql_driver(self):
        try:
            return self.job['location']['config']['sql']['connection']['driver']
        except KeyError:
            return None

    @property
    def sql_connection_info(self):
        """SQL connection information as key, value pairs."""
        try:
            return self.job['location']['config']['sql']
        except KeyError:
            return None

    @property
    def sql_queries(self):
        """A SQL query"""
        try:
            return self.job['location']['config']['queries']
        except KeyError:
            return None

    @property
    def schema_only(self):
        """Index the schema only."""
        try:
            if self.job['location']['config']['schema_only'] == 'true':
                return True
            else:
                return False
        except KeyError:
            return False

    @property
    def sql_schema(self):
        """Returns the database schema."""
        try:
            return self.job['location']['config']['sql']['connection']['schema']
        except KeyError:
            return None

    @property
    def sql_server_connection_str(self):
       return self.__sql_server_connection_str

    #
    # Public methods
    #
    def connect_to_database(self):
        """Makes an ODBC database connection."""
        #TODO: test "Driver={Microsoft ODBC for Oracle};Server=" + dbInst + ';Uid=' + schema + ';Pwd=' + passwd + ";"
        if self.mongodb_client_info:
            import pymongo
            import bson
            client = pymongo.MongoClient(self.mongodb_client_info)
            self.db_connection = client[self.mongodb_database]
        elif 'dynamodb' in self.job['location']['config']:
            import boto3
            if self.dynamodb_endpoint_url:
                self.dynamodb = boto3.resource('dynamodb', region_name=self.dynamodb_region, endpoint_url=self.dynamodb_endpoint_url)
            else:
                self.dynamodb = boto3.resource('dynamodb', region_name=self.dynamodb_region)
        else:
            self.drvr = self.sql_connection_info['connection']['driver']
            srvr = self.sql_connection_info['connection']['server']
            db = self.sql_connection_info['connection']['database']
            un = self.sql_connection_info['connection']['uid']
            pw = self.sql_connection_info['connection']['password']
            if 'instance' in self.sql_connection_info['connection']:
                inst = self.sql_connection_info['connection']['instance']
            else:
                inst = None

            if self.drvr == 'Oracle':
                import cx_Oracle
                try:
                    self.db_connection = cx_Oracle.connect("{0}/{1}@{2}/{3}".format(un, pw, srvr, db))
                except Exception:
                    port = self.sql_connection_info['connection']['port']
                    sid = self.sql_connection_info['connection']['sid']
                    dsn_string = cx_Oracle.makedsn(srvr, port, sid)
                    try:
                        self.db_connection = cx_Oracle.connect(user=un, password=pw, dsn=dsn_string)
                    except cx_Oracle.DatabaseError:
                        # Try - uid/pwd@server:port/sid
                        port = self.sql_connection_info['connection']['port']
                        sid = self.sql_connection_info['connection']['sid']
                        self.db_connection = cx_Oracle.connect("{0}/{1}@{2}:{3}/{4}".format(un, pw, srvr, port, sid))
                self.db_cursor = self.db_connection.cursor()
            elif self.drvr == 'SQL Server':
                import pyodbc
                if inst is not None:
                    srvr = '{0}\\{1}'.format(srvr, inst)                
                self.__sql_server_connection_str = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}".format(self.drvr, srvr, db, un, pw)
                self.db_connection = pyodbc.connect(self.__sql_server_connection_str)
                self.db_cursor = self.db_connection.cursor()
                self.__sql_server_connection_str = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4};OPTION=3".format(self.drvr, srvr, db, '', '')
            elif 'MySQL' in self.drvr:
                import pyodbc
                # Ex. "DRIVER={MySQL ODBC 5.3 ANSI Driver}; SERVER=localhost; DATABASE=test; UID=root;OPTION=3"
                self.__sql_server_connection_str = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4};OPTION=3".format(self.drvr, srvr, db, un, pw)
                self.db_connection = pyodbc.connect(self.__sql_server_connection_str)
                self.db_cursor = self.db_connection.cursor()
                self.__sql_server_connection_str = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4};OPTION=3".format(self.drvr, srvr, db, '', '')

    def layers_to_keep(self):
        """List of layers to keep."""
        layers_to_keep = set()
        try:
            layers = self.job['location']['config']['layers']
            for layer in layers:
                try:
                    if layer['action'] == 'INCLUDE':
                        layers_to_keep.add((layer['name'], layer['owner']))
                        self.__get_info(layer)
                except KeyError:
                    self.__get_info(layer)
                    continue
        except KeyError:
            pass
        return list(layers_to_keep)

    def layers_to_skip(self):
        """List of layers to skip."""
        layers_to_skip = set()
        try:
            layers = self.job['location']['config']['layers']
            for layer in layers:
                try:
                    if layer['action'] == 'EXCLUDE':
                        layers_to_skip.add((layer['name'], layer['owner']))
                except KeyError:
                    continue
        except KeyError:
            pass
        return list(layers_to_skip)

    def views_to_keep(self):
        """List of views to keep."""
        views_to_keep = set()
        try:
            views = self.job['location']['config']['views']
            for view in views:
                try:
                    if view['action'] == 'INCLUDE':
                        views_to_keep.add((view['name'], view['owner'], view['schema']))
                        self.__get_info(view)
                except KeyError:
                    self.__get_info(view)
                    continue
        except KeyError:
            pass
        return list(views_to_keep)

    def views_to_skip(self):
        """List of views to skip."""
        views_to_skip = set()
        try:
            views = self.job['location']['config']['views']
            for view in views:
                try:
                    if view['action'] == 'EXCLUDE':
                        views_to_skip.add((view['name'], view['owner'], view['schema']))
                except KeyError:
                    continue
        except KeyError:
            pass
        return list(views_to_skip)

    def tables_to_keep(self):
        """List of tables to keep (may include wild card)."""
        tables_to_keep = set()
        try:
            tables = self.job['location']['config']['tables']
            for table in tables:
                try:
                    if table['action'] == 'INCLUDE':
                        if 'owner' in table:
                            tables_to_keep.add((table['name'], table['owner']))
                        else:
                            tables_to_keep.add(table['name'])
                        self.__get_info(table)
                except KeyError:
                    # There is no action, but map fields.
                    self.__get_info(table)
                    continue
        except KeyError:
            pass
        return list(tables_to_keep)

    def tables_to_skip(self):
        """List of tables to skip (may include a wild card)."""
        tables_to_skip = set()
        try:
            tables = self.job['location']['config']['tables']
            for table in tables:
                try:
                    if table['action'] == 'EXCLUDE':
                        if 'owner' in table:
                            tables_to_skip.add((table['name'], table['owner']))
                        else:
                            tables_to_skip.add(table['name'])
                except KeyError:
                    # There is no action, but continue.
                    continue
        except KeyError:
            pass
        return list(tables_to_skip)

    def get_increment(self, count):
        """Returns a suitable base 10 increment."""
        p = int(math.log10(count))
        if not p:
            p = 1
        return int(math.pow(10, p - 1))

    def default_mapping(self, field_type=''):
        """Get the default prefix name to append to each field."""
        if field_type:
            try:
                ft = self.field_types[field_type]
                return ft
            except KeyError:
                return 'meta_'
        else:
            return None

    def map_fields(self, table_name, field_names, field_types={}):
        """Returns mapped field names. Order matters."""
        mapped_field_names = copy.copy(field_names)
        if self.__field_mapping:
            for mapping in self.field_mapping:
                if mapping['name'] == '*':
                    fmap = mapping['map']
                elif mapping['name'].lower() == table_name.lower():
                    mapped_field_names = copy.copy(field_names)
                    fmap = mapping['map']
                else:
                    continue
                for i, field in enumerate(mapped_field_names):
                    try:
                        mapped_field_names[i] = fmap[field]
                    except KeyError:
                        if field_types:
                            if field in field_types:
                                field_map = self.default_mapping(field_types[field])
                                mapped_field_names[i] = '{0}{1}'.format(field_map, field)
                            else:
                                mapped_field_names[i] = '{0}{1}'.format('meta_', field)
                        else:
                            mapped_field_names[i] = '{0}{1}'.format('meta_', field)
            return mapped_field_names
        elif mapped_field_names:
            for i, field in enumerate(mapped_field_names):
                mapped_field_names[i] = '{0}{1}'.format(self.default_mapping(field_types[field]), field)
            return mapped_field_names
        else:
            return mapped_field_names

    def get_join(self, table_name):
        """Get and return the join information."""
        join = None
        if self.joins:
            for j in self.joins:
                if table_name.lower() == j['name'].lower():
                    join = j['join']
            return join

    def get_table_constraint(self, table_name):
        """Get and return the constraint for a table."""
        constraint = ''
        if self.table_constraints:
            for tc in self.table_constraints:
                if tc['name'] == '*':
                    constraint = tc['constraint']
                elif tc['name'].lower() == table_name.lower():
                    constraint = tc['constraint']
                    break
        return constraint

    def get_table_query(self, table_name):
        """Get and return the query for a table."""
        query = ''
        if self.table_queries:
            for q in self.table_queries:
                if q['name'] == '*':
                    query = q['query']
                elif q['name'].lower() == table_name.lower():
                    query = q['query']
                    break
        return query

    def execute_query(self, query):
        """Execute the SQL query and return a new cursor object."""
        return self.db_cursor.execute(query)

    def connect_to_zmq(self):
        """Connect to zmq instance."""
        try:
            self.zmq_socket = zmq.Context.instance().socket(zmq.PUSH)
            self.zmq_socket.connect(self.job['connection']['indexer'])
        except Exception as ex:
            sys.stdout.write(repr(ex))
            sys.exit(1)

    def send_entry(self, entry):
        """Sends an entry to be indexed using pyzmq."""
        try:
            self.zmq_socket.send_json(entry, cls=ObjectEncoder)
        except Exception as ex:
            print(ex)

    def search_fields(self, dataset):
        """Returns a valid list of existing fields for the search cursor."""
        #TODO: use prefered dict comprehension method: {f.name: f.type for f in arcpy.ListFields(dataset, fld)}
        import arcpy
        fields = {}
        if not self.fields_to_keep == ['*']:
            for fld in self.fields_to_keep:
                fdict = dict((f.name, f.type) for f in arcpy.ListFields(dataset, '*{0}*'.format(fld)))
                fields = dict(fields.items() + fdict.items())
        if self.fields_to_skip:
            for fld in self.fields_to_skip:
                [fields.pop(f.name) for f in arcpy.ListFields(dataset, fld) if f.name in fields]
            return fields
        else:
            return dict((f.name, f.type) for f in arcpy.ListFields(dataset))

    #
    # Private functions.
    #

    def __get_domains(self):
        """List of workspace domains (for Esri workspace types).
        Only supported with ArcGIS 10.1 and higher.
        """
        if self.use_coded_value_descriptions:
            for ext in ('.gdb', '.mdb', '.sde'):
                if ext in self.path:
                    import arcpy
                    workspace = '{0}{1}'.format(self.path.split(ext)[0], ext)
                    self.domains = {d.name: d.codedValues for d in arcpy.da.ListDomains(workspace)}
                    break

    def __get_info(self, table):
        """Gets info such as field mapping, queries, and constraints."""
        try:
            try:
                if table['query'] and table['constraint']:
                    sys.stderr.write('Config Error: A table cannot have a query and a constraint.')
                    sys.stderr.flush()
                    sys.exit(1)
            except KeyError:
                pass
            try:
                self.__format = table['format']
            except KeyError:
                pass
            try:
                self.__related_tables = table['related_tables']
            except KeyError:
                pass
            try:
                if self.sql_queries:
                    self.__table_queries.append({'name': '*', 'query': self.sql_queries[0]})
                self.__table_queries.append({'name': table['name'], 'query': table['query']})
            except KeyError:
                pass
            try:
                self.__table_constraints.append({'name': table['name'], 'constraint': table['constraint']})
            except KeyError:
                pass
            try:
                if not {'name': table['name'], 'map': table['map']} in self.__field_mapping:
                    self.__field_mapping.append({'name': table['name'], 'map': table['map']})
            except KeyError:
                pass
            try:
                if not {'name': table['name'], 'new_fields': table['new_fields']} in self.__new_fields:
                    self.__new_fields.append({'name': table['name'], 'new_fields': table['new_fields']})
            except KeyError:
                pass
            try:
                if not {'name': table['name'], 'join': table['join']} in self.__joins:
                    self.__joins.append({'name': table['name'], 'join': table['join']})
            except KeyError:
                pass
        except KeyError:
            pass
