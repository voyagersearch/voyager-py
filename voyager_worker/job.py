import os
import sys
import json
try:
    try:
        import zmq
    except ImportError:
        pyzmq = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname( __file__ )), '..', 'arch', 'win32_x86', 'py', 'pyzmq-14.3.0-py2.7-win32.egg'))
        sys.path.append(pyzmq)
        import zmq
except ImportError as ie:
    sys.stdout.write(repr(ie))
    sys.exit(1)


class Job(object):
    def __init__(self, job_file):
        self.job_file = job_file
        self.job = json.load(open(job_file, 'r'))
        self.zmq_context = zmq.Context.instance()
        self.zmq_socket = self.zmq_context.socket(zmq.PUSH)
        self.connect_to_zmq()

    @property
    def fields_to_keep(self):
        """List of fields to keep (may include wild card)."""
        try:
            return self.job['location']['config']['fields']['include']
        except KeyError:
            return '*'

    @property
    def fields_to_skip(self):
        """List of fields to skip (may include a wild card)."""
        try:
            return self.job['location']['config']['fields']['exclude']
        except KeyError:
            return None

    @property
    def field_mapping(self):
        """Field mapping as key, value pairs."""
        try:
            return self.job['location']['config']['fields']['map']
        except KeyError:
            return None

    @property
    def default_mapping(self):
        """Default prefix name to append to each field."""
        try:
            return self.job['location']['config']['fields']['map_default_prefix']
        except KeyError:
            return None

    @property
    def action_type(self):
        """The action type."""
        return 'ADD'

    @property
    def location_id(self):
        """The location ID"""
        return self.job['location']['id']

    @property
    def path(self):
        """Catalog path for esri data types."""
        try:
            return self.job['location']['config']['path']
        except KeyError:
            return ''

    @property
    def sql_info(self):
        """SQL connection information as key, value pairs."""
        try:
            return self.job['location']['config']['sql']
        except KeyError:
            return None

    def map_fields(self, field_names):
        """Returns mapped field names."""
        field_map = self.field_mapping
        default_map = self.default_mapping
        for i, field in enumerate(field_names):
            try:
                field_names[i] = field_map[field]
            except KeyError:
                if default_map:
                    field_names[i] = '{0}{1}'.format(default_map, field)
        return field_names

    def connect_to_zmq(self):
        """Connect to zmq instance."""
        try:
            self.zmq_socket.connect(self.job['connection']['indexer'])
        except Exception as ex:
            sys.stdout.write(repr(ex))
            sys.exit(1)

    def send_entry(self, entry):
        """Sends an entry to be indexed using pyzmq."""
        self.zmq_socket.send_json(entry)
