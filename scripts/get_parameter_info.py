import sys
import json
import inspect
import argparse

# Don't create .pyc file.
sys.dont_write_bytecode = True


class TaskInfo(object):
    """Provides parameter information for each processing task."""
    def __call__(self, *args, **kwargs):
        """Return all the processing tasks argument information as JSON.

        By using the inspect module to get the arguments for each task,
        argument names and defaults can change in the processing scripts without changes here.
        """
        self.add_to_gdb_info()
        self.clip_data_info()
        self.convert_to_kml_info()
        self.zip_files_info()


    @staticmethod
    def add_to_gdb_info():
        import add_to_geodatabase

        arg_spec = inspect.getargspec(add_to_geodatabase.add_to_gdb)
        req_args = arg_spec.args[:-len(arg_spec.defaults)]
        args_defaults = zip(arg_spec.args[-len(arg_spec.defaults):], arg_spec.defaults)

        params = list()
        params.append({'name': req_args[0], 'type': 'VoyagerResults', 'required': 'True'})
        params.append({'name': args_defaults[0][0], 'type': 'Projection', 'code': args_defaults[0][1], 'required': 'false'})
        param_info = {'task': 'add_to_geodatabase', 'params': params}
        sys.stdout.write(json.dumps(param_info))
        sys.stdout.flush()


    @staticmethod
    def clip_data_info():
        import clip_data

        arg_spec = inspect.getargspec(clip_data.clip_data)
        req_args = arg_spec.args[:-len(arg_spec.defaults)]
        args_defaults = zip(arg_spec.args[-len(arg_spec.defaults):], arg_spec.defaults)

        params = list()
        params.append({'name': req_args[0], 'type': 'VoyagerResults', 'required': 'True'})
        params.append({'name': req_args[2], 'type': 'Geometry', 'required': 'True'})
        params.append({'name': args_defaults[0][0], 'type': 'Projection', 'code': args_defaults[0][1], 'required': 'false'})
        params.append({'name': args_defaults[1][0],
                       'type': 'StringChoice',
                       'value': args_defaults[1][1],
                       'required': 'false',
                       'choices': [['FileGDB', 'File Geodatabase'],
                                   ['SHP', 'Shapefile'],
                                   ['LPK', 'Layer Package'],
                                   ['MPK', 'Map Package']]})

        param_info = {'task': 'clip_data', 'params': params}
        sys.stdout.write(json.dumps(param_info))
        sys.stdout.flush()


    @staticmethod
    def convert_to_kml_info():
        import convert_to_kml
        arg_spec = inspect.getargspec(convert_to_kml.convert_to_kml)
        req_args = arg_spec.args[:-len(arg_spec.defaults)]
        args_defaults = zip(arg_spec.args[-len(arg_spec.defaults):], arg_spec.defaults)

        params = list()
        params.append({'name': req_args[0], 'type': 'VoyagerResults', 'required': 'True'})
        params.append({'name': args_defaults[0][0], 'type': 'Geometry', 'required': 'false'})
        param_info = {'task': 'convert_to_kml', 'params': params}
        sys.stdout.write(json.dumps(param_info))
        sys.stdout.flush()


    @staticmethod
    def zip_files_info():
        import zip_files

        arg_spec = inspect.getargspec(zip_files.zip_files)
        req_args = arg_spec.args

        params = list()
        params.append({'name': req_args[0], 'type': 'VoyagerResults', 'required': 'True'})
        param_info = {'task': 'zip_files', 'params': params}
        sys.stdout.write(json.dumps(param_info))
        sys.stdout.flush()

# End TaskInfo class

if __name__ == '__main__':
    task_info = TaskInfo()
    parser = argparse.ArgumentParser(description='Provide parameter information for a processing task.')
    parser.add_argument('--info', action='store_true')
    parser.add_argument('--add_to_geodatabase', action='store_true')
    parser.add_argument('--clip_data', action='store_true')
    parser.add_argument('--convert_to_kml', action='store_true')
    parser.add_argument('--zip_files', action='store_true')

    args = vars(parser.parse_args())
    if args['info']:
        task_info()
    elif args['add_to_geodatabase']:
        task_info.add_to_gdb_info()
    elif args['clip_data']:
        task_info.clip_data_info()
    elif args['convert_to_kml']:
        task_info.convert_to_kml_info()
    elif args['zip_files']:
        task_info.zip_files_info()
