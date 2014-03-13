"""Executes a Voyager processing task."""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'voyager_tasks'))
import collections
import json
import voyager_tasks


def run_task(json_file):
    """Main function for running processing tasks."""
    with open(json_file) as data_file:
        request = json.load(data_file)
        try:
            __import__(request['type'])
        except ImportError as ie:
            sys.stderr.write('Error: {0}.'.format(ie.message))
            sys.exit(1)
        getattr(sys.modules[request['type']], "execute")(request)


if __name__ == '__main__':
    if sys.argv[1] == '--info':
        task_info = collections.defaultdict(list)
        for task in voyager_tasks.__tasks__:
            try:
                __import__(task)
                task_info['tasks'].append({'name': task, 'available': True})
            except ImportError as ie:
                task_info['tasks'].append({'name': task, 'available': False, 'warning': ie.message})
        sys.stdout.write(json.dumps(task_info))
        sys.stdout.flush()
    else:
        run_task(sys.argv[1])
    sys.exit(0)


