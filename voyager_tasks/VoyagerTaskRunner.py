"""Executes a Voyager processing task."""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'voyager_tasks'))
import json
import voyager_tasks


def run_task(json_file):
    """Main function for running processing tasks."""
    with open(json_file) as data_file:
        request = json.load(data_file)
        if not os.path.exists(request['folder']):
            os.makedirs(request['folder'])
        getattr(sys.modules[request['task']], "execute")(request)


if __name__ == '__main__':
    if sys.argv[1] == '--info':
        for task in voyager_tasks.__all__:
            param_info = getattr(sys.modules[task], "get_info")()
            sys.stdout.write(json.dumps(param_info))
            sys.stdout.flush()
    else:
        run_task(sys.argv[1])
    sys.exit(0)


