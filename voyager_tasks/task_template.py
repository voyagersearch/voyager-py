"""A template script for a geoprocessing task.
This script must live in the voyager_tasks folder.
"""

#TODO: add/remove imports as needed. Here are common libraries often needed.
import os
import sys
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


"""All tasks should have an execute function. This function takes a request as a required argument.
The request is a dictionary loaded from a .json file and contains the parameter information necessary
to perform the operation. The info folder contains json files that define the parameters of the task.
Refer to the pretend_task.info.json file for list of parameter types. Create a new json file that
contains the name of the task with suffix .info.json (e.g. clip_data.info.json). The task_utils module
contains utility functions and any functions that can be used by multiple tasks should be added there.
The status module can be use to add status to your task."""
def execute(request):
    """TODO: add documentation string.
    :param request: json as a dict.
    """
    pass

