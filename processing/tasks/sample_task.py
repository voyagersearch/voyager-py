# -*- coding: utf-8 -*-
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
import sys
from utils import status
from utils import task_utils
from tasks import _

status_writer = status.Writer()
### outfile = open('c:\\temp\\outfile.txt', 'w')
outfile = open('outfile.txt', 'w')

def execute(request):

	status_writer.send_status(_('Starting process ...'))
	outfile.write("=== STARTING ===\n\n")
	
	outfile.write("Writing parameter values passed from Voyager. Format: 'Parameter Type (param name, value property)'\n\n")

	parameters = request['params']
	
	sample_string = task_utils.get_parameter_value(parameters, 'sample_string', 'value')
	sample_integer = str(task_utils.get_parameter_value(parameters, 'sample_integer', 'value'))
	sample_choice = task_utils.get_parameter_value(parameters, 'sample_choice', 'value')
	sample_catalog = task_utils.get_parameter_value(parameters, 'sample_catalog', 'value')
	sample_folder = task_utils.get_parameter_value(parameters, 'sample_folder', 'value')
	sample_checkbox = str(task_utils.get_parameter_value(parameters, 'sample_checkbox', 'value'))
	sample_projection = str(task_utils.get_parameter_value(parameters, 'sample_projection', 'code'))
	sample_geometry = task_utils.get_parameter_value(parameters, 'sample_geometry', 'wkt')
	sample_mapview = task_utils.get_parameter_value(parameters, 'sample_mapview', 'extent')

	outfile.write("Sample String (sample_string, value) @ " + sample_string + "\n\n")
	outfile.write("Sample Integer (sample_integer, value) @ " + sample_integer + "\n\n")
	outfile.write("Sample StringChoice (sample_choice, value) @ " + sample_choice + "\n\n")
	outfile.write("Sample CatalogPath (sample_catalog, value) @ " + sample_catalog + "\n\n")
	outfile.write("Sample FolderLocation (sample_folder, value) @ " + sample_folder + "\n\n")
	outfile.write("Sample CheckBox (sample_checkbox, value) @ " + sample_checkbox + "\n\n")
	outfile.write("Sample Projection (sample_projection, code) @ " + sample_projection + "\n\n")
	outfile.write("Sample Geometry (sample_geometry, wkt) @ " + sample_geometry + "\n\n")
	outfile.write("Sample MapView (sample_mapview, extent) @ " + sample_mapview + "\n\n")

	status_writer.send_status(_('Stopping process ...'))
	outfile.write("=== STOPING ===\n\n")
	
	outfile.close()
	
	return

