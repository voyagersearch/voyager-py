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
## outfile = open('c:\\temp\\outfile.txt', 'w')
outfile = open('outfile.txt', 'w')

def execute(request):

	status_writer.send_status(_('Starting process ...'))
	outfile.write("=== STARTING ===\n\n")
	
	outfile.write("Writing parameter values passed from Voyager. Format: 'Parameter Type (param name, value property)'\n\n")

	parameters = request['params']
	
	sample_catalog = task_utils.get_parameter_value(parameters, 'sample_catalog', 'value')
	sample_checkbox = str(task_utils.get_parameter_value(parameters, 'sample_checkbox', 'value'))
	sample_choice = task_utils.get_parameter_value(parameters, 'sample_choice', 'value')
	sample_folder = task_utils.get_parameter_value(parameters, 'sample_folder', 'value')
	sample_geometry = task_utils.get_parameter_value(parameters, 'sample_geometry', 'wkt')
	sample_integer = str(task_utils.get_parameter_value(parameters, 'sample_integer', 'value'))
	sample_mapview = task_utils.get_parameter_value(parameters, 'sample_mapview', 'extent')
	sample_projection = str(task_utils.get_parameter_value(parameters, 'sample_projection', 'code'))
	sample_string = task_utils.get_parameter_value(parameters, 'sample_string', 'value')	
	sample_choicecombo = task_utils.get_parameter_value(parameters, 'sample_choicecombo', 'value')
	sample_choiceservice = task_utils.get_parameter_value(parameters, 'sample_choiceservice', 'value')
	sample_date = task_utils.get_parameter_value(parameters, 'sample_date', 'value')
	sample_datetime = task_utils.get_parameter_value(parameters, 'sample_datetime', 'value')
	sample_field = task_utils.get_parameter_value(parameters, 'sample_field', 'value')
	sample_fieldlist = task_utils.get_parameter_value(parameters, 'sample_fieldlist', 'value')
	sample_password = task_utils.get_parameter_value(parameters, 'sample_password', 'value')
	sample_textarea = task_utils.get_parameter_value(parameters, 'sample_textarea', 'value')

	outfile.write("Sample CatalogPath (sample_catalog, value) @ " + sample_catalog + "\n\n")	
	outfile.write("Sample CheckBox (sample_checkbox, value) @ " + sample_checkbox + "\n\n")	
	outfile.write("Sample StringChoice (sample_choice, value) @ " + sample_choice + "\n\n")	
	outfile.write("Sample Folder (sample_folder, value) @ " + sample_folder + "\n\n")	
	outfile.write("Sample Geometry (sample_geometry, value) @ " + sample_geometry + "\n\n")	
	outfile.write("Sample Integer (sample_integer, value) @ " + sample_integer + "\n\n")	
	outfile.write("Sample MapView (sample_mapview, value) @ " + sample_mapview + "\n\n")	
	outfile.write("Sample Projection (sample_projection, value) @ " + sample_projection + "\n\n")	
	outfile.write("Sample String (sample_string, value) @ " + sample_string + "\n\n")	
	outfile.write("Sample StringChoiceCombo (sample_choicecombo, value) @ " + sample_choicecombo + "\n\n")	
	outfile.write("Sample StringChoiceService (sample_choiceservice, value) @ " + sample_choiceservice + "\n\n")	
	outfile.write("Sample Date (sample_date, value) @ " + sample_date + "\n\n")	
	outfile.write("Sample DateTime (sample_datetime, value) @ " + sample_datetime + "\n\n")	
	outfile.write("Sample Field (sample_field, value) @ " + sample_field + "\n\n")	
	outfile.write("Sample FieldList (sample_fieldlist, value) @ " + sample_fieldlist + "\n\n")	
	outfile.write("Sample Password (sample_password, value) @ " + sample_password + "\n\n")	
	outfile.write("Sample TextArea (sample_textarea, value) @ " + sample_textarea + "\n\n")	
	
	status_writer.send_status(_('Stopping process ...'))
	outfile.write("=== STOPING ===\n\n")
	
	outfile.close()
	
	return
