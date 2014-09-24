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
import glob
import shutil
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils
from voyager_tasks import _

status_writer = status.Writer()

# Custom Exceptions
class AnalyzeServiceException(Exception):
    pass


def create_service(temp_folder, map_document, server_path, service_name,  folder_name=''):
    """Creates a map service on an ArcGIS Server machine or in an ArcGIS Online account.

    :param temp_folder: folder path where temporary files are created
    :param map_document: map document object
    :param server_path: the ArcGIS server path or connection file path (.ags)
    :param service_name: the name of the service to be created
    :param folder_name: the folder where the service is created
    """
    # Create a temporary definition file.
    draft_file = '{0}.sddraft'.format(os.path.join(temp_folder, service_name))
    status_writer.send_status(_('Creating map sd draft...'))
    arcpy.mapping.CreateMapSDDraft(map_document,
                                   draft_file,
                                   service_name,
                                   'ARCGIS_SERVER',
                                   folder_name=folder_name,
                                   copy_data_to_server=True,
                                   summary=map_document.description,
                                   tags=map_document.tags)

    # Analyze the draft file for any errors before staging.
    status_writer.send_status(_('Analyzing the map sd draft...'))
    analysis = arcpy.mapping.AnalyzeForSD(draft_file)
    if analysis['errors'] == {}:
        # Stage the service.
        stage_file = draft_file.replace('sddraft', 'sd')
        status_writer.send_status(_('Staging the map service...'))
        arcpy.StageService_server(draft_file, stage_file)
    else:
        # If the sddraft analysis contained errors, display them and quit.
        raise AnalyzeServiceException(analysis['errors'])

    # Upload/publish the service.
    status_writer.send_status(_('Publishing the map service to: {0}...').format(server_path))
    result = arcpy.UploadServiceDefinition_server(stage_file, server_path, service_name)
    status_writer.send_status(_('Successfully created: {0}').format(result.getOutput(0)))
    return


def execute(request):
    """Deletes files.
    :param request: json as a dict
    """
    app_folder = os.path.dirname(os.path.abspath(__file__))
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    server_conn = task_utils.get_parameter_value(parameters, 'server_connection_path', 'value')
    service_name = task_utils.get_parameter_value(parameters, 'service_name', 'value')
    folder_name = task_utils.get_parameter_value(parameters, 'folder_name', 'value')
    if not server_conn:
        status_writer.send_state(status.STAT_FAILED, _('A server path is required'))
        return

    request_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(request_folder):
        os.makedirs(request_folder)

    status_writer.send_status(_('Initializing...'))

    map_template = os.path.join(request_folder, 'output.mxd')
    shutil.copyfile(os.path.join(app_folder, 'supportfiles', 'MapTemplate.mxd'), map_template)

    for item in input_items:
        try:
            # Code required because of an Esri bug - cannot describe a map package (raises IOError).
            if item.endswith('.mpk'):
                status_writer.send_status(_('Extracting: {0}').format(item))
                arcpy.ExtractPackage_management(item, request_folder)
                pkg_folder = os.path.join(request_folder, glob.glob1(request_folder, 'v*')[0])
                mxd_file = os.path.join(pkg_folder, glob.glob1(pkg_folder, '*.mxd')[0])
                mxd = arcpy.mapping.MapDocument(mxd_file)
                create_service(request_folder, mxd, server_conn, service_name, folder_name)
            else:
                data_type = arcpy.Describe(item).dataType
                if data_type == 'MapDocument':
                    mxd = arcpy.mapping.MapDocument(item)
                    create_service(request_folder, mxd, server_conn, service_name, folder_name)
                elif data_type == 'Layer':
                    if item.endswith('.lpk'):
                        status_writer.send_status(_('Extracting: {0}').format(item))
                        arcpy.ExtractPackage_management(item, request_folder)
                        pkg_folder = os.path.join(request_folder, glob.glob1(request_folder, 'v*')[0])
                        item = os.path.join(pkg_folder, glob.glob1(pkg_folder, '*.lyr')[0])
                    layer = arcpy.mapping.Layer(item)
                    mxd = arcpy.mapping.MapDocument(map_template)
                    mxd.description = layer.name
                    mxd.tags = layer.name
                    mxd.save()
                    data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
                    arcpy.mapping.AddLayer(data_frame, layer)
                    mxd.save()
                    create_service(request_folder, mxd, server_conn, service_name, folder_name)
                elif data_type in ('FeatureClass', 'ShapeFile', 'RasterDataset'):
                    if data_type == 'RasterDataset':
                        arcpy.MakeRasterLayer_management(item, os.path.basename(item))
                    else:
                        arcpy.MakeFeatureLayer_management(item, os.path.basename(item))
                    layer = arcpy.mapping.Layer(os.path.basename(item))
                    mxd = arcpy.mapping.MapDocument(map_template)
                    mxd.title = layer.name
                    data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
                    arcpy.mapping.AddLayer(data_frame, layer)
                    mxd.save()
                    create_service(request_folder, mxd, server_conn, service_name, folder_name)
        except AnalyzeServiceException as ase:
            status_writer.send_state(status.STAT_FAILED, _(ase))
            return
        except arcpy.ExecuteError as ee:
            status_writer.send_state(status.STAT_FAILED, _(ee))
            return
        except Exception as ex:
            status_writer.send_state(status.STAT_FAILED, _(ex))
            return
