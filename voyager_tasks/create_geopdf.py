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
import datetime
import locale
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def dd_to_dms(dd):
    """Convert decimal degrees to degrees, minutes, seconds.
    :param dd: decimal degrees as float
    :rtype : tuple of degrees, minutes, seconds
    """
    dd = abs(dd)
    minutes, seconds = divmod(dd*3600, 60)
    degrees, minutes = divmod(minutes, 60)
    seconds = float('{0:.2f}'.format(seconds))
    return int(degrees), int(minutes), seconds


def get_local_date():
    """Returns formatted local date.
    :rtype : str
    """
    locale.setlocale(locale.LC_TIME)
    d = datetime.datetime.today()
    return d.strftime('%x')


def execute(request):
    """Creates a GeoPDF.
    :param request: json as a dict.
    """
    added_to_map = 0
    errors = 0
    skipped = 0
    status_writer = status.Writer()
    parameters = request['params']

    input_items = task_utils.get_input_items(parameters)
    map_template = task_utils.get_parameter_value(parameters, 'map_template', 'value')
    map_title = task_utils.get_parameter_value(parameters, 'map_title', 'value')
    attribute_setting = task_utils.get_parameter_value(parameters, 'attribute_settings', 'value')
    author = task_utils.get_parameter_value(parameters, 'map_author', 'value')
    try:
        map_view = task_utils.get_parameter_value(parameters, 'map_view', 'extent')
    except KeyError:
        map_view = None
        pass

    temp_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    mxd_path = os.path.join(os.path.dirname(__file__), 'supportfiles', 'frame', map_template)
    mxd = arcpy.mapping.MapDocument(mxd_path)
    data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
    layer = None
    for item in input_items:
        try:
            dsc = arcpy.Describe(item)
            if dsc.dataType == 'Layer':
                layer = arcpy.mapping.Layer(dsc.catalogPath)

            elif dsc.dataType == 'FeatureClass' or dsc.dataType == 'ShapeFile':
                feature_layer = arcpy.MakeFeatureLayer_management(item, os.path.basename(item))
                layer_file = arcpy.SaveToLayerFile_management(
                    feature_layer,
                    os.path.join(temp_folder, '{0}.lyr'.format(os.path.basename(item)))
                )
                layer = arcpy.mapping.Layer(layer_file.getOutput(0))

            elif dsc.dataType == 'FeatureDataset' or dsc.datasetType == 'FeatureDataset':
                arcpy.env.workspace = item
                for fc in arcpy.ListFeatureClasses():
                    layer_file = arcpy.SaveToLayerFile_management(arcpy.MakeFeatureLayer_management(fc, fc),
                                                                  os.path.join(temp_folder, fc))
                    layer = arcpy.mapping.Layer(layer_file.getOutput(0))

            elif dsc.dataType == 'RasterDataset':
                raster_layer = arcpy.MakeRasterLayer_management(item, os.path.basename(item))
                layer_file = arcpy.SaveToLayerFile_management(
                    raster_layer,
                    os.path.join(temp_folder, os.path.basename(item))
                )
                layer = arcpy.mapping.Layer(layer_file.getOutput(0))

            elif dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                name = os.path.splitext(dsc.name)[0]
                layer_file = arcpy.KMLToLayer_conversion(dsc.catalogPath, temp_folder, name)
                layer = arcpy.mapping.Layer(layer_file)

            if layer:
                arcpy.mapping.AddLayer(data_frame, layer)
                layer = None
                added_to_map += 1
            else:
                status_writer.send_status(_('Invalid input type: {0}').format(item))
                skipped += 1
        except Exception as ex:
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors += 1
            pass

    if map_view:
        extent = map_view.split(' ')
        new_extent = data_frame.extent
        new_extent.XMin, new_extent.YMin = float(extent[0]), float(extent[1])
        new_extent.XMax, new_extent.YMax = float(extent[2]), float(extent[3])
        data_frame.extent = new_extent
    else:
        data_frame.zoomToSelectedFeatures()

    # Update text elements in map template.
    date_element = arcpy.mapping.ListLayoutElements(mxd, 'TEXT_ELEMENT', 'date')
    if date_element:
        date_element[0].text = 'Date: {0}'.format(get_local_date())

    title_element = arcpy.mapping.ListLayoutElements(mxd, 'TEXT_ELEMENT', 'title')
    if title_element:
        title_element[0].text = map_title

    author_element = arcpy.mapping.ListLayoutElements(mxd, 'TEXT_ELEMENT', 'author')
    if author_element:
        author_element[0].text = '{0} {1}'.format(author_element[0].text, author)

    if map_template in ('ANSI_D_LND.mxd', 'ANSI_E_LND.mxd'):
        coord_elements = arcpy.mapping.ListLayoutElements(mxd, 'TEXT_ELEMENT', 'x*')
        coord_elements += arcpy.mapping.ListLayoutElements(mxd, 'TEXT_ELEMENT', 'y*')
        if coord_elements:
            for e in coord_elements:
                new_text = e.text
                if e.name == 'xmin':
                    dms = dd_to_dms(data_frame.extent.XMin)
                    if data_frame.extent.XMin > 0:
                        new_text = new_text.replace('W', 'E')
                elif e.name == 'xmax':
                    dms = dd_to_dms(data_frame.extent.XMax)
                    if data_frame.extent.XMax > 0:
                        new_text = new_text.replace('W', 'E')
                elif e.name == 'ymin':
                    dms = dd_to_dms(data_frame.extent.YMin)
                    if data_frame.extent.YMin < 0:
                        new_text = new_text.replace('N', 'S')
                elif e.name == 'ymax':
                    if data_frame.extent.YMax < 0:
                        new_text = new_text.replace('N', 'S')
                    dms = dd_to_dms(data_frame.extent.YMax)

                new_text = new_text.replace('d', str(dms[0]))
                new_text = new_text.replace('m', str(dms[1]))
                new_text = new_text.replace('s', str(dms[2]))
                e.text = new_text

    if added_to_map > 0:
        arcpy.mapping.ExportToPDF(mxd,
                                  os.path.join(request['folder'], 'output.pdf'),
                                  layers_attributes=attribute_setting)
        # Create a thumbnail size PNG of the mxd.
        task_utils.make_thumbnail(mxd, os.path.join(request['folder'], '_thumb.png'), False)
    else:
        status_writer.send_state(status.STAT_FAILED, _('No results can be exported to PDF'))
        task_utils.report(os.path.join(request['folder'], '_report.json'), added_to_map, skipped, skipped, 0)
        return

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), added_to_map, skipped, errors)