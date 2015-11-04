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
import collections
import shutil
import requests
from utils import status
from utils import task_utils


status_writer = status.Writer()
import arcpy


pixel_types = {"U1": "1_BIT",
               "U2": "2_BIT",
               "U4": "4_BIT",
               "U8": "8_BIT_UNSIGNED",
               "S8": "8_BIT_SIGNED",
               "U16": "16_BIT_UNSIGNED",
               "S16": "16_BIT_SIGNED",
               "U32": "32_BIT_UNSIGNED",
               "S32": "32_BIT_SIGNED",
               "F32": "32_BIT_FLOAT",
               "F64": "64_BIT"}
skipped_reasons = {}


def get_items(input_items):
    pixels = []
    raster_items = []
    bands = collections.defaultdict(int)
    skipped = 0
    for item in input_items:
        # Number of bands for each item should be the same.
        dsc = arcpy.Describe(item)
        if dsc.dataType == 'RasterDataset':
            raster_items.append(item)
            if hasattr(dsc, 'pixeltype'):
                pixels.append(dsc.pixeltype)
            elif dsc.bandcount > 1:
                pixels.append(arcpy.Describe(os.path.join(dsc.catalogPath, 'Band_1')).pixeltype)
            bands[dsc.bandcount] = 1
        else:
            status_writer.send_status(_('Invalid input type: {0}').format(item))
            skipped_reasons[item] = 'Invalid input type'
            skipped += 1
    return raster_items, pixels, bands, skipped


def execute(request):
    """Mosaics input raster datasets into a new raster dataset.
    :param request: json as a dict.
    """
    parameters = request['params']
    out_coordinate_system = task_utils.get_parameter_value(parameters, 'output_projection', 'code')
    # Advanced options
    output_raster_format = task_utils.get_parameter_value(parameters, 'raster_format', 'value')
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')
    arcpy.env.compression = '{0} {1}'.format(compression_method, compression_quality)

    clip_area = None
    if not output_raster_format == 'MosaicDataset':
        # Get the clip region as an extent object.
        try:
            clip_area_wkt = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
            if not clip_area_wkt:
                clip_area_wkt = 'POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))'
            if not out_coordinate_system == '0':
                clip_area = task_utils.get_clip_region(clip_area_wkt, out_coordinate_system)
            else:
                clip_area = task_utils.get_clip_region(clip_area_wkt)
        except KeyError:
            pass

    status_writer.send_status(_('Setting the output workspace...'))
    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)
    if output_raster_format == 'FileGDB' or output_raster_format == 'MosaicDataset':
        out_workspace = arcpy.CreateFileGDB_management(out_workspace, 'output.gdb').getOutput(0)
    arcpy.env.workspace = out_workspace

    status_writer.send_status(_('Starting to process...'))
    num_results, response_index = task_utils.get_result_count(parameters)
    raster_items = None
    if num_results > task_utils.CHUNK_SIZE:
        # Query the index for results in groups of 25.
        query_index = task_utils.QueryIndex(parameters[response_index])
        fl = query_index.fl
        query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
        fq = query_index.get_fq()
        if fq:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')
            query += fq
        elif 'ids' in parameters[response_index]:
            groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
        else:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')

        headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
        for group in groups:
            if fq:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
            elif 'ids' in parameters[response_index]:
                results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
            else:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

            input_items = task_utils.get_input_items(results.json()['response']['docs'])
            if not input_items:
                input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
            raster_items, pixels, bands, skipped = get_items(input_items)
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        raster_items, pixels, bands, skipped = get_items(input_items)

    if not raster_items:
        if skipped == 0:
            status_writer.send_state(status.STAT_FAILED, _('Invalid input types'))
            skipped_reasons['All Items'] = _('Invalid input types')
            task_utils.report(os.path.join(request['folder'], '__report.json'), len(raster_items), num_results, skipped_details=skipped_reasons)
            return
        else:
            status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
            task_utils.report(os.path.join(request['folder'], '__report.json'), len(raster_items), skipped, skipped_details=skipped_reasons)
            return

    # Get most common pixel type.
    pixel_type = pixel_types[max(set(pixels), key=pixels.count)]
    if output_raster_format in ('FileGDB', 'GRID', 'MosaicDataset'):
        output_name = arcpy.ValidateTableName('mosaic', out_workspace)
    else:
        output_name = '{0}.{1}'.format(arcpy.ValidateTableName('mosaic', out_workspace), output_raster_format.lower())

    if output_raster_format == 'MosaicDataset':
        try:
            status_writer.send_status(_('Generating {0}. Large input {1} will take longer to process.'.format('Mosaic', 'rasters')))
            if out_coordinate_system == '0':
                out_coordinate_system = raster_items[0]
            else:
                out_coordinate_system = None
            mosaic_ds = arcpy.CreateMosaicDataset_management(out_workspace,
                                                             output_name,
                                                             out_coordinate_system,
                                                             max(bands),
                                                             pixel_type)
            arcpy.AddRastersToMosaicDataset_management(mosaic_ds, 'Raster Dataset', raster_items)
            arcpy.MakeMosaicLayer_management(mosaic_ds, 'mosaic_layer')
            layer_object = arcpy.mapping.Layer('mosaic_layer')
            task_utils.make_thumbnail(layer_object, os.path.join(request['folder'], '_thumb.png'))
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
            return
    else:
        try:
            if len(bands) > 1:
                status_writer.send_state(status.STAT_FAILED, _('Input rasters must have the same number of bands'))
                return
            status_writer.send_status(_('Generating {0}. Large input {1} will take longer to process.'.format('Mosaic', 'rasters')))
            if out_coordinate_system == '0':
               out_coordinate_system = None
            if clip_area:
                ext = '{0} {1} {2} {3}'.format(clip_area.XMin, clip_area.YMin, clip_area.XMax, clip_area.YMax)
                tmp_mosaic = arcpy.MosaicToNewRaster_management(
                    raster_items,
                    out_workspace,
                    'tmpMosaic',
                    out_coordinate_system,
                    pixel_type,
                    number_of_bands=bands.keys()[0]
                )
                status_writer.send_status(_('Clipping...'))
                out_mosaic = arcpy.Clip_management(tmp_mosaic, ext, output_name)
                arcpy.Delete_management(tmp_mosaic)
            else:
                out_mosaic = arcpy.MosaicToNewRaster_management(raster_items,
                                                                out_workspace,
                                                                output_name,
                                                                out_coordinate_system,
                                                                pixel_type, number_of_bands=bands.keys()[0])
            arcpy.MakeRasterLayer_management(out_mosaic, 'mosaic_layer')
            layer_object = arcpy.mapping.Layer('mosaic_layer')
            task_utils.make_thumbnail(layer_object, os.path.join(request['folder'], '_thumb.png'))
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
            return

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)
    zip_file = task_utils.zip_data(out_workspace, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '__report.json'), len(raster_items), skipped, skipped_details=skipped_reasons)
