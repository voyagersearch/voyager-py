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
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


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


def execute(request):
    """Mosaics input raster datasets into a new raster dataset.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    out_coordinate_system = task_utils.get_parameter_value(parameters, 'output_projection', 'code')
    # Advanced options
    output_raster_format = task_utils.get_parameter_value(parameters, 'raster_format', 'value')
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')
    arcpy.env.compression = '{0} {1}'.format(compression_method, compression_quality)

    if not output_raster_format == 'MosaicDataset':
        # Get the clip region as an extent object.
        try:
            clip_area_wkt = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
            clip_area = task_utils.get_clip_region(clip_area_wkt, out_coordinate_system)
        except KeyError:
            clip_area = None

    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)
    if output_raster_format == 'FileGDB' or output_raster_format == 'MosaicDataset':
        out_workspace = arcpy.CreateFileGDB_management(out_workspace, 'output.gdb').getOutput(0)
    arcpy.env.workspace = out_workspace

    pixels = []
    raster_items = []
    bands = collections.defaultdict(int)
    skipped = 0
    for item in input_items:
        # Number of bands for each item should be the same.
        dsc = arcpy.Describe(item)
        if dsc.datasettype == 'RasterDataset':
            raster_items.append(item)
            if hasattr(dsc, 'pixeltype'):
                pixels.append(dsc.pixeltype)
            bands[dsc.bandcount] = 1
        else:
            status_writer.send_status(_('invalid_input_type').format(item))
            skipped += 1

    if not raster_items:
        status_writer.send_state(status.STAT_FAILED, _('invalid_input_types'))
        sys.exit(1)

    # Get most common pixel type.
    pixel_type = pixel_types[max(set(pixels), key=pixels.count)]
    if output_raster_format in ('FileGDB', 'GRID', 'MosaicDataset'):
        output_name = arcpy.ValidateTableName('mosaic', out_workspace)
    else:
        output_name = '{0}.{1}'.format(arcpy.ValidateTableName('mosaic', out_workspace), output_raster_format.lower())

    if output_raster_format == 'MosaicDataset':
        try:
            if out_coordinate_system == '':
                out_coordinate_system = raster_items[0]
            mosaic_ds = arcpy.CreateMosaicDataset_management(out_workspace,
                                                             output_name,
                                                             out_coordinate_system,
                                                             max(bands),
                                                             pixel_type)
            arcpy.AddRastersToMosaicDataset_management(mosaic_ds, 'Raster Dataset', raster_items)
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
            sys.exit(1)
    else:
        try:
            if len(bands) > 1:
                status_writer.send_state(status.STAT_FAILED, _('must_have_the_same_number_of_bands'))
                sys.exit(1)
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
                status_writer.send_status(_('clipping'))
                arcpy.Clip_management(tmp_mosaic, ext, output_name)
                arcpy.Delete_management(tmp_mosaic)
            else:
                arcpy.MosaicToNewRaster_management(raster_items,
                                                   out_workspace,
                                                   output_name,
                                                   out_coordinate_system,
                                                   pixel_type, number_of_bands=bands.keys()[0])
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
            sys.exit(1)

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)
    zip_file = task_utils.zip_data(out_workspace, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('results_could_not_be_processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), len(raster_items), skipped)
