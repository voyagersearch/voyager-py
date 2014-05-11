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
    """Mosaics input raster datasets into a new raster dataset or mosaic dataset.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    target_workspace = task_utils.get_parameter_value(parameters, 'target_workspace', 'value')
    output_name = task_utils.get_parameter_value(parameters, 'output_dataset_name', 'value')
    out_coordinate_system = task_utils.get_parameter_value(parameters, 'output_projection', 'code')
    # Advanced options
    output_raster_format = task_utils.get_parameter_value(parameters, 'raster_format', 'value')
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')
    arcpy.env.compression = '{0} {1}'.format(compression_method, compression_quality)

    if output_raster_format in ('FileGDB', 'MosaicDataset'):
        if not os.path.splitext(target_workspace)[1] in ('.gdb', '.mdb', '.sde'):
            status_writer.send_state(status.STAT_FAILED, _('Target workspace must be a geodatabase'))
            sys.exit(1)

    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    if not output_raster_format == 'MosaicDataset':
        # Get the clip region as an extent object.
        try:
            clip_area_wkt = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
            clip_area = task_utils.get_clip_region(clip_area_wkt, out_coordinate_system)
        except KeyError:
            clip_area = None

    #out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(target_workspace):
        status_writer.send_state(status.STAT_FAILED, _('Target workspace does not exist'))
        sys.exit(1)
    arcpy.env.workspace = target_workspace

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
            elif dsc.bandcount > 1:
                pixels.append(arcpy.Describe(os.path.join(dsc.catalogPath, 'Band_1')).pixeltype)
            bands[dsc.bandcount] = 1
        else:
            status_writer.send_status(_('Invalid input type: {0}').format(item))
            skipped += 1

    if not raster_items:
        status_writer.send_state(status.STAT_FAILED, _('Invalid input types'))
        sys.exit(1)

    # Get most common pixel type.
    pixel_type = pixel_types[max(set(pixels), key=pixels.count)]
    if output_raster_format in ('FileGDB', 'GRID', 'MosaicDataset'):
        output_name = arcpy.ValidateTableName(output_name, target_workspace)
    else:
        output_name = '{0}.{1}'.format(arcpy.ValidateTableName(output_name, target_workspace), output_raster_format.lower())

    if output_raster_format == 'MosaicDataset':
        try:
            if out_coordinate_system == '':
                out_coordinate_system = raster_items[0]
            mosaic_ds = arcpy.CreateMosaicDataset_management(target_workspace,
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
            sys.exit(1)
    else:
        try:
            if len(bands) > 1:
                status_writer.send_state(status.STAT_FAILED, _('Input rasters must have the same number of bands'))
                sys.exit(1)
            if clip_area:
                ext = '{0} {1} {2} {3}'.format(clip_area.XMin, clip_area.YMin, clip_area.XMax, clip_area.YMax)
                tmp_mosaic = arcpy.MosaicToNewRaster_management(
                    raster_items,
                    target_workspace,
                    'tempMosaic',
                    out_coordinate_system,
                    pixel_type,
                    number_of_bands=bands.keys()[0]
                )
                status_writer.send_status(_('Clipping...'))
                out_mosaic = arcpy.Clip_management(tmp_mosaic, ext, output_name)
                arcpy.Delete_management(tmp_mosaic)
            else:
                out_mosaic = arcpy.MosaicToNewRaster_management(raster_items,
                                                                target_workspace,
                                                                output_name,
                                                                out_coordinate_system,
                                                                pixel_type,
                                                                number_of_bands=bands.keys()[0],
                                                                mosaic_method='BLEND')
            arcpy.MakeRasterLayer_management(out_mosaic, 'mosaic_layer')
            layer_object = arcpy.mapping.Layer('mosaic_layer')
            task_utils.make_thumbnail(layer_object, os.path.join(request['folder'], '_thumb.png'))
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
            sys.exit(1)

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.md'), len(raster_items), skipped)
