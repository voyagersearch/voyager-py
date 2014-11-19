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
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils
from voyager_tasks import _

def execute(request):
    """Builds raster pyramids for input raster datasets.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)

    # Get the extent for for which to use to calculate statistics.
    extent = ''
    try:
        try:
            ext = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
            if ext:
                sr = task_utils.get_spatial_reference("4326")
                extent = task_utils.from_wkt(ext, sr)
        except KeyError:
            ext = task_utils.get_parameter_value(parameters, 'processing_extent', 'feature')
            if ext:
                extent = arcpy.Describe(ext).extent
    except KeyError:
        pass

    horizontal_skip_factor = task_utils.get_parameter_value(parameters, 'horizontal_skip_factor', 'value')
    vertical_skip_factor = task_utils.get_parameter_value(parameters, 'vertical_skip_factor', 'value')
    ignore_pixel_values = task_utils.get_parameter_value(parameters, 'ignore_pixel_values', 'value')

    # Create the task folder to hold report files.
    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    processed = 0
    skipped = 0
    i = 1.
    count = len(input_items)
    status_writer.send_percent(0.0, _('Starting to process...'), 'calculate_raster_statistics')
    for result in input_items:
        dsc = arcpy.Describe(result)
        if not dsc.datasetType in ('RasterDataset', 'MosaicDataset'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a valid raster type.').format(result))
            skipped += 1
            i += 1
        else:
            try:
                # Calculate Statistics
                if result.endswith('.lyr'):
                    result = dsc.dataElement.catalogPath
                status_writer.send_status(_('Calculating statistics for: {0}').format(result))
                arcpy.CalculateStatistics_management(result,
                                                     horizontal_skip_factor,
                                                     vertical_skip_factor,
                                                     ignore_pixel_values,
                                                     'SKIP_EXISTING',
                                                     extent)
                status_writer.send_percent(i/count,
                                           _('Calculated statistics for: {0}').format(dsc.name),
                                           'calculate_raster_statistics')
                i += 1
                processed += 1
            except arcpy.ExecuteError as ee:
                status_writer.send_state(status.STAT_WARNING,
                                         _('Failed to calculate statistics for: {0}. {1}').format(result, ee))
                skipped += 1
                i += 1

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.md'), processed, skipped)
