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
    resampling_method = task_utils.get_parameter_value(parameters, 'resampling_method', 'value')

    # Advanced options
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')

    # Create the task folder to hold report files.
    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    processed = 0
    skipped = 0
    resampling_options = {'NEAREST_NEIGHBOR': 'NEAREST',
                          'BILINEAR_INTERPOLATION': 'BILINEAR',
                          'CUBIC_CONVOLUTION': 'CUBIC'}

    i = 1.
    count = len(input_items)
    status_writer.send_percent(0.0, _('Starting to process...'), 'build_raster_pyramids')
    for result in input_items:
        dsc = arcpy.Describe(result)
        if not dsc.dataType in ('RasterDataset', 'MosaicDataset', 'RasterCatalog'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a raster dataset type.').format(result))
            skipped += 1
            i += 1
        else:
            try:
                # Build pyramids
                if dsc.dataType in ('RasterCatalog', 'MosaicDataset'):
                    arcpy.BuildPyramidsandStatistics_management(
                        result,
                        calculate_statistics='NONE',
                        resample_technique=resampling_options[resampling_method],
                        compression_type=compression_method,
                        compression_quality=compression_quality
                    )
                else:
                    arcpy.BuildPyramids_management(
                        result,
                        resample_technique=resampling_options[resampling_method],
                        compression_type=compression_method,
                        compression_quality=compression_quality
                    )
                status_writer.send_percent(i/count, _('Built Pyramids for: {0}').format(dsc.name), 'build_raster_pyramids')
                i += 1
                processed += 1
            except arcpy.ExecuteError as ee:
                status_writer.send_state(status.STAT_WARNING, _('Failed to clip {0}. {1}').format(result, ee))
                skipped += 1
                i += 1

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.md'), processed, skipped)
