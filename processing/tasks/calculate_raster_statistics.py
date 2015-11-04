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
import requests
from utils import status
from utils import task_utils


status_writer = status.Writer()
import arcpy

errors_reasons = {}
skipped_reasons = {}


def execute(request):
    """Builds raster pyramids for input raster datasets.
    :param request: json as a dict.
    """
    processed = 0
    skipped = 0
    parameters = request['params']

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

    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    num_results, response_index = task_utils.get_result_count(parameters)
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

        # Begin processing
        status_writer.send_percent(0.0, _('Starting to process...'), 'calculate_raster_statistics')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
            elif 'ids' in parameters[response_index]:
                results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
            else:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

            # input_items = task_utils.get_input_items(eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs'])
            input_items = task_utils.get_input_items(results.json()['response']['docs'])
            if not input_items:
                input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])

            result = calculate_raster_statistics(input_items, extent, horizontal_skip_factor, vertical_skip_factor, ignore_pixel_values)
            processed += result[0]
            skipped += result[1]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'calculate_raster_statistics')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        processed, skipped = calculate_raster_statistics(input_items, extent, horizontal_skip_factor,
                                                         vertical_skip_factor, ignore_pixel_values, True)

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '__report.json'), processed, skipped, skipped_details=skipped_reasons)


def calculate_raster_statistics(input_items, extent, horizontal_skip_factor,
                                vertical_skip_factor, ignore_pixel_values, show_progress=False):
    """Calculates raster statistics."""
    processed = 0
    skipped = 0
    if show_progress:
        i = 1.
        count = len(input_items)
        status_writer.send_percent(0.0, _('Starting to process...'), 'calculate_raster_statistics')

    for result in input_items:
        dsc = arcpy.Describe(result)
        if not hasattr(dsc, 'datasetType'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a raster dataset type.').format(result))
            skipped += 1
            skipped_reasons[result] = _('is not a raster dataset type.')
            if show_progress:
                i += 1
            continue

        if dsc.datasetType not in ('RasterDataset', 'MosaicDataset'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a valid raster type.').format(result))
            skipped_reasons[result] = _('is not a valid raster type.')
            skipped += 1
            if show_progress:
                i += 1
        else:
            try:
                # Calculate Statistics
                if result.endswith('.lyr'):
                    result = dsc.dataElement.catalogPath
                arcpy.CalculateStatistics_management(result,
                                                     horizontal_skip_factor,
                                                     vertical_skip_factor,
                                                     ignore_pixel_values,
                                                     'SKIP_EXISTING',
                                                     extent)
                if show_progress:
                    status_writer.send_percent(i / count,
                                               _('Calculated statistics for: {0}').format(result),
                                               'calculate_raster_statistics')
                    i += 1
                else:
                    status_writer.send_status(_('Calculated statistics for: {0}').format(result))
                processed += 1
            except arcpy.ExecuteError as ee:
                status_writer.send_state(status.STAT_WARNING,
                                         _('Failed to calculate statistics for: {0}. {1}').format(result, ee))
                skipped_reasons[result] = ee.message
                skipped += 1
                if show_progress:
                    i += 1
    return processed, skipped
