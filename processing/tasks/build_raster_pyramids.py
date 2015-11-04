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

resampling_options = {'NEAREST_NEIGHBOR': 'NEAREST',
                      'BILINEAR_INTERPOLATION': 'BILINEAR',
                      'CUBIC_CONVOLUTION': 'CUBIC'}
skipped_reasons = {}


def execute(request):
    """Builds raster pyramids for input raster datasets.
    :param request: json as a dict.
    """
    processed = 0
    skipped = 0
    parameters = request['params']
    resampling_method = task_utils.get_parameter_value(parameters, 'resampling_method', 'value')

    # Advanced options
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')

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
        status_writer.send_percent(0.0, _('Starting to process...'), 'build_raster_pyramids')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
            elif 'ids' in parameters[response_index]:
                results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
            else:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

            input_items = task_utils.get_input_items(results.json()['response']['docs'])
            if not input_items:
                input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
            result = build_pyramids(input_items, compression_method, compression_quality, resampling_method)
            processed += result[0]
            skipped += result[1]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'build_raster_pyramids')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        processed, skipped = build_pyramids(input_items, compression_method, compression_quality, resampling_method, True)

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '__report.json'), processed, skipped, skipped_details=skipped_reasons)


def build_pyramids(input_items, compression_method, compression_quality, resampling_method, show_progress=False):
    """Build raster pyramids."""
    processed = 0
    skipped = 0
    if show_progress:
        i = 1.
        count = len(input_items)
        status_writer.send_percent(0.0, _('Starting to process...'), 'build_raster_pyramids')

    for result in input_items:
        dsc = arcpy.Describe(result)
        if not hasattr(dsc, 'datasetType'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a raster dataset type.').format(result))
            skipped += 1
            skipped_reasons[result] = _('is not a raster dataset type.')
            if show_progress:
                i += 1
            continue

        if not dsc.datasetType in ('RasterDataset', 'MosaicDataset', 'RasterCatalog'):
            status_writer.send_state(status.STAT_WARNING, _('{0} is not a raster dataset type.').format(result))
            skipped += 1
            skipped_reasons[result] = _('is not a raster dataset type.')
            if show_progress:
                i += 1
        else:
            try:
                # Build pyramids
                if dsc.datasetType in ('RasterCatalog', 'MosaicDataset'):
                    status_writer.send_status(_('Building pyramids for: {0}').format(result))
                    arcpy.BuildPyramidsandStatistics_management(
                        result,
                        calculate_statistics='NONE',
                        resample_technique=resampling_options[resampling_method],
                        compression_type=compression_method,
                        compression_quality=compression_quality
                    )
                # ArcGIS 10.1 bug - Pyramids are not build beyond the first level for rasters in SDE.
                # See: https://geonet.esri.com/thread/71775
                else:
                    arcpy.BuildPyramids_management(
                        result,
                        resample_technique=resampling_options[resampling_method],
                        compression_type=compression_method,
                        compression_quality=compression_quality
                    )
                if show_progress:
                    status_writer.send_percent(i / count,
                                               _('Built Pyramids for: {0}').format(dsc.name),
                                               'build_raster_pyramids')
                    i += 1
                else:
                    status_writer.send_status(_('Built Pyramids for: {0}').format(dsc.name))
                processed += 1
            except arcpy.ExecuteError as ee:
                status_writer.send_state(status.STAT_WARNING,
                                         _('Failed to build pyramids for: {0}. {1}').format(result, ee))
                skipped_reasons[result] = ee.message
                skipped += 1
                if show_progress:
                    i += 1
        continue
    return processed, skipped
