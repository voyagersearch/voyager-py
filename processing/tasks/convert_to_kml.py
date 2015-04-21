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
import glob
import shutil
import urllib2
from tasks.utils import status
from tasks.utils import task_utils
from tasks import _


status_writer = status.Writer()
import arcpy


def execute(request):
    """Converts each input dataset to kml (.kmz).
    :param request: json as a dict.
    """
    converted = 0
    skipped = 0
    errors = 0
    parameters = request['params']

    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)

    # Get the boundary box extent for input to KML tools.
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
        else:
            groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

        status_writer.send_percent(0.0, _('Starting to process...'), 'convert_to_kml')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'])
            result = convert_to_kml(input_items, out_workspace, extent)
            converted += result[0]
            errors += result[1]
            skipped += result[2]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'convert_to_kml')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        converted, errors, skipped = convert_to_kml(input_items, out_workspace, extent, True)

    # Zip up kmz files if more than one.
    if converted > 1:
        status_writer.send_status("Converted: {}".format(converted))
        zip_file = task_utils.zip_data(out_workspace, 'output.zip')
        shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    else:
        kml_file = glob.glob(os.path.join(out_workspace, '*.kmz'))[0]
        tmp_lyr = arcpy.KMLToLayer_conversion(kml_file, out_workspace, 'kml_layer')
        task_utils.make_thumbnail(tmp_lyr.getOutput(0), os.path.join(request['folder'], '_thumb.png'))
        shutil.move(kml_file, os.path.join(request['folder'], os.path.basename(kml_file)))

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(errors + skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), converted, skipped, errors)


def convert_to_kml(input_items, out_workspace, extent, show_progress=False):
    converted = 0
    errors = 0
    skipped = 0
    if show_progress:
        count = len(input_items)
        i = 1.
        status_writer.send_percent(0.0, _('Starting to process...'), 'convert_to_kml')
    arcpy.env.overwriteOutput = True
    for ds, out_name in input_items.iteritems():
        try:
            # Is the input a mxd data frame.
            map_frame_name = task_utils.get_data_frame_name(ds)
            if map_frame_name:
                ds = ds.split('|')[0].strip()

            dsc = arcpy.Describe(ds)

            if dsc.dataType == 'FeatureClass':
                arcpy.MakeFeatureLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'ShapeFile':
                arcpy.MakeFeatureLayer_management(ds, dsc.name[:-4])
                if out_name == '':
                    out_name = dsc.name[:-4]
                arcpy.LayerToKML_conversion(dsc.name[:-4],
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'RasterDataset':
                arcpy.MakeRasterLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'Layer':
                if out_name == '':
                    if dsc.name.endswith('.lyr'):
                        out_name = dsc.name[:-4]
                    else:
                        out_name = dsc.name
                arcpy.LayerToKML_conversion(ds,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.MakeFeatureLayer_management(fc, 'tmp_lyr')
                    arcpy.LayerToKML_conversion('tmp_lyr',
                                                '{0}.kmz'.format(os.path.join(out_workspace, fc)),
                                                1,
                                                boundary_box_extent=extent)
                    converted += 1

            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        try:
                            cad_anno = arcpy.ImportCADAnnotation_conversion(
                                cad_fc,
                                arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB)
                            )
                        except arcpy.ExecuteError:
                            cad_anno = arcpy.ImportCADAnnotation_conversion(
                                cad_fc,
                                arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB),
                                1
                            )
                        arcpy.MakeFeatureLayer_management(cad_anno, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)
                        converted += 1
                    else:
                        arcpy.MakeFeatureLayer_management(cad_fc, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)
                        converted += 1

            # Map document to KML.
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(ds)
                if map_frame_name:
                    data_frames = arcpy.mapping.ListDataFrames(mxd, map_frame_name)
                else:
                    data_frames = arcpy.mapping.ListDataFrames(mxd)
                for df in data_frames:
                    name = '{0}_{1}'.format(dsc.name[:-4], df.name)
                    arcpy.MapToKML_conversion(ds,
                                              df.name,
                                              '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                              extent_to_export=extent)
                converted += 1

            else:
                if show_progress:
                    status_writer.send_percent(i / count, _('Invalid input type: {0}').format(dsc.name), 'convert_to_kml')
                    i += 1
                skipped += 1
                continue
            if show_progress:
                status_writer.send_percent(i / count, _('Converted: {0}').format(ds), 'convert_to_kml')
                i += 1
            else:
                status_writer.send_status(_('Converted: {0}').format(ds))
        except Exception as ex:
            if show_progress:
                status_writer.send_percent(i / count, _('Skipped: {0}').format(ds), 'convert_to_kml')
                i += 1
            status_writer.send_status(_('WARNING: {0}').format(repr(ex)))
            errors += 1
            pass

    return converted, errors, skipped
