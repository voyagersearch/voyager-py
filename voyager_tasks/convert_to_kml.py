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


def execute(request):
    """Converts each input dataset to kml (.kmz).
    :param request: json as a dict.
    """
    converted = 0
    skipped = 0
    errors = 0
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    count = len(input_items)
    if count > 1:
        out_workspace = os.path.join(request['folder'], 'temp')
    else:
        out_workspace = request['folder']
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

    i = 1.
    status_writer = status.Writer()
    status_writer.send_percent(0.0, _('Starting to process...'), 'convert_to_kml')
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)

            if dsc.dataType == 'FeatureClass':
                arcpy.MakeFeatureLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'ShapeFile':
                arcpy.MakeFeatureLayer_management(ds, dsc.name[:-4])
                if out_name == '':
                    out_name = dsc.name[:-4]
                arcpy.LayerToKML_conversion(dsc.name[:-4],
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'RasterDataset':
                arcpy.MakeRasterLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)

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

            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.MakeFeatureLayer_management(fc, 'tmp_lyr')
                    arcpy.LayerToKML_conversion('tmp_lyr',
                                                '{0}.kmz'.format(os.path.join(out_workspace, fc)),
                                                1,
                                                boundary_box_extent=extent)

            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        cad_anno = arcpy.ImportCADAnnotation_conversion(
                            cad_fc,
                            arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB)
                        )
                        arcpy.MakeFeatureLayer_management(cad_anno, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)
                    else:
                        arcpy.MakeFeatureLayer_management(cad_fc, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)

            # Map document to KML.
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(ds)
                data_frames = arcpy.mapping.ListDataFrames(mxd)
                for df in data_frames:
                    name = '{0}_{1}'.format(dsc.name[:-4], df.name)
                    arcpy.MapToKML_conversion(ds,
                                              df.name,
                                              '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                              extent_to_export=extent)

            else:
                status_writer.send_percent(i/count, _('Invalid input type: {0}').format(dsc.name), 'convert_to_kml')
                skipped += 1
                continue

            status_writer.send_percent(i/count, _('Converted: {0}').format(ds), 'convert_to_kml')
            i += 1.
            converted += 1
        except Exception as ex:
            status_writer.send_percent(i/count, _('Skipped: {0}').format(ds), 'convert_to_kml')
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            i += 1
            errors += 1
            pass

    # Zip up kmz files if more than one.
    if count > 1:
        zip_file = task_utils.zip_data(out_workspace, 'output.zip')
        shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    else:
        kml_file = glob.glob(os.path.join(out_workspace, '*.kmz'))[0]
        tmp_lyr = arcpy.KMLToLayer_conversion(kml_file, out_workspace, 'kml_layer')
        task_utils.make_thumbnail(tmp_lyr.getOutput(0), os.path.join(request['folder'], '_thumb.png'))

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(errors + skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), converted, skipped, errors)
