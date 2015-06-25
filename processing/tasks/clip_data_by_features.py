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
import shutil
import urllib2
import arcpy
from utils import status
from utils import task_utils


status_writer = status.Writer()
result_count = 0
processed_count = 0.
files_to_package = list()


def clip_data(input_items, out_workspace, clip_polygons, out_format):
    """Clips input results."""
    clipped = 0
    errors = 0
    skipped = 0
    fds = None
    global processed_count

    for ds, out_name in input_items.iteritems():
        try:
            # -----------------------------------------------
            # If the item is a service layer, process and continue.
            # -----------------------------------------------
            if ds.startswith('http'):
                try:
                    service_layer = task_utils.ServiceLayer(ds)
                    # if out_coordinate_system == 0:
                    #     wkid = service_layer.wkid
                    #     out_sr = arcpy.SpatialReference(wkid)
                    #     arcpy.env.outputCoordinateSystem = out_sr
                    # else:
                    #     out_sr = task_utils.get_spatial_reference(out_coordinate_system)
                    #     arcpy.env.outputCoordinateSystem = out_sr
                    #
                    # if not out_sr.name == gcs_sr.name:
                    #     try:
                    #         geo_transformation = arcpy.ListTransformations(gcs_sr, out_sr)[0]
                    #         clip_poly = gcs_clip_poly.projectAs(out_sr, geo_transformation)
                    #     except (AttributeError, IndexError):
                    #         try:
                    #             clip_poly = gcs_clip_poly.projectAs(out_sr)
                    #         except AttributeError:
                    #             clip_poly = gcs_clip_poly
                    #     except ValueError:
                    #         clip_poly = gcs_clip_poly
                    # else:
                    #     clip_poly = gcs_clip_poly

                    arcpy.env.overwriteOutput = True
                    oid_groups = service_layer.object_ids
                    out_features = None
                    for group in oid_groups:
                        group = [oid for oid in group if not oid == None]
                        where = '{0} IN {1}'.format(service_layer.oid_field_name, tuple(group))
                        url = ds + "/query?where={}&outFields={}&returnGeometry=true&geometryType=esriGeometryPolygon&geometry={}&f=json&token={}".format(where, '*', eval(clip_poly.JSON), '')
                        feature_set = arcpy.FeatureSet()
                        feature_set.load(url)
                        if not out_features:
                            out_features = arcpy.Clip_analysis(feature_set, clip_polygons, out_name)
                        else:
                            clip_features = arcpy.Clip_analysis(feature_set, clip_polygons, 'in_memory/features')
                            arcpy.Append_management(clip_features, out_features, 'NO_TEST')
                    processed_count += 1.
                    clipped += 1
                    status_writer.send_percent(processed_count / result_count, _('Clipped: {0}').format(ds), 'clip_data')
                    continue
                except Exception as ex:
                    status_writer.send_state(status.STAT_WARNING, str(ex))
                    errors += 1
                    continue

            # -----------------------------------------------
            # Check if the path is a MXD data frame type.
            # ------------------------------------------------
            map_frame_name = task_utils.get_data_frame_name(ds)
            if map_frame_name:
                ds = ds.split('|')[0].strip()

            dsc = arcpy.Describe(ds)
            try:
                if dsc.spatialReference.name == 'Unknown':
                    status_writer.send_state(status.STAT_WARNING, _('{0} has an Unknown projection. Output may be invalid or empty.').format(dsc.name))
            except AttributeError:
                pass

            # # --------------------------------------------------------------------
            # # If no output coord. system, get output spatial reference from input.
            # # --------------------------------------------------------------------
            # if out_coordinate_system == 0:
            #     try:
            #         out_sr = dsc.spatialReference
            #         arcpy.env.outputCoordinateSystem = out_sr
            #     except AttributeError:
            #         out_sr = task_utils.get_spatial_reference(4326)
            #         arcpy.env.outputCoordinateSystem = out_sr
            # else:
            #     out_sr = task_utils.get_spatial_reference(out_coordinate_system)
            #     arcpy.env.outputCoordinateSystem = out_sr
            #
            # # -------------------------------------------------
            # # If the item is not a file, project the clip area.
            # # -------------------------------------------------
            # if dsc.dataType not in ('File', 'TextFile'):
            #     if not out_sr.name == gcs_sr.name:
            #         try:
            #             geo_transformation = arcpy.ListTransformations(gcs_sr, out_sr)[0]
            #             clip_poly = gcs_clip_poly.projectAs(out_sr, geo_transformation)
            #         except (AttributeError, IndexError):
            #             try:
            #                 clip_poly = gcs_clip_poly.projectAs(out_sr)
            #             except AttributeError:
            #                 clip_poly = gcs_clip_poly
            #         except ValueError:
            #             clip_poly = gcs_clip_poly
            #     else:
            #         clip_poly = gcs_clip_poly
            #     extent = clip_poly.extent


            # -----------------------------
            # Check the data type and clip.
            # -----------------------------

            # Feature Class or ShapeFile
            if dsc.dataType in ('FeatureClass', 'ShapeFile'):
                if out_name == '':
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                arcpy.Clip_analysis(ds, clip_polygons, name)

            # Feature dataset
            elif dsc.dataType == 'FeatureDataset':
                if not out_format == 'SHP':
                    fds_name = os.path.basename(task_utils.create_unique_name(dsc.name, out_workspace))
                    fds = arcpy.CreateFeatureDataset_management(out_workspace, fds_name)
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    try:
                        if not out_format == 'SHP':
                            arcpy.Clip_analysis(fc, clip_polygons, task_utils.create_unique_name(fc, fds))
                        else:
                            arcpy.Clip_analysis(fc, clip_polygons, task_utils.create_unique_name(fc, out_workspace))
                    except arcpy.ExecuteError:
                        pass
                arcpy.env.workspace = out_workspace

            # Raster dataset
            elif dsc.dataType == 'RasterDataset':
                if out_name == '':
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                # ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
                extent = arcpy.Describe(clip_polygons).extent
                ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
                arcpy.Clip_management(ds, ext, name, in_template_dataset=clip_polygons, clipping_geometry="ClippingGeometry")

            # Layer file
            elif dsc.dataType == 'Layer':
                task_utils.clip_layer_file(dsc.catalogPath, clip_polygons, arcpy.env.workspace)

            # Cad drawing dataset
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    name = task_utils.create_unique_name('{0}_{1}'.format(cad_wks_name, cad_fc), out_workspace)
                    arcpy.Clip_analysis(cad_fc, clip_polygons, name)
                arcpy.env.workspace = out_workspace

            # File
            elif dsc.dataType in ('File', 'TextFile'):
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    kml_layer = arcpy.KMLToLayer_conversion(dsc.catalogPath, arcpy.env.scratchFolder, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.Clip_analysis(layer,
                                                clip_polygons,
                                                task_utils.create_unique_name(layer, out_workspace))
                    # Clean up temp KML results.
                    arcpy.Delete_management(os.path.join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    arcpy.Delete_management(kml_layer[1])
                    del group_layer
                else:
                    if out_name == '':
                        out_name = dsc.name
                    if out_workspace.endswith('.gdb'):
                        f = arcpy.Copy_management(ds, os.path.join(os.path.dirname(out_workspace), out_name))
                    else:
                        f = arcpy.Copy_management(ds, os.path.join(out_workspace, out_name))
                    processed_count += 1.
                    status_writer.send_percent(processed_count / result_count, _('Copied file: {0}').format(dsc.name), 'clip_data')
                    status_writer.send_state(_('Copied file: {0}').format(dsc.name))
                    clipped += 1
                    if out_format in ('LPK', 'MPK'):
                        files_to_package.append(f.getOutput(0))
                    continue

            # Map document
            elif dsc.dataType == 'MapDocument':
                task_utils.clip_mxd_layers(dsc.catalogPath, clip_polygons, arcpy.env.workspace, map_frame_name)
            else:
                processed_count += 1.
                status_writer.send_percent(processed_count / result_count, _('Invalid input type: {0}').format(ds), 'clip_data')
                status_writer.send_state(_('Invalid input type: {0}').format(ds))
                skipped += 1
                continue

            processed_count += 1.
            status_writer.send_percent(processed_count / result_count, _('Clipped: {0}').format(dsc.name), 'clip_data')
            status_writer.send_status(_('Clipped: {0}').format(dsc.name))
            clipped += 1
        # Continue. Process as many as possible.
        except Exception as ex:
            processed_count += 1.
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(os.path.basename(ds)), 'clip_data')
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors += 1
            pass
    return clipped, errors, skipped


def execute(request):
    """Clips selected search results using the clip geometry.
    :param request: json as a dict.
    """
    clipped = 0
    errors = 0
    skipped = 0
    global result_count
    parameters = request['params']

    # Retrieve the clip features.
    clip_features = task_utils.get_parameter_value(parameters, 'clip_features', 'value')

    # Retrieve the coordinate system code.
    out_coordinate_system = int(task_utils.get_parameter_value(parameters, 'output_projection', 'code'))

    # Retrieve the output format and create mxd parameter values.
    out_format = task_utils.get_parameter_value(parameters, 'output_format', 'value')
    create_mxd = task_utils.get_parameter_value(parameters, 'create_mxd', 'value')

    # Create the temporary workspace if clip_feature_class:
    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)

    # Set the output coordinate system.
    if not out_coordinate_system == 0:  # Same as Input
        out_sr = task_utils.get_spatial_reference(out_coordinate_system)
        arcpy.env.outputCoordinateSystem = out_sr

    # Create the clip polygon geometry object in WGS84 projection.
    # gcs_sr = task_utils.get_spatial_reference(4326)
    # gcs_clip_poly = task_utils.from_wkt(clip_area, gcs_sr)
    # if not gcs_clip_poly.area > 0:
    #     gcs_clip_poly = task_utils.from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', gcs_sr)

    # Set the output workspace.
    status_writer.send_status(_('Setting the output workspace...'))
    if not out_format == 'SHP':
        out_workspace = arcpy.CreateFileGDB_management(out_workspace, 'output.gdb').getOutput(0)
    arcpy.env.workspace = out_workspace

    # Query the index for results in groups of 25.
    result_count, response_index = task_utils.get_result_count(parameters)
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl
    query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    else:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

    status_writer.send_percent(0.0, _('Starting to process...'), 'clip_data')
    for group in groups:
        if fq:
            results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
        else:
            results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

        input_items = task_utils.get_input_items(eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs'])
        result = clip_data(input_items, out_workspace, clip_features, out_format)
        clipped += result[0]
        errors += result[1]
        skipped += result[2]

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)
    if clipped > 0:
        try:
            if out_format == 'MPK':
                mxd_template = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'MapTemplate.mxd')
                mxd = task_utils.create_mxd(out_workspace, mxd_template, 'output')
                status_writer.send_status(_("Packaging results..."))
                task_utils.create_mpk(out_workspace, mxd, files_to_package)
                shutil.move(os.path.join(out_workspace, 'output.mpk'),
                            os.path.join(os.path.dirname(out_workspace), 'output.mpk'))
            elif out_format == 'LPK':
                status_writer.send_status(_("Packaging results..."))
                task_utils.create_lpk(out_workspace, files_to_package)
            elif out_format == 'KML':
                task_utils.convert_to_kml(os.path.join(out_workspace, "output.gdb"))
                arcpy.env.workspace = ''
                arcpy.Delete_management(os.path.join(out_workspace, "output.gdb"))
                zip_file = task_utils.zip_data(out_workspace, 'output.zip')
                shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
            else:
                if create_mxd:
                    mxd_template = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'MapTemplate.mxd')
                    task_utils.create_mxd(out_workspace, mxd_template, 'output')
                zip_file = task_utils.zip_data(out_workspace, 'output.zip')
                shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
        except arcpy.ExecuteError as ee:
            status_writer.send_state(status.STAT_FAILED, _(ee))
            sys.exit(1)
    else:
        status_writer.send_state(status.STAT_FAILED, _('No output created. Zero inputs were clipped.'))

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(errors + skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), clipped, skipped, errors)

