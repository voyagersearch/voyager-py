"""Converts data to kml (.kmz)."""
import os
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def execute(request):
    """Converts each input dataset to kml (.kmz).
    :param request: json as a dict.
    """
    # Retrieve input items to be clipped.
    parameters = request['params']
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', parameters)
    docs = in_data.get('response').get('docs')
    input_items = str(dict((task_utils.get_feature_data(v), v['name']) for v in docs))
    extent = task_utils.find(lambda p: p['name'] == 'extent', parameters)['wkt']
    out_workspace = request['folder']

    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        input_items = eval(input_items)
    except SyntaxError:
        # If not output names are passed in.
        input_items = dict((k, '') for k in input_items.split(';'))

    if not extent == '':
        extent = task_utils.from_wkt(extent, 4326)

    i = 1.
    count = len(input_items)
    status_writer = status.Writer()
    status_writer.send_status('Converting to kml...')
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)

            if dsc.dataType == 'FeatureClass':
                arcpy.management.MakeFeatureLayer(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.conversion.LayerToKML(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'ShapeFile':
                arcpy.management.MakeFeatureLayer(ds, dsc.name[:-4])
                if out_name == '':
                    out_name = dsc.name[:-4]
                arcpy.conversion.LayerToKML(dsc.name[:-4],
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'RasterDataset':
                arcpy.management.MakeRasterLayer(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.conversion.LayerToKML(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'Layer':
                if out_name == '':
                    if dsc.name.endswith('.lyr'):
                        out_name = dsc.name[:-4]
                    else:
                        out_name = dsc.name
                arcpy.conversion.LayerToKML(ds,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.management.MakeFeatureLayer(fc, 'tmp_lyr')
                    arcpy.conversion.LayerToKML('tmp_lyr',
                                                '{0}.kmz'.format(os.path.join(out_workspace, fc)),
                                                0,
                                                boundary_box_extent=extent)

            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                #cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        cad_anno = arcpy.conversion.ImportCADAnnotation(
                            cad_fc,
                            arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB)
                        )
                        arcpy.management.MakeFeatureLayer(cad_anno, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.conversion.LayerToKML('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    0,
                                                    boundary_box_extent=extent)
                    else:
                        arcpy.management.MakeFeatureLayer(cad_fc, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.conversion.LayerToKML('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    0,
                                                    boundary_box_extent=extent)

            # Map document to KML.
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(ds)
                data_frames = arcpy.mapping.ListDataFrames(mxd)
                for df in data_frames:
                    name = '{0}_{1}'.format(dsc.name[:-4], df)
                    arcpy.conversion.MapToKML(ds,
                                              df,
                                              '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                              extent_to_export=extent)

            status_writer.send_percent(i/count, 'Converted {0}.'.format(ds), 'convert_to_kml')
            i += 1.
        except Exception as ex:
            status_writer.send_percent(i/count,
                                       '--Error: {0}.\n Failed to convert: {1}.\n'.format(ex, ds),
                                       'convert_to_kml')
            pass

    if count > 1:
        status_writer.send_status('Creating the output zip file: {0}...'.format(os.path.join(out_workspace, 'output.zip')))
        zip_file = task_utils.zip_data(out_workspace, 'output.zip')
        task_utils.clean_up(os.path.dirname(zip_file))
# End convert_to_kml function

