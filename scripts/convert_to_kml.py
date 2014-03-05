"""Converts data to kml (.kmz)."""
from os.path import basename, dirname, join
import os
import status
import zipfile
import arcpy


def zip_data(data_location, name):
    """Create and return a compressed zip file containing
     the clipped datasets."""
    zfile = join(data_location, name)
    with zipfile.ZipFile(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(data_location):
            for f in files:
                if not f.endswith('zip'):
                    absf = join(root, f)
                    zf = absf[len(data_location) + len(os.sep):]
                    z.write(absf, join(basename(data_location), zf))
    return zfile
# End zip_data function


def from_wkt(wkt, sr):
    """Return the clip geometry from a list
    of well-known text coordinates."""
    coordinates = wkt[wkt.find('(') + 2: wkt.find(')')].split(',')
    array = arcpy.Array()
    for p in coordinates:
        pt = p.strip().split(' ')
        array.add(arcpy.Point(float(pt[0]), float(pt[1])))

    poly = arcpy.Polygon(array, sr)
    return poly
# End from_wkt function


def convert_to_kml(input_items, out_workspace, extent=''):
    """Converts each input dataset to kml (.kmz)."""
    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        input_items = eval(input_items)
    except SyntaxError:
        # If not output names are passed in.
        input_items = dict((k, '') for k in input_items.split(';'))

    if not extent == '':
        extent = from_wkt(extent, 4326)

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
                                            '{0}.kmz'.format(join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'ShapeFile':
                arcpy.management.MakeFeatureLayer(ds, dsc.name[:-4])
                if out_name == '':
                    out_name = dsc.name[:-4]
                arcpy.conversion.LayerToKML(dsc.name[:-4],
                                            '{0}.kmz'.format(join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'RasterDataset':
                arcpy.management.MakeRasterLayer(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.conversion.LayerToKML(dsc.name,
                                            '{0}.kmz'.format(join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'Layer':
                if out_name == '':
                    if dsc.name.endswith('.lyr'):
                        out_name = dsc.name[:-4]
                    else:
                        out_name = dsc.name
                arcpy.conversion.LayerToKML(ds,
                                            '{0}.kmz'.format(join(out_workspace, out_name)),
                                            0,
                                            boundary_box_extent=extent)

            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.management.MakeFeatureLayer(fc, 'tmp_lyr')
                    arcpy.conversion.LayerToKML('tmp_lyr',
                                                '{0}.kmz'.format(join(out_workspace, fc)),
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
                                                    '{0}.kmz'.format(join(out_workspace, name)),
                                                    0,
                                                    boundary_box_extent=extent)
                    else:
                        arcpy.management.MakeFeatureLayer(cad_fc, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.conversion.LayerToKML('cad_lyr',
                                                    '{0}.kmz'.format(join(out_workspace, name)),
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
                                              '{0}.kmz'.format(join(out_workspace, name)),
                                              extent_to_export=extent)

            status_writer.send_percent(i/count, 'Converted {0}.'.format(ds), 'convert_to_kml')
            i += 1.
        except Exception as ex:
            status_writer.send_percent(i/count,
                                       '--Error: {0}.\n Failed to convert: {1}.\n'.format(ex, ds),
                                       'convert_to_kml')
            pass

    if count > 1:
        status_writer.send_status('Creating the output zip file: {0}...'.format(join(out_workspace, 'output.zip')))
        zip_data(out_workspace, 'output.zip')
# End convert_to_kml function

