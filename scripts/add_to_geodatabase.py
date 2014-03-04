"""Copies data to a new or existing geodatabase."""
from __future__ import unicode_literals
import os
import zipfile
import arcpy
import status

__author__ = 'VoyagerSearch'
__copyright__ = 'VoyagerSearch, 2014'
__date__ = '03/04/2014'


def zip_data(file_gdb, name):
    """Creates a compressed zip file for the geodatabase folder."""
    zfile = os.path.join(os.path.dirname(file_gdb), name)
    with zipfile.ZipFile(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(file_gdb):
            for f in files:
                if not f.endswith('zip'):
                    absf = os.path.join(root, f)
                    zf = absf[len(file_gdb) + len(os.sep):]
                    z.write(absf, os.path.join(os.path.basename(file_gdb), zf))
    return zfile
# End zip_data function


def create_unique_name(name, gdb):
    """Creates a valid and unique name for the geodatabase."""
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name
# End create_unique_name function


def add_to_gdb(input_items, output_workspace, out_coordinate_system=None):
    """Copies data to a new or existing geodatabase."""
    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        input_items = eval(input_items)
    except SyntaxError:
        # If not output names are passed in.
        input_items = dict((k, '') for k in input_items.split(';'))

    # Create the geodatabase if it does not exist.
    if not output_workspace.endswith('.gdb'):
        out_gdb = arcpy.management.CreateFileGDB(output_workspace, 'output.gdb').getOutput(0)
    else:
        out_gdb = output_workspace
    arcpy.env.workspace = out_gdb

    # Set the output coordinate system environment.
    if out_coordinate_system is not None:
        arcpy.env.outputCoordinateSystem = int(out_coordinate_system)

    i = 1.
    count = len(input_items)
    status_writer = status.Writer()
    status_writer.send_status('Starting to add data to {0}...'.format(out_gdb))
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)
            if dsc.dataType == 'FeatureClass':
                if out_name == '':
                    arcpy.management.CopyFeatures(ds, create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyFeatures(ds, create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'ShapeFile':
                if out_name == '':
                    arcpy.management.CopyFeatures(ds, create_unique_name(dsc.name[:-4], out_gdb))
                else:
                    arcpy.management.CopyFeatures(ds, create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_gdb, dsc.name)
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(create_unique_name(fc, out_gdb))
                    arcpy.management.CopyFeatures(fc, os.path.join(fds.getOutput(0), name))
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'RasterDataset':
                if out_name == '':
                    arcpy.management.CopyRaster(ds, create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRaster(ds, create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'RasterCatalog':
                if out_name == '':
                    arcpy.management.CopyRasterCatalogItems(ds, create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRasterCatalogItems(ds, create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if out_name == '':
                        name = create_unique_name(layer.name, out_gdb)
                    else:
                        name = create_unique_name(out_name, out_gdb)
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource, name)
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, name)
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    arcpy.management.CopyFeatures(cad_fc, create_unique_name(cad_wks_name + '_' + cad_fc, out_gdb))
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'File':
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    kml_layer = arcpy.conversion.KMLToLayer(dsc.catalogPath, arcpy.env.scratchFolder, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(arcpy.env.scratchFolder, '{}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.management.CopyFeatures(layer, arcpy.ValidateTableName(layer))
                    # Clean up temp KML results.
                    arcpy.management.Delete(os.path.join(arcpy.env.scratchFolder, '{}.lyr'.format(name)))
                    arcpy.management.Delete(kml_layer[1])
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(mxd)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource, create_unique_name(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, create_unique_name(layer.name, out_gdb))
            elif dsc.dataType.find('Table') > 0:
                if out_name == '':
                    arcpy.management.CopyRows(ds, create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRows(ds, create_unique_name(out_name, out_gdb))

            status_writer.send_percent(i/count, 'Added {0}.'.format(ds), 'add_to_geodatabase')
            i += 1.

        # Continue if an error. Process as many as possible.
        except Exception as ex:
            status_writer.send_percent(i/count,
                                       '--Error: {0}.\n Failed to add: {1}.\n'.format(ex, ds),
                                       'add_to_geodatabase')
            pass

    # Zip the output gdb and log file.
    zip_name = os.path.splitext(os.path.basename(out_gdb))[0]
    zip_data(out_gdb, '{0}.zip'.format(zip_name))
    status_writer.send_status('Completed.')
# End add_to_gdb function

