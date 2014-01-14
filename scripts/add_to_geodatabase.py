"""----------------------------------------------------------------------------
Name:        add_to_geodatabase.py
Purpose:     Copies data to a new or existing geodatabase.
Author:
Created:     01/10/2013
Copyright:
----------------------------------------------------------------------------"""
import os
import zipfile
import arcpy

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

        z.write(os.path.join(os.path.dirname(file_gdb), 'output.log'), 'output.log')

    return zfile
# End zip_data function

def create_unique_name(name, gdb):
    """Creates a valid and unique name for the geodatabase."""
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name
# End create_unique_name function

def add_to_gdb(datasets, out_gdb, out_coordinate_system=''):
    """Copys data to a new or existing geodatabase."""
    log_file = open(os.path.join(os.path.dirname(out_gdb), 'output.log'), 'w')

    # Create the geodatabase if it does not exist.
    if not os.path.exists(out_gdb):
        out_gdb = arcpy.management.CreateFileGDB(os.path.dirname(out_gdb),
                                                os.path.basename(out_gdb))[0]
        log_file.write('Created output geodatabase: {0}.\n'.format(out_gdb))
    else:
        log_file.write('Using existing geodatabase: {0}.\n'.format(out_gdb))

    # Set the output coordinate system environment.
    if not out_coordinate_system in ('', '#', None):
        arcpy.env.outputCoordinateSystem = int(out_coordinate_system)
        log_file.write('Output coordinate system: {0}.\n'.format(arcpy.env.outputCoordinateSystem))
    else:
        log_file.write('Output coordinate system is same as inputs.\n')

    arcpy.env.workspace = out_gdb
    datasets = datasets.split(';')
    for ds in datasets:
        try:
            dsc = arcpy.Describe(ds)
            if dsc.dataType == 'FeatureClass':
                arcpy.management.CopyFeatures(ds, create_unique_name(dsc.name, out_gdb))
            elif dsc.dataType == 'ShapeFile':
                arcpy.management.CopyFeatures(ds, create_unique_name(dsc.name[:-4], out_gdb))
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_gdb, dsc.name)
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(create_unique_name(fc, out_gdb))
                    arcpy.management.CopyFeatures(fc, os.path.join(fds[0], name))
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'RasterDataset':
                arcpy.management.CopyRaster(ds, create_unique_name(dsc.name, out_gdb))
            elif dsc.dataType == 'RasterCatalog':
                arcpy.management.CopyRasterCatalogItems(ds, create_unique_name(dsc.name, out_gdb))
            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource, create_unique_name(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, create_unique_name(layer.name, out_gdb))
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
                    del group_layer, layer
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

            arcpy.AddMessage('--Added {0} to {1}...'.format(ds, out_gdb))
            log_file.write('--Added {0} to {1}.\n'.format(ds, out_gdb))

        # Continue if an error. Process as many as possible.
        except Exception as ex:
            arcpy.AddWarning(ex)
            log_file.write('--Failed to add: {0}...\n'.format(ds))
            log_file.write('--Error: {0}.\n'.format(ex))
            continue

    log_file.flush()
    log_file.close()

    # Zip the output gdb and log file.
    zip_name = os.path.splitext(os.path.basename(out_gdb))[0]
    zf = zip_data(out_gdb, '{}.zip'.format(zip_name))
    arcpy.SetParameterAsText(3, zf)

# End add_to_gdb function

if __name__ == '__main__':
    input_datasets = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    out_cs = arcpy.GetParameterAsText(2)
    add_to_gdb(input_datasets, output_geodatabase, out_cs)

