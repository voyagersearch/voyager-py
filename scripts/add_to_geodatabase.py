"""----------------------------------------------------------------------------
Name:        clip_data.py
Purpose:     Copies data to a new or existing geodatabase.
Author:
Created:     01/10/2013
Copyright:
----------------------------------------------------------------------------"""
import os
import arcpy


def copy_data(datasets, out_gdb, out_coordinate_system=''):
    """Copys data to a new or existing geodatabase."""
    # Create the geodatabase if it does not exist.
    if not os.path.exists(out_gdb):
        out_gdb = arcpy.management.CreateFileGDB(os.path.dirname(out_gdb),
                                                os.path.basename(out_gdb))[0]

    # Set the output coordinate system environment.
    if not out_coordinate_system in ('', '#', None):
        arcpy.env.outputCoordinateSystem = int(out_coordinate_system)

    arcpy.env.workspace = out_gdb
    datasets = datasets.split(';')
    for ds in datasets:
        try:
            dsc = arcpy.Describe(ds)
            if dsc.dataType == 'FeatureClass':
                arcpy.management.CopyFeatures(ds, arcpy.CreateUniqueName(dsc.name, out_gdb))
            elif dsc.dataType == 'ShapeFile':
                arcpy.management.CopyFeatures(ds, arcpy.CreateUniqueName(dsc.name[:-4], out_gdb))
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_gdb, dsc.name)
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(arcpy.CreateUniqueName(fc, out_gdb))
                    arcpy.management.CopyFeatures(fc, os.path.join(fds[0], name))
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'RasterDataset':
                arcpy.management.CopyRaster(ds, arcpy.CreateUniqueName(dsc.name, out_gdb))
            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource, arcpy.CreateUniqueName(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, arcpy.CreateUniqueName(layer.name, out_gdb))
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        arcpy.conversion.ImportCADAnnotation(cad_fc, arcpy.CreateUniqueName(cad_fc, out_gdb))
                    arcpy.management.CopyFeatures(cad_fc, arcpy.CreateUniqueName(cad_fc, out_gdb))
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
                        arcpy.management.CopyFeatures(layer.dataSource, arcpy.CreateUniqueName(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, arcpy.CreateUniqueName(layer.name, out_gdb))

        # Continue if an error. Process as many as possible.
        except Exception as ex:
            arcpy.AddWarning(ex)
            continue
# End copy_data function

if __name__ == '__main__':
    input_datasets = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    out_cs = arcpy.GetParameterAsText(2)
    copy_data(input_datasets, output_geodatabase, out_cs)

