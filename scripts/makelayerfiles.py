"""---------------------------------------------------------------------------
Name:        makelayerfiles.py
Purpose:     Make layer files for feature classes & rasters in a geodatabase.
Author:      NewfoundGEO Consulting
Created:     29/05/2013
Updated:     05/06/2013
---------------------------------------------------------------------------"""
import os
import arcpy

arcpy.env.overwriteOutput = True

def save_layer_file(dataset, folder, data_type='Feature'):
    """Save feature class as a layer file."""
    try:
        layer_name = '{0}_layer'.format(dataset.split('.')[-1])
        if data_type == 'Feature':
            layer = arcpy.management.MakeFeatureLayer(dataset, layer_name)
        elif data_type == 'Raster':
            layer = arcpy.management.MakeRasterLayer(dataset, layer_name)
        elif data_type == 'RasterCatalog':
            layer = arcpy.management.MakeRasterCatalogLayer(dataset, layer_name)
        elif data_type == 'Mosaic':
            layer = arcpy.management.MakeMosaicLayer(dataset, layer_name)

        layer_file = os.path.join(folder, '{0}.lyr'.format(dataset.split('.')[-1]))
        arcpy.management.SaveToLayerFile(layer, layer_file)
    except Exception as ex:
        arcpy.AddWarning(ex.args[0])
        pass
# End save_layer_file function

def main(input_workspace, parent_folder):
    """Makes layer files for all feature classes and rasters in the geodatabase.
    Layer files for stand-alone feature classes and rasters will be created
    in the parent folder. Layer files for feature classes in feature datasets
    will be created in sub-folders.
    """
    arcpy.env.workspace = input_workspace

    # Rasters
    arcpy.AddMessage('Processing rasters...')
    rasters = arcpy.ListRasters()
    if rasters:
        increment = 1
        arcpy.SetProgressor('step',
                            'Saving to layer files...',
                            0,
                            len(rasters),
                            increment)
        for raster in rasters:
            save_layer_file(raster, parent_folder, 'Raster')
            arcpy.SetProgressorPosition()
        arcpy.SetProgressorPosition(len(rasters))

    # Raster catalogs
    arcpy.AddMessage('Processing raster catalogs...')
    raster_catalogs = arcpy.ListDatasets('', 'RasterCatalog')
    if raster_catalogs:
        increment = 1
        arcpy.SetProgressor('step',
                            'Saving to layer files...',
                            0,
                            len(raster_catalogs),
                            increment)
        for rc in raster_catalogs:
            save_layer_file(rc, parent_folder, 'RasterCatalog')
            arcpy.SetProgressorPosition()
        arcpy.SetProgressorPosition(len(raster_catalogs))

    # Mosaic datasets
    arcpy.AddMessage('Processing mosaic datasets...')
    mosaics = arcpy.ListDatasets('', 'Mosaic')
    if mosaics:
        increment = 1
        arcpy.SetProgressor('step',
                            'Saving to layer files...',
                            0,
                            len(mosaics),
                            increment)
        for md in mosaics:
            save_layer_file(md, parent_folder, 'Mosaic')
            arcpy.SetProgressorPosition()
        arcpy.SetProgressorPosition(len(mosaics))

    # Stand-alone feature classes
    arcpy.AddMessage('Processing stand-alone feature classes...')
    feature_classes = arcpy.ListFeatureClasses()
    if feature_classes:
        increment = 1
        arcpy.SetProgressor('step',
                            'Saving to layer files...',
                            0,
                            len(feature_classes),
                            increment)
        for fc in feature_classes:
            save_layer_file(fc, parent_folder, 'Feature')
            arcpy.SetProgressorPosition()
        arcpy.SetProgressorPosition(len(feature_classes))

    # Feature classes in feature datasets
    feature_datasets = arcpy.ListDatasets('', 'Feature')
    if feature_datasets:
        for fds in feature_datasets:
            arcpy.AddMessage('Processing feature classes in feature dataset {0}...'.format(fds))
            fds_folder = arcpy.management.CreateFolder(parent_folder, fds)
            arcpy.env.workspace = os.path.join(input_workspace, fds)
            feature_classes = arcpy.ListFeatureClasses()
            increment = 1
            arcpy.SetProgressor('step',
                            'Saving to layer files...',
                            0,
                            len(feature_classes),
                            increment)
            for fc in feature_classes:
                save_layer_file(fc, fds_folder.getOutput(0), 'Feature')
                arcpy.SetProgressorPosition()
            arcpy.SetProgressorPosition(len(feature_classes))
# End make_layer_files function

if __name__ == '__main__':
    in_workspace = arcpy.GetParameterAsText(0)
    out_folder = arcpy.GetParameterAsText(1)
    main(in_workspace, out_folder)
