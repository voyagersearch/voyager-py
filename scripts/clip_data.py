"""----------------------------------------------------------------------------
Name:        clip_data.py
Purpose:     Clips data using extent to a new or existing geodatabase.
Author:
Created:     01/10/2013
Copyright:
----------------------------------------------------------------------------"""
import os
from os.path import join, dirname, basename
import glob
import zipfile
import arcpy


def zip_data(file_gdb, name):
    """Creates a compressed zip file for the geodatabase folder."""
    zfile = join(dirname(file_gdb), name)
    with zipfile.ZipFile(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(file_gdb):
            for f in files:
                if not f.endswith('zip'):
                    absf = join(root, f)
                    zf = absf[len(file_gdb) + len(os.sep):]
                    z.write(absf, join(basename(file_gdb), zf))
        z.write('output.log')
        for mpk in glob.glob(join(dirname(file_gdb), '*.mpk')):
            z.write(basename(mpk))
    return zfile
# End zip_data function

def create_unique_name(name, gdb):
    """Creates a valid and unique name for the geodatabase."""
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name
# End create_unique_name function

def clip_mxd_layers(mxd_path, coord_sys, geo_poly):
    """Walks each feature and raster layer and clips it."""
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    for layer in layers:
        if coord_sys == '' or coord_sys == '#':
            out_sr = arcpy.Describe(layer).spatialReference
        else:
            out_sr = arcpy.SpatialReference(int(coord_sys))
        if not geo_poly.spatialReference.name == out_sr.name:
            geo_transformation = arcpy.ListTransformations(geo_poly.spatialReference, out_sr)[0]
            clip_poly = geo_poly.projectAs(out_sr, geo_transformation)
        else:
            clip_poly = geo_poly.projectAs(out_sr)

        if layer.isFeatureLayer:
            arcpy.analysis.Clip(layer.dataSource, clip_poly, layer.name)
        elif layer.isRasterLayer:
            ext = clip_poly.extent
            arcpy.management.Clip(
                layer.dataSource,
                '{} {} {} {}'.format(ext.XMin, ext.YMin, ext.XMax, ext.YMax),
                layer.name
            )
# End clip_mxd_layers function

def clip_data(datasets,
              out_gdb,
              clip_area='',
              out_coordinate_system='',
              non_spatial_data=True):
    """Clips data to a new or existing geodatabase.
    Optionally, the geodatbase can be zipped into
    a distributable file.
    """
    log_file = open(os.path.join(os.path.dirname(out_gdb), 'output.log'), 'w')

    # Create the clip polygon.
    gcs_sr = arcpy.SpatialReference(4326)
    xys = clip_area.split()
    points = arcpy.Array([
        arcpy.Point(xys[0], xys[1]),
        arcpy.Point(xys[0], xys[3]),
        arcpy.Point(xys[2], xys[3]),
        arcpy.Point(xys[2], xys[1])
    ])
    gcs_clip_poly = arcpy.Polygon(points, gcs_sr)

    if not os.path.exists(out_gdb):
        out_gdb = arcpy.management.CreateFileGDB(dirname(out_gdb),
                                                 basename(out_gdb))[0]

    if non_spatial_data is True:
        file_folder = arcpy.management.CreateFolder(dirname(out_gdb),
                                                    'output_files')

    arcpy.env.workspace = out_gdb
    datasets = datasets.split(';')
    for ds in datasets:
        try:
            dsc = arcpy.Describe(ds)

            # Determine output spatial reference and create the clip feature.
            if not dsc.dataType == 'File' and not dsc.dataType == 'MapDocument':
                if out_coordinate_system == '' or out_coordinate_system == '#':
                    try:
                        out_sr = dsc.spatialReference
                    except AttributeError:
                        out_sr = arcpy.SpatialReference(4326)
                else:
                    out_sr = arcpy.SpatialReference(int(out_coordinate_system))
                if not gcs_sr.name == out_sr.name and not out_sr.name == 'Unknown':
                    geo_transformation = arcpy.ListTransformations(gcs_sr,
                                                                   out_sr)[0]
                    clip_poly = gcs_clip_poly.projectAs(out_sr,
                                                        geo_transformation)
                else:
                    clip_poly = gcs_clip_poly.projectAs(out_sr)

            # Geodatabase feature class
            if dsc.dataType == 'FeatureClass':
                arcpy.analysis.Clip(ds,
                                    clip_poly,
                                    create_unique_name(dsc.name, out_gdb))

            # Geodatabase feature dataset
            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.analysis.Clip(fc,
                                        clip_poly,
                                        create_unique_name(fc, out_gdb))
                arcpy.env.workspace = out_gdb

            # Shapefile
            elif dsc.dataType == 'ShapeFile':
                arcpy.analysis.Clip(ds,
                                    clip_poly,
                                    create_unique_name(dsc.name[:-4], out_gdb))

            # Raster dataset
            elif dsc.dataType == 'RasterDataset':
                ext = clip_poly.extent
                arcpy.management.Clip(
                    ds,
                    '{} {} {} {}'.format(ext.XMin, ext.YMin, ext.XMax, ext.YMax),
                    create_unique_name(dsc.name, out_gdb)
                )

            # Layer file
            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.analysis.Clip(
                            layer.dataSource,
                            clip_poly,
                            create_unique_name(layer.name, out_gdb)
                        )
                    elif layer.isRasterLayer:
                        ext = clip_poly.extent
                        arcpy.management.Clip(
                            layer.dataSource,
                            '{} {} {} {}'.format(ext.XMin, ext.YMin, ext.XMax, ext.YMax),
                            create_unique_name(layer.name, out_gdb)
                        )

            # Cad drawing dataset
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        cad_anno = arcpy.conversion.ImportCADAnnotation(cad_fc, arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB))
                        arcpy.analysis.Clip(cad_anno, clip_poly, join(cad_wks_name, cad_fc))
                    arcpy.analysis.Clip(cad_fc, clip_poly, join(cad_wks_name, cad_fc))
                arcpy.env.workspace = out_gdb

            # File
            elif dsc.dataType == 'File':
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    kml_layer = arcpy.conversion.KMLToLayer(
                        dsc.catalogPath,
                        arcpy.env.scratchFolder,
                        name
                    )
                    group_layer = arcpy.mapping.Layer(
                        join(arcpy.env.scratchFolder,
                             '{}.lyr'.format(name))
                    )
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.analysis.Clip(
                                layer,
                                gcs_clip_poly,
                                create_unique_name(layer, out_gdb)
                            )
                    # Clean up temp KML results.
                    arcpy.management.Delete(
                        join(arcpy.env.scratchFolder,
                             '{}.lyr'.format(name))
                    )
                    arcpy.management.Delete(kml_layer[1])
                    del group_layer
                elif non_spatial_data is True:
                    arcpy.management.Copy(ds, join(file_folder, dsc.name))

            # Map document
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(ds)
                if mxd.description == '':
                    mxd.description = dsc.name
                    mxd.save()
                arcpy.management.PackageMap(
                    ds,
                    '{}.mpk'.format(join(dirname(out_gdb), dsc.name)),
                    'CONVERT',
                    'CONVERT_ARCSDE',
                    clip_area
                )
                log_file.write('--Created map package for {0}.'.format(ds))
                continue
                #clip_mxd_layers(dsc.catalogPath, out_coordinate_system, gcs_clip_poly)

            arcpy.AddMessage('--Clipped {0} to {1}...'.format(ds, out_gdb))
            log_file.write('--Clipped {0} to {1}.\n'.format(ds, out_gdb))

        # Continue if an error. Process as many as possible.
        except Exception as ex:
            log_file.write('--Failed to clip {0}.\n'.format(ds))
            log_file.write('--Error: {0}.\n'.format(ex))
            arcpy.AddWarning('--Failed to clip {0}. Error: {1}.'.format(ds, ex))
            pass

    log_file.flush()
    log_file.close()

    zip_name = os.path.splitext(os.path.basename(out_gdb))[0]
    zf = zip_data(out_gdb, '{}.zip'.format(zip_name))
    arcpy.SetParameterAsText(5, zf)
# End clip_data function

if __name__ == '__main__':
    """Collect parameters and call main function."""
    input_datasets = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    clip_extent = arcpy.GetParameterAsText(2)
    out_cs = arcpy.GetParameterAsText(3)
    allow_non_spatial_data = arcpy.GetParameter(4)
    clip_data(input_datasets,
              output_geodatabase,
              clip_extent,
              out_cs,
              allow_non_spatial_data)