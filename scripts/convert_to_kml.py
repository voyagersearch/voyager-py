"""----------------------------------------------------------------------------
Name:        convert_to_kml.py
Purpose:     Converts data to kml (.kmz).
Author:
Created:     11/29/2013
Copyright:
----------------------------------------------------------------------------"""
from os.path import join
import arcpy

def convert_to_kml(in_datasets, out_workspace, extent=''):
    """Converts each input dataset to kml (.kmz) in the output
    location.
    """
    datatsets = in_datasets.split(';')

    for ds in datatsets:

        dsc = arcpy.Describe(ds)

        if dsc.dataType == 'FeatureClass':
            arcpy.management.MakeFeatureLayer(ds, dsc.name)
            arcpy.conversion.LayerToKML(
                            dsc.name,
                            '{0}.kmz'.format(join(out_workspace, dsc.name)),
                            boundary_box_extent=extent)

        elif dsc.dataType == 'ShapeFile':
            arcpy.management.MakeFeatureLayer(ds, dsc.name[:-4])
            arcpy.conversion.LayerToKML(
                        dsc.name[:-4],
                        '{0}.kmz'.format(join(out_workspace, dsc.name[:-4])),
                        boundary_box_extent=extent)

        elif dsc.dataType == 'RasterDataset':
            arcpy.management.MakeRasterLayer(ds, dsc.name)
            arcpy.conversion.LayerToKML(
                            dsc.name,
                            '{0}.kmz'.format(join(out_workspace, dsc.name)),
                            boundary_box_extent=extent)

        elif dsc.dataType == 'Layer':
            if dsc.name.endswith('.lyr'):
                name = dsc.name[:-4]
            else:
                name = dsc.name
            arcpy.conversion.LayerToKML(
                                ds,
                                '{0}.kmz'.format(join(out_workspace, name)),
                                boundary_box_extent=extent)

        elif dsc.dataType == 'FeatureDataset':
            arcpy.env.workspace = ds
            for fc in arcpy.ListFeatureClasses():
                arcpy.management.MakeFeatureLayer(fc, 'tmp_lyr')
                arcpy.conversion.LayerToKML(
                                'tmp_lyr',
                                '{0}.kmz'.format(join(out_workspace, fc)),
                                boundary_box_extent=extent)

        elif dsc.dataType == 'CadDrawingDataset':
            arcpy.env.workspace = dsc.catalogPath
            cad_wks_name = os.path.splitext(dsc.name)[0]
            for cad_fc in arcpy.ListFeatureClasses():
                if cad_fc.lower() == 'annotation':
                    cad_anno = arcpy.conversion.ImportCADAnnotation(cad_fc, arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB))
                    arcpy.management.MakeFeatureLayer(cad_anno, 'cad_lyr')
                    name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                    arcpy.conversion.LayerToKML(
                                'cad_lyr',
                                '{0}.kmz'.format(join(out_workspace, name)),
                                boundary_box_extent=extent)
                else:
                    arcpy.management.MakeFeatureLayer(cad_fc, 'cad_lyr')
                    name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                    arcpy.conversion.LayerToKML(
                                'cad_lyr',
                                '{0}.kmz'.format(join(out_workspace, name)),
                                boundary_box_extent=extent)

        # Map document to KML.
        elif dsc.dataType == 'MapDocument':
            mxd = arcpy.mapping.MapDocument(ds)
            data_frames = arcpy.mapping.ListDataFrames(mxd)
            for df in data_frames:
                name = '{0}_{1}'.format(dsc.name[:-4], df)
                arcpy.conversion.MapToKML(
                                ds,
                                df,
                                '{0}.kmz'.format(join(out_workspace, name)),
                                extent_to_export=extent)



if __name__ == '__main__':
    input_datasets = arcpy.GetParameterAsText(0)
    output_location = arcpy.GetParameterAsText(1)
    clip_extent = arcpy.GetParameterAsText(2)
    convert_to_kml(input_datasets, output_location, clip_extent)

