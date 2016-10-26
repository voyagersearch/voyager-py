import os
import sys
import json


def run(entry):
    """
    Creates a layer file (.lyr) in the Voyager meta location for a GIS item that currently has no associated layer file.
    Currently has support for Shapefiles, raster datasets and geodatbase feature classes.
    :param entry: a JSON file containing a voyager entry.
    """
    try:
        import arcpy
    except ImportError as ie:
        sys.stdout.write(ie.message)
        sys.exit(1)
    meta_folder = 'c:/voyager/data/meta'
    vmoptions = os.path.join(os.path.abspath(os.path.join(__file__, "../../../..")), 'Voyager.vmoptions')
    mxd_path = os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), 'processing/supportfiles/GroupLayerTemplate.mxd') #os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'GroupLayerTemplate.mxd')
    with open(vmoptions, 'rb') as fp:
        for i, line in enumerate(fp):
            if line.startswith('-Ddata.dir'):
                data_path = line.split('=')[1]
                meta_folder = os.path.normpath('{0}\meta'.format(data_path.strip()))
                break
    new_entry = json.load(open(entry, "rb"))
    if 'job' in new_entry and 'id' in new_entry['entry']['fields'] and 'path' in new_entry['job']:
        path = new_entry['job']['path']
        id = new_entry['entry']['fields']['id']
        dsc = arcpy.Describe(path)
        layer_folder = os.path.join(meta_folder, id[0], id[1:4])
        if not os.path.exists(layer_folder):
            os.makedirs(layer_folder)
            try:
                if dsc.dataType in ('FeatureClass', 'Shapefile', 'ShapeFile'):
                    feature_layer = arcpy.MakeFeatureLayer_management(path, os.path.basename(path))
                    arcpy.SaveToLayerFile_management(feature_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    new_entry['entry']['fields']['hasLayerFile'] = True
                    new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                elif dsc.dataType == 'RasterDataset':
                    raster_layer = arcpy.MakeRasterLayer_management(path, os.path.basename(path))
                    arcpy.SaveToLayerFile_management(raster_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    new_entry['entry']['fields']['hasLayerFile'] = True
                    new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                elif dsc.dataType in ('CadDrawingDataset', 'FeatureDataset'):
                    arcpy.env.workspace = path
                    lyr_mxd = arcpy.mapping.MapDocument(mxd_path)
                    data_frame = arcpy.mapping.ListDataFrames(lyr_mxd)[0]
                    group_layer = arcpy.mapping.ListLayers(lyr_mxd, 'Group Layer', data_frame)[0]
                    for fc in arcpy.ListFeatureClasses():
                        dataset_name = os.path.splitext(os.path.basename(path))[0]
                        l = arcpy.MakeFeatureLayer_management(fc,
                                                              '{0}_{1}'.format(dataset_name, os.path.basename(fc)))
                        arcpy.mapping.AddLayerToGroup(data_frame, group_layer, l.getOutput(0))
                    arcpy.ResetEnvironments()
                    group_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    new_entry['entry']['fields']['hasLayerFile'] = True
                    new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                else:
                    return
            except arcpy.ExecuteError:
                fp.write(arcpy.GetMessages(2))
        else:
            # Does the layer already exist?
            if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(id))):
                try:
                    if dsc.dataType in ('FeatureClass', 'Shapefile', 'ShapeFile'):
                        feature_layer = arcpy.MakeFeatureLayer_management(path, os.path.basename(path))
                        arcpy.SaveToLayerFile_management(feature_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                        new_entry['entry']['fields']['hasLayerFile'] = True
                        new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                    elif dsc.dataType == 'RasterDataset':
                        raster_layer = arcpy.MakeRasterLayer_management(path, os.path.basename(path))
                        arcpy.SaveToLayerFile_management(raster_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                        new_entry['entry']['fields']['hasLayerFile'] = True
                        new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                    elif dsc.dataType in ('CadDrawingDataset', 'FeatureDataset'):
                        arcpy.env.workspace = path
                        lyr_mxd = arcpy.mapping.MapDocument(mxd_path)
                        data_frame = arcpy.mapping.ListDataFrames(lyr_mxd)[0]
                        group_layer = arcpy.mapping.ListLayers(lyr_mxd, 'Group Layer', data_frame)[0]
                        for fc in arcpy.ListFeatureClasses():
                            dataset_name = os.path.splitext(os.path.basename(path))[0]
                            l = arcpy.MakeFeatureLayer_management(fc,
                                                                  '{0}_{1}'.format(dataset_name, os.path.basename(fc)))
                            arcpy.mapping.AddLayerToGroup(data_frame, group_layer, l.getOutput(0))
                        arcpy.ResetEnvironments()
                        group_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                        new_entry['entry']['fields']['hasLayerFile'] = True
                        new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)
                    else:
                        return
                except arcpy.ExecuteError:
                    pass

    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()
