# (C) Copyright 2017 Voyager Search
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
import json
try:
    import arcpy
    import requests
    from bottle import route, run, request, Bottle, response
except ImportError as ie:
    sys.stdout.write(ie.message)
    sys.exit(1)


if 'VOYAGER_META_DIR' in os.environ:
    meta_folder = os.environ['VOYAGER_META_DIR']
else:
    meta_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/meta')

if 'VOYAGER_APPS_DIR' in os.environ:
    app_folder = os.environ['VOYAGER_APPS_DIR']
else:
    app_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

mxd_path = os.path.join(app_folder, 'py/processing/supportfiles/GroupLayerTemplate.mxd')


def create_layer_file(new_entry):
    new_entry = json.loads(new_entry)
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
                pass
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

        return json.dumps(new_entry)


service = Bottle()


@service.route('/createlayerfiles', method='POST')
def createlayerfiles():
    entry = request.body.read()
    return create_layer_file(entry)
