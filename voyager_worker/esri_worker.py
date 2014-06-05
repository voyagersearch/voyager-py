# (C) Copyright 2014 Voyager Search
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
import arcpy
import job


class EsriJob(job.Job):
    def __repr__(self):
        return "EsriJob{0}".format(str(self))

    def worker(self, data_path):
        geo = {}
        entry = {}
        dsc = arcpy.Describe(data_path)

        if dsc.dataType == 'Table':
            fields = self.search_fields(data_path)
            with arcpy.da.SearchCursor(data_path, fields) as rows:
                for i, row in enumerate(rows, 1):
                    mapped_fields = self.map_fields(dsc.name, fields)
                    mapped_fields = dict(zip(mapped_fields, row))
                    oid_field = filter(lambda x: x in ('FID', 'OID', 'OBJECTID'), rows.fields)
                    if oid_field:
                       fld_index = rows.fields.index(oid_field[0])
                    else:
                        fld_index = i
                    entry['id'] = '{0}_{1}_{2}'.format(self.location_id, os.path.basename(data_path), row[fld_index])
                    entry['location'] = self.location_id
                    entry['action'] = self.action_type
                    entry['entry'] = {'fields': mapped_fields}
                    self.send_entry(entry)
        else:
            sr = arcpy.SpatialReference(4326)
            geo['spatialReference'] = dsc.spatialReference.name
            geo['code'] = dsc.spatialReference.factoryCode
            fields = self.search_fields(dsc.catalogPath)
            if dsc.shapeFieldName in fields:
                fields.remove(dsc.shapeFieldName)
            if dsc.shapeType == 'Point':
                with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@XY'] + fields, '', sr) as rows:
                    for i, row in enumerate(rows):
                        geo['lon'] = row[0][0]
                        geo['lat'] = row[0][1]
                        mapped_fields = self.map_fields(dsc.name, list(rows.fields[1:]))
                        mapped_fields = dict(zip(mapped_fields, row[1:]))
                        entry['id'] = '{0}_{1}_{2}'.format(self.location_id, os.path.basename(data_path), i)
                        entry['location'] = self.location_id
                        entry['action'] = self.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        self.send_entry(entry)
            else:
                with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@'] + fields, '', sr) as rows:
                    for i, row in enumerate(rows):
                        geo['xmin'] = row[0].extent.XMin
                        geo['xmax'] = row[0].extent.XMax
                        geo['ymin'] = row[0].extent.YMin
                        geo['ymax'] = row[0].extent.YMax
                        mapped_fields = self.map_fields(dsc.name, list(rows.fields[1:]))
                        mapped_fields = dict(zip(mapped_fields, row[1:]))
                        entry['id'] = '{0}_{1}_{2}'.format(self.location_id, os.path.basename(data_path), i)
                        entry['location'] = self.location_id
                        entry['action'] = self.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        self.send_entry(entry)

    def search_fields(self, dataset):
        """Returns a valid list of existing fields for the search cursor."""
        fields = []
        if not self.fields_to_keep == ['*']:
            for fld in self.fields_to_keep:
                [fields.append(f.name) for f in arcpy.ListFields(dataset, fld)]
        if self.fields_to_skip:
            for fld in self.fields_to_skip:
                [fields.remove(f.name) for f in arcpy.ListFields(dataset, fld)]
            return fields
        else:
            return [f.name for f in arcpy.ListFields(dataset)]

    def assign_job(self):
        """Determines the data type and each dataset is sent to the worker to be processed."""
        self.connect_to_zmq()
        dsc = arcpy.Describe(self.path)
        if dsc.dataType in ('DbaseTable', 'FeatureClass', 'Shapefile', 'Table'):
            self.worker(self.path)
        elif dsc.dataType == 'Workspace':
            arcpy.env.workspace = self.path
            feature_datasets = arcpy.ListDatasets('*', 'Feature')
            all_tables = []
            if self.tables_to_keep:
                for t in self.tables_to_keep:
                    [all_tables.append(os.path.join(self.path, tbl)) for tbl in arcpy.ListTables(t)]
                    [all_tables.append(os.path.join(self.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                    for fds in feature_datasets:
                        [all_tables.append(os.path.join(self.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]
            else:
                [all_tables.append(os.path.join(self.path, tbl)) for tbl in arcpy.ListTables()]
                [all_tables.append(os.path.join(self.path, fc)) for fc in arcpy.ListFeatureClasses()]
                for fds in feature_datasets:
                    [all_tables.append(os.path.join(self.path, fds, fc)) for fc in arcpy.ListFeatureClasses(feature_dataset=fds)]

            if self.tables_to_skip:
                for t in self.tables_to_keep:
                    [all_tables.remove(os.path.join(self.path, tbl)) for tbl in arcpy.ListTables(t)]
                    [all_tables.remove(os.path.join(self.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                    for fds in feature_datasets:
                        [all_tables.remove(os.path.join(self.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]

            for table in all_tables:
                self.worker(table)
        elif dsc.dataType == 'FeatureDataset' or dsc.dataType == 'CadDrawingDataset':
            arcpy.env.workspace = self.path
            if self.tables_to_keep:
                feature_classes = []
                for tbl in self.tables_to_keep:
                    [feature_classes.append(os.path.join(self.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
            else:
                feature_classes = [os.path.join(self.path, fc) for fc in arcpy.ListFeatureClasses()]
            if self.tables_to_skip:
                for tbl in self.tables_to_skip:
                    [feature_classes.remove(os.path.join(self.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
            for fc in feature_classes:
                self.worker(os.path.join(self.path, fc))
