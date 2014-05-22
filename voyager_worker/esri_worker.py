import os
import sys
import json
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
                mapped_fields = self.map_fields(fields)
                for i, row in enumerate(rows, 1):
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
            if dsc.shapeType == 'Point':
                with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@XY'] + fields, '', sr) as rows:
                    for i, row in enumerate(rows):
                        geo['lon'] = row[0][0]
                        geo['lat'] = row[0][1]
                        mapped_fields = self.map_fields(list(rows.fields))
                        mapped_fields = dict(zip(mapped_fields, row))
                        try:
                            if self.default_mapping:
                                mapped_fields.pop('{0}SHAPE@XY'.format(self.default_mapping))
                                mapped_fields.pop('{0}Shape'.format(self.default_mapping))
                            else:
                                mapped_fields.pop('SHAPEXY')
                                mapped_fields.pop('SHAPE')
                        except KeyError:
                                pass
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
                        mapped_fields = self.map_fields(list(rows.fields))
                        mapped_fields = dict(zip(mapped_fields, row))
                        try:
                            if self.default_mapping:
                                mapped_fields.pop('{0}SHAPE@'.format(self.default_mapping))
                                mapped_fields.pop('{0}Shape'.format(self.default_mapping))
                            else:
                                mapped_fields.pop('SHAPE@')
                                mapped_fields.pop('SHAPE')
                        except KeyError:
                                pass
                        entry['id'] = '{0}_{1}_{2}'.format(self.location_id, os.path.basename(data_path), i)
                        entry['location'] = self.location_id
                        entry['action'] = self.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        self.send_entry(entry)

    def search_fields(self, dataset):
        """Returns a valid list of existing fields for the search cursor."""
        #all_fields = set([f.name for f in arcpy.ListFields(dataset)])
        fields = []
        if not self.fields_to_keep == '*':
            for fld in self.fields_to_keep:
                [fields.append(f.name) for f in arcpy.ListFields(dataset, fld)]
        if self.fields_to_skip:
            for fld in self.fields_to_skip:
                [fields.remove(f.name) for f in arcpy.ListFields(dataset, fld)]
            return fields
        else:
            return [f.name for f in arcpy.ListFields(dataset)]

    def assign_job(self):
        dsc = arcpy.Describe(self.path)
        if dsc.dataType in ('DbaseTable', 'FeatureClass', 'Shapefile', 'Table'):
            self.worker(self.path)
        elif dsc.dataType == 'Workspace':
            walker = arcpy.da.Walk(self.path)
            for root, fds, tables in walker:
                for table in tables:
                    self.worker(os.path.join(root, table))
        elif dsc.dataType == 'FeatureDataset' or dsc.dataType == 'CadDrawingDataset':
            arcpy.env.workspace = self.path
            feature_classes = arcpy.ListFeatureClasses()
            for fc in feature_classes:
                self.worker(os.path.join(self.path, fc))