"""---------------------------------------------------------------------------
Name:        addlocations.py
Purpose:     Adds feature locations in WGS84 to new fields.
Author:      NewfoundGEO Consulting
Created:     01/06/2013
Updated:     01/06/2013
---------------------------------------------------------------------------"""
import os
import arcpy

arcpy.env.overwriteOutput = True

def create_wkt(lower_left, upper_left, upper_right, lower_right, spatial_ref):
    """Returns well-known text for an extent of a feature."""
    pt_array = arcpy.Array([lower_left, upper_left, upper_right, lower_right])
    extent_poly = arcpy.Polygon(pt_array, spatial_ref)
    return extent_poly.WKT
# End create_wkt function

def add_locations(in_features):
    """Updates an input feature class or layer with X, Y coordinates
    in GCS WGS 84 for points. For lines and polygons, the extent in
    GCS WGS 84 for each feature is added to a field in well-known text format.
    """
    sr = arcpy.SpatialReference(4326)
    dsc = arcpy.Describe(in_features)
    if dsc.shapeType == 'Point':
        fields = ['X_WGS84', 'Y_WGS84']
        for field in fields:
            arcpy.management.AddField(in_features, field, 'DOUBLE')
        fields.insert(0, 'shape@xy')
        with arcpy.da.UpdateCursor(in_features, fields, spatial_reference=sr) as rows:
            for row in rows:
                row[1] = row[0][0]
                row[2] = row[0][1]
                rows.updateRow(row)
    elif dsc.shapeType == 'Multipoint':
        arcpy.management.AddField(in_features, 'MP_WKT', 'TEXT', field_length=355)
        with arcpy.da.UpdateCursor(in_features, ['shape@', 'MP_WKT']) as rows:
            for row in rows:
                row[1] = row[0].WKT
                rows.updateRow(row)
    else:
        arcpy.management.AddField(in_features, 'EXTENT_WKT', 'TEXT')
        with arcpy.da.UpdateCursor(in_features, ['shape@', 'EXTENT_WKT'], spatial_reference=sr) as rows:
            for row in rows:
                ext = row[0].extent
                row[1] = create_wkt(ext.lowerLeft,
                                    ext.upperLeft,
                                    ext.upperRight,
                                    ext.lowerRight,
                                    sr)
                rows.updateRow(row)

def main(input_workspace):
    arcpy.env.workspace = input_workspace

    # Stand-alone feature classes
    feature_classes = arcpy.ListFeatureClasses()
    if feature_classes:
        increment = 1
        arcpy.SetProgressor('step',
                            'Processing stand-alone feature classes...',
                            0,
                            len(feature_classes),
                            increment)
        for fc in feature_classes:
            try:
                add_locations(fc)
                arcpy.SetProgressorPosition()
            except Exception:
                pass
        arcpy.SetProgressorPosition(len(feature_classes))

    # Feature classes in feature datasets
    feature_datasets = arcpy.ListDatasets('', 'Feature')
    if feature_datasets:
        increment = 1
        arcpy.SetProgressor('step',
                            'Processing feature datasets...',
                            0,
                            len(feature_classes),
                            increment)
        for fds in feature_datasets:
            arcpy.env.workspace = os.path.join(input_workspace, fds)
            feature_classes = arcpy.ListFeatureClasses()
            for fc in feature_classes:
                add_locations(fc)
            arcpy.SetProgressorPosition()
        arcpy.SetProgressorPosition(len(feature_datasets))

if __name__ == '__main__':
    main(arcpy.GetParameterAsText(0))
