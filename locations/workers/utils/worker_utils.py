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
import math
import sys
import ogr

class GeoJSONConverter(object):
    """
    Class with helper methods to convert GeoJSON to WKT.
    """
    def __str__(self):
        return "GeoJSONConverter"

    def convert_to_wkt(self, geojson, number_of_decimals):
        if geojson['type'].upper() == 'POINT':
            wkt = self._point_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'MULTIPOINT':
            wkt = self._multipoint_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'LINESTRING':
            wkt = self._line_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'MULTILINESTRING':
            wkt = self._multiline_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'POLYGON':
            wkt = self._polygon_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'MULTIPOLYGON':
            wkt = self._multipolygon_to_wkt(geojson, number_of_decimals)
        elif geojson['type'].upper() == 'GEOMETRYCOLLECTION':
            wkt = self._geometry_collection_to_wkt(geojson, number_of_decimals)
        else:
            raise Exception('Unknown geometry type.')

        return wkt

    def _point_to_wkt(self, point, decimals=3):
        """Converts a GeoJSON POINT to WKT."""
        x_coord = round(point['coordinates'][0], decimals)
        y_cood = round(point['coordinates'][0], decimals)
        wkt_point = 'POINT ({0} {1})'.format(x_coord, y_cood)
        return wkt_point

    def _multipoint_to_wkt(self, multipoint, decimals=3):
        """Converts a GeoJSON MULTIPOINT to WKT."""
        coords = multipoint['coordinates']
        points = (' '.join(str(round(c, decimals)) for c in pt) for pt in coords)
        points = ('({0})'.format(pt) for pt in points)
        wkt_multipoint = 'MULTIPOINT ({0})'.format(', '.join(points))
        return wkt_multipoint

    def _line_to_wkt(self, polyline, decimals=3):
        """Converts a GeoJSON LINESTRING to WKT."""
        coords = polyline['coordinates']
        wkt_line = 'LINESTRING ({0})'.format(', '.join(' '.join(str(round(c, decimals)) for c in pt) for pt in coords))
        return wkt_line

    def _multiline_to_wkt(self, multiline, decimals=3):
        """Converts a GeoJSON MULTILINESTRING to WKT."""
        coords = multiline['coordinates']
        lines = ('({0})'.format(', '.join(' '.join(str(round(c, decimals)) for c in pt) for pt in coord)) for coord in coords)
        wkt_multilines = 'MULTILINESTRING ({0})'.format(', '.join(ls for ls in lines))
        return wkt_multilines

    def _polygon_to_wkt(self, polygon, decimals=3):
        """Converts a GeoJSON POLYGON to WKT."""
        coords = polygon['coordinates']
        parts = (', '.join(' '.join(str(round(c, decimals)) for c in pt) for pt in part) for part in coords)
        parts = ('({0})'.format(r) for r in parts)
        wkt_polygon = 'POLYGON ({0})'.format(', '.join(parts))
        return wkt_polygon

    def _multipolygon_to_wkt(self, multipolygon, decimals=3):
        """Converts a GeoJSON MULTIPOLYGON to WKT."""
        coords = multipolygon['coordinates']
        polys = (', '.join('({0})'.format(', '.join('({0})'.format(', '.join(' '.join(str(round(c, decimals)) for c in pt) for pt in part)) for part in poly)) for poly in coords))
        wkt_multipolygon = 'MULTIPOLYGON ({0})'.format(polys)
        return wkt_multipolygon

    def _geometry_collection_to_wkt(self, geometrycollection, decimals=3):
        """Converts a GeoJSON GEOMETRYCOLLECTION to WKT."""
        geometries = geometrycollection['geometries']
        wkt_geometries = list()
        for geometry in geometries:
            geometry_type = geometry['type']
            if geometry_type.upper() == 'POINT':
                wkt_geometries.append(self._point_to_wkt(geometry, decimals))
            elif geometry_type.upper() == 'MULTIPOINT':
                wkt_geometries.append(self._multipoint_to_wkt(geometry, decimals))
            elif geometry_type.upper() == 'LINESTRING':
                wkt_geometries.append(self._line_to_wkt(geometry, decimals))
            elif geometry_type.upper() == 'MULTILINESTRING':
                wkt_geometries.append(self._multiline_to_wkt(geometry, decimals))
            elif geometry_type.upper() == 'POLYGON':
                wkt_geometries.append(self._polygon_to_wkt(geometry, decimals))
            elif geometry_type.upper() == 'MULTIPOLYGON':
                wkt_geometries.append(self._multipolygon_to_wkt(geometry, decimals))
            else:
                raise Exception('Unknown geometry type.')

        return 'GEOMETRYCOLLECTION ({0})'.format(wkt_geometries)


class GeometryOps(object):
    """Geometry operators."""
    def __str__(self):
        return "GeometryOps"

    def approximate_radius(self, geometry):
        """Return the approximate radius of a polygon geometry.
        :param geometry: an OGR geometry
        """
        corners = geometry.GetEnvelope()
        centroid = geometry.Centroid()
        corner_point = ogr.CreateGeometryFromWkt('POINT({0} {1})'.format(corners[0], corners[1]))
        return centroid.Distance(corner_point)

    def generalize_geometry(self, wkt, tolerance):
        """Return a generalized geometry by a given tolerance.
        :param wkt: the well-known text string of the geometry to be generalized
        :param tolerance: a simplification tolerance
        """
        try:
            geometry = ogr.CreateGeometryFromWkt(wkt)
            if geometry.GetGeometryName() == 'LINESTRING' and tolerance > 0.9:
                first_point = "{0:.2f} {1:.2f}".format(geometry.GetPoint()[0], geometry.GetPoint()[1])
                mid_point = "{0:.2f} {1:.2f}".format(geometry.Centroid().GetPoint()[0], geometry.Centroid().GetPoint()[1])
                last_point = "{0:.2f} {1:.2f}".format(geometry.GetPoint(geometry.GetPointCount() - 1)[0], geometry.GetPoint(geometry.GetPointCount() - 1)[1])
                return "LINESTRING ({0}, {1}, {2})".format(first_point, mid_point, last_point)
            elif geometry.GetGeometryName() == 'MULTILINESTRING' and tolerance > 0.9:
                gen_geometry = 'MULTILINESTRING ('
                parts = []
                for line in geometry:
                    first_point = "{0:.2f} {1:.2f}".format(line.GetPoint()[0], line.GetPoint()[1])
                    mid_point = "{0:.2f} {1:.2f}".format(line.Centroid().GetPoint()[0], line.Centroid().GetPoint()[1])
                    last_point = "{0:.2f} {1:.2f}".format(line.GetPoint(line.GetPointCount() - 1)[0], line.GetPoint(line.GetPointCount() - 1)[1])
                    parts.append("({0}, {1}, {2})".format(first_point, mid_point, last_point))
                gen_geometry += ",".join(parts)
                gen_geometry += ')'
                return gen_geometry

            else:
                if tolerance > 0.9:
                    gen_geometry = geometry.ConvexHull()
                else:
                    factor = self.approximate_radius(geometry)
                    if not 'LINESTRING' in geometry.GetGeometryName():
                        factor /= max((geometry.GetArea(), 10))
                    else:
                        factor /= 10
                    factor *= math.pow(1 + tolerance, tolerance * 10) - 1
                    gen_geometry = geometry.SimplifyPreserveTopology(factor)
                    wkt = gen_geometry.ExportToWkt()
                    if sys.getsizeof(wkt) > 32766:
                        gen_geometry = geometry.Simplify(factor)
                        wkt = gen_geometry.ExportToWkt()
                    return wkt
        except AttributeError:
            return None
