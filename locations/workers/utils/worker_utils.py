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
