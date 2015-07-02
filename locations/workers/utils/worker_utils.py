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
import json
import sys
import itertools
import urllib
import ogr


class ArcGISServiceHelper(object):
    """ArcGIS Server and Portal helper class."""
    def __init__(self, portal_url, username, password, referer='', token_expiration=60):
        self._username = username
        self._password = password
        self._portal_url = portal_url
        self._token_expiration = token_expiration
        if referer:
            self._referer = referer
        else:
            self._referer = self._portal_url
        self._service_types = {"Map Service": "MapServer", "Feature Service": "FeatureServer"}
        self._http = "{0}/sharing/rest".format(self._portal_url)
        self.oid_field_name = 'objectid'
        self.token = self._generate_token()

    def _generate_token(self):
        """Generates a token required for ArcGIS Online authentication."""
        try:
            query_dict = {'username': self._username,
                          'password': self._password,
                          'referer': self._referer,
                          'expiration': str(self._token_expiration)}

            query_string = urllib.urlencode(query_dict)
            url = "{0}/sharing/rest/generateToken".format(self._portal_url)
            token = json.loads(urllib.urlopen(url + "?f=json", query_string).read())

            if "token" not in token:
               return None
            return token['token']
        except ValueError:
            return ''

    def find_item_url(self, service_name, service_type, folder_name='', token=''):
        """Return the layer or table view url.
        :param service_name: service name
        :param service_type: service type (i.e Feature Server)
        :param folder_name: folder name service is located in
        :param token: token value
        """
        service_url = None
        if token:
            search_url = self._http + "/search"
            query_dict = {'f': 'json',
                          'token': token,
                          'q': '''title:"{0}" AND owner:"{1}" AND type:"{2}"'''.format(service_name, self._username, service_type)}
            response = urllib.urlopen(search_url, urllib.urlencode(query_dict))
            data = json.loads(response.read())
            if 'results' in data:
                if data['results']:
                    result = data['results'][0]
                    service_url = result['url']
                else:
                    raise IndexError
        else:
            if folder_name:
                search_url = self._portal_url + '/arcgis/rest/services/{0}/{1}/{2}'.format(folder_name, service_name, self._service_types[service_type])
            else:
                search_url = self._portal_url + '/arcgis/rest/services/{0}/{1}'.format(service_name, self._service_types[service_type])

        if not service_url:
            service_url = search_url
        if self.token:
            r = urllib.urlopen(service_url, urllib.urlencode({'f': 'json', 'token': self.token, 'referer': self._referer}))
        else:
            r = urllib.urlopen(service_url, urllib.urlencode({'f': 'json'}))
        items = json.loads(r.read())
        return service_url, items


    def get_item_row_count(self, url, layer_id, token):
        """Returns the row count of a service layer or table.
        :param url: Service url
        :param layer_id: service layer/table ID
        :param token: token value
        """
        if self.token:
            query = {'where': '1=1', 'returnIdsOnly':True, 'token': token, 'f': 'json'}
        else:
            query = {'where': '1=1', 'returnIdsOnly':True, 'f': 'json'}
        response = urllib.urlopen('{0}/{1}/query?'.format(url, layer_id), urllib.urlencode(query))
        data = json.loads(response.read())
        objectids = data['objectIds']
        self.oid_field_name = data['objectIdFieldName']
        if not objectids:
            return None, None
        args = [iter(objectids)] * 100
        id_groups = itertools.izip_longest(fillvalue=None, *args)
        return id_groups, len(objectids)

    def get_item_fields(self, url, layer_id, token):
        """Return the fields of a service layer or table.
        :param url: service url
        :param layer_id: service layer/table ID
        :param token: token value
        """
        if self.token:
            query = {'where': '1=1', 'outFields': '*', 'returnGeomery':False, 'token': token, 'f': 'json'}
        else:
            query = {'where': '1=1', 'outFields': '*', 'returnGeomery':False, 'f': 'json'}
        response = urllib.urlopen('{0}/{1}/query?'.format(url, layer_id), urllib.urlencode(query))
        data = json.loads(response.read())
        fields = data['fields']
        return fields

    def get_item_rows(self, url, layer_id, token, spatial_rel='esriSpatialRelIntersects',
                 where='1=1', out_fields='*', out_sr=4326, return_geometry=True):
        """Return the rows for a service layer or table.
        :param url: service url
        :param layer_id: service layer/table ID
        :param token: token value
        :param spatial_rel: spatial relationship (default is esriSpatialRelIntersects)
        :param where: where clause
        :param out_fields: output fields (default is *)
        :param out_sr: output spatial reference WKID
        :param return_geometry: boolean to return geometry
        """
        if self.token:
            query = {'spatialRel': spatial_rel, 'where': where, 'outFields': out_fields, 'returnGeometry': return_geometry, 'outSR': out_sr, 'token': token, 'f': 'json'}
        else:
            query = {'spatialRel': spatial_rel, 'where': where, 'outFields': out_fields, 'returnGeometry': return_geometry, 'outSR': out_sr, 'f': 'json'}
        response = urllib.urlopen('{0}/{1}/query?'.format(url, layer_id), urllib.urlencode(query))
        data = json.loads(response.read())
        return data


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
    def __init__(self):
        self.__max_precision = 25
        self.__hash_len_to_lat_height = []
        self.__hash_len_to_lon_width = []
        self.__hash_len_to_lat_height.append(90.0*2)
        self.__hash_len_to_lon_width.append(180.0*2)
        even = False
        for i in range(1, self.__max_precision + 1):
            self.__hash_len_to_lat_height.append(self.__hash_len_to_lat_height[-1] / (8 if even else 4))
            self.__hash_len_to_lon_width.append(self.__hash_len_to_lon_width[-1] / (4 if even else 8))
            even = not even

    def __str__(self):
        return "GeometryOps"

    def __approximate_radius(self, geometry):
        """Return the approximate radius of a polygon geometry.
        :param geometry: an OGR geometry
        """
        corners = geometry.GetEnvelope()
        centroid = geometry.Centroid()
        corner_point = ogr.CreateGeometryFromWkt('POINT({0} {1})'.format(corners[0], corners[2]))
        return centroid.Distance(corner_point)

    def __compute_distance(self, geometry, tolerance):
        """Return a distance (in DD) based on
        :param geometry:
        :param tolerance:
        :return:
        """
        radius = self.__approximate_radius(geometry) * 0.1
        level = self.__lookup_hashLen_for_width_height(radius, radius)
        distance = self.__lookup_degrees_size_for_hash_len(level)[1] * tolerance
        return distance

    def __lookup_hashLen_for_width_height(self, lonErr, latErr):
        for i in range(1, self.__max_precision):
            latHeight = self.__hash_len_to_lat_height[i]
            lonWidth = self.__hash_len_to_lon_width[i]
            if latHeight < latErr and lonWidth < lonErr:
                return i
        return self.__max_precision

    def __lookup_degrees_size_for_hash_len(self, hash_length):
        return (self.__hash_len_to_lat_height[hash_length], self.__hash_len_to_lon_width[hash_length])

    def generalize_geometry(self, wkt, tolerance):
        """Return a generalized geometry by a given tolerance.
        :param wkt: the well-known text string of the geometry to be generalized
        :param tolerance: a simplification tolerance
        """
        try:
            # If Polyline and tolerance is 1, just get the first, mid and last points.
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
            elif geometry.GetGeometryName() in ('POLYGON', 'POLYGON Z', 'POLYGON M', 'MULTIPOLYGON M', 'MULTIPOLYGON Z',) and tolerance > 0.9:
                # Get the bbox (extent)
                extent = geometry.GetEnvelope()
                ring = ogr.Geometry(ogr.wkbLinearRing)
                ring.AddPoint(extent[0],extent[2])
                ring.AddPoint(extent[1], extent[2])
                ring.AddPoint(extent[1], extent[3])
                ring.AddPoint(extent[0], extent[3])
                ring.AddPoint(extent[0],extent[2])
                poly = ogr.Geometry(ogr.wkbPolygon)
                poly.AddGeometry(ring)
                return poly.ExportToWkt()
            else:
                # If number points less than threshold, return full WKT.
                threshold = 4 + (1 - tolerance) * 50
                num_points = 0
                for part in geometry:
                    if part.GetGeometryName() == 'LINESTRING':
                        num_points += part.GetPointCount()
                    else:
                        geom_ref = part.GetGeometryRef(0)
                        if geom_ref:
                            num_points += geom_ref.GetPointCount()
                        else:
                            num_points += part.GetPointCount()
                if num_points <= threshold:
                    return geometry.ExportToWkt()

                factor = self.__compute_distance(geometry, tolerance)
                gen_geometry = geometry.SimplifyPreserveTopology(factor)
                wkt = gen_geometry.ExportToWkt()
                if sys.getsizeof(wkt) > 32766:
                    gen_geometry = geometry.Simplify(factor)
                    wkt = gen_geometry.ExportToWkt()
                return wkt
                # ##OLD METHOD##
                # factor = self.__approximate_radius(geometry)
                # if not 'LINESTRING' in geometry.GetGeometryName():
                #     factor /= max((geometry.GetArea(), 10))
                # else:
                #     factor /= 10
                # factor *= math.pow(1 + tolerance, tolerance * 10) - 1
        except AttributeError:
            return None
