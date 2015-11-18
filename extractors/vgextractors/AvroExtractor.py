import os
import datetime
import fastavro
from _vgdexfield import VgDexField


class AvroExtractor(object):

    @staticmethod
    def format_date(date_object):
        # Format date to iso 8601
        if isinstance(date_object, str):
            dt = datetime.datetime.strptime(date_object, "%Y%m%d%H%M%S")
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
        else:
            try:
                if date_object.tzinfo:
                    return date_object.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
                else:
                    return date_object.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                # For dates before 1900.
                date_str = datetime.datetime.isoformat(date_object) + 'Z'
                return date_str
            except Exception:
                return list(date_object.timetuple())[0:6]

    @staticmethod
    def extractor():
        return "avro"

    def get_info(self):
        return {'name': AvroExtractor.extractor(), 'description': 'extract avro file information'}

    def extract(self, infile, job):
        minx, miny, maxx, maxy = (None, None, None, None)
        poly_wkt = None
        job.set_field(VgDexField.NAME, os.path.basename(infile))
        job.set_field(VgDexField.PATH, infile)
        with open(infile, 'rb') as avro_file:
            reader = fastavro.reader(avro_file)
            for record in reader:
                for k, v in record.iteritems():
                    if k.lower() == 'footprint_geometry':
                        poly_wkt = v
                        job.set_field(VgDexField.GEO_WKT, poly_wkt)
                        if job.get(VgDexField.GEO):
                            job.geo['wkt'] = poly_wkt
                        job.set_field(VgDexField.GEO, job.get_field(VgDexField.GEO_WKT))
                    else:
                        if k == 'MBR_EAST' and v:
                            minx = float(v)
                        elif k == 'MBR_WEST' and v:
                            maxx = float(v)
                        elif k == 'MBR_NORTH' and v:
                            maxy = float(v)
                        elif k == 'MBR_SOUTH' and v:
                            miny = float(v)

                    # Map values to correct data type.
                    if isinstance(v, str):
                        job.set_field("fs_{0}".format(k), v)
                    elif isinstance(v, unicode):
                        job.set_field("fs_{0}".format(k), v)
                    elif isinstance(v, bool):
                        job.set_field("fs_{0}".format(k), v)
                    elif isinstance(v, int):
                        job.set_field("fl_{0}".format(k), v)
                    elif isinstance(v, float):
                        job.set_field("fu_{0}".format(k), v)
                    elif isinstance(v, datetime.datetime):
                        job.set_field("fd_{0}".format(k), self.format_date(v))
                    elif (v and "Date" in k) or (isinstance(v, unicode) and len(v.strip()) == 14):
                        job.set_field("fd_{0}".format(k), self.format_date(v))
                    elif isinstance(v, list):
                        job.set_field("fs_{0}".format(k), v)
                    else:
                        job.set_field("meta_{0}".format(k), v)

            if minx and not poly_wkt:
                poly_wkt = "POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(minx, miny, maxx, maxy)
                job.set_field(VgDexField.GEO_WKT, poly_wkt)
                if job.get(VgDexField.GEO):
                    job.geo['wkt'] = poly_wkt
                job.set_field(VgDexField.GEO, job.get_field(VgDexField.GEO_WKT))
