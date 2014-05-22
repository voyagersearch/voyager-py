import decimal
import json
import pyodbc

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def database_extractor(self):
    conn = pyodbc.connect(self.path)
    cursor = conn.cursor()
    cursor.execute("select name from sysobjects where type='U'")
    tables = [t[0] for t in cursor.fetchall()]
    for tbl in tables:
        geo = {}
        entry = {}
        has_shape = False
        cols = [c.column_name for c in cursor.columns(table=tbl).fetchall()]
        for c in cursor.columns(table=tbl).fetchall():
            if c.type_name == 'geometry':
                geom_type = cursor.execute("select '{0}.STGeomtryType()".format(c.column_name)).fetchone()[0]
                if geom_type == 'POINT':
                    cursor.execute("select {0}.STPointN(1).STX, Shape.STPointN(1).STY from {1}".format(c.column_name, tbl))
                    pt = cursor.fetchone()
                    geo['lon'] = pt[0]
                    geo['lat'] = pt[1]
                else:
                    cursor.execute("select {0}.STEnvelope().ToString() from {1}".format(c.column_name, tbl))
                    geom = cursor.fetchone()
                    geo['XMin'] = geom[0]
                break

        #cursor.execute("select * from {0}".format(tbl))
        #cols = [t[0] for t in cursor.description]
        if not self.keep_fields == '*':
            cols = ','.join(set(cols).intersection(set(self.keep_fields)))
        else:
            cols = '*'
        cursor.execute("select {0} from {1}".format(cols, tbl))
        cols = [t[0] for t in cursor.description]
        mapped_cols = self.map_fields(cols)
        for row in cursor.fetchall():
            if has_shape:
                sr_id = cursor.execute()
            mapped_cols = dict(zip(mapped_cols, row))
            try:
                if self.default_map:
                    mapped_cols.pop('{0}_Shape'.format(self.default_map))
                else:
                    mapped_cols.pop('SHAPE')
            except KeyError:
                    pass
            self._entry = mapped_cols


def assign_job(job):
    pass