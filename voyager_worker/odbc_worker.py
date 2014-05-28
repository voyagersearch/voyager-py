import decimal
import json
import job

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ODBCJob(job.Job):
    def __repr__(self):
        return "ODBCJob{0}".format(str(self))

    def worker(self):
        """Worker function to index each row in each table in the database."""
        self.db_cursor.execute("select name from sysobjects where type='U'")
        if not self.tables_to_keep == '*':
            tables = [t[0] for t in self.db_cursor.fetchall() if t[0] in self.tables_to_keep]
        else:
            tables = [t[0] for t in self.db_cursor.fetchall()]

        for tbl in tables:
            geo = {}
            has_shape = False
            if not self.fields_to_keep == '*':
                columns = []
                for col in self.fields_to_keep:
                    qry = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                    [columns.append(c[0]) for c in self.execute_query(qry).fetchall()]
            else:
                columns = [c.column_name for c in self.db_cursor.columns(table=tbl).fetchall()]

            if self.fields_to_skip:
                for col in self.fields_to_skip:
                    qry = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                    [columns.remove(c[0]) for c in self.execute_query(qry).fetchall()]

            mapped_cols = self.map_fields(columns)
            for c in self.db_cursor.columns(table=tbl).fetchall():
                if c.type_name == 'geometry':
                    has_shape = True
                    srid = self.db_cursor.execute("select {0}.STSrid from {1}".format(c.column_name, tbl)).fetchone()[0]
                    geo['code'] = srid
                    geom_type = self.db_cursor.execute("select {0}.STGeometryType() from {1}".format(c.column_name, tbl)).fetchone()[0]
                    if geom_type == 'Point':
                        is_point = True
                        columns.insert(0, "{0}.STPointN(1).STX".format(c.column_name))
                        columns.insert(0, "{0}.STPointN(1).STY".format(c.column_name))
                    else:
                        is_point = False
                        columns.insert(0, "{0}.STEnvelope().STPointN((3)).STY".format(c.column_name))
                        columns.insert(0, "{0}.STEnvelope().STPointN((3)).STX".format(c.column_name))
                        columns.insert(0, "{0}.STEnvelope().STPointN((1)).STY".format(c.column_name))
                        columns.insert(0, "{0}.STEnvelope().STPointN((1)).STX".format(c.column_name))
                    break

            self.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))
            for i, row in enumerate(self.db_cursor.fetchall()):
                entry = {}
                if has_shape:
                    if is_point:
                        geo['lon'] = row[1]
                        geo['lat'] = row[0]
                        mapped_cols = dict(zip(mapped_cols, row[2:]))
                    else:
                        geo['xmin'] = row[0]
                        geo['ymin'] = row[1]
                        geo['xmax'] = row[2]
                        geo['ymax'] = row[3]
                        mapped_cols  = dict(zip(mapped_cols, row[4:]))
                else:
                    mapped_cols = dict(zip(mapped_cols, row))
                try:
                    if self.default_mapping:
                        mapped_cols.pop('{0}Shape'.format(self.default_mapping))
                    else:
                        mapped_cols.pop('SHAPE')
                except KeyError:
                        pass
                entry['id'] = '{0}_{1}_{2}'.format(self.location_id, tbl, i)
                entry['location'] = self.location_id
                entry['action'] = self.action_type
                entry['entry'] = {'geo': geo, 'fields': mapped_cols}
                self.send_entry(entry)

    def assign_job(self):
        self.connect_to_zmq()
        self.connect_to_database()
        self.worker()

