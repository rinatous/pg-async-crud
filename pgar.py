
from tornado import gen
from database import DatabaseMixin


def yieldme(fn):
    def wrapper(*args, **kwargs):
        return gen.Task(fn, *args, **kwargs)
    return wrapper


class PGActiveRecord(dict, DatabaseMixin):
    table_name = ''
    primary_key = 'id'

    def data_callback_fetch(self, c, e, callback):
        if e:
            raise e

        row = self.fetch_row(c)
        if row:
            for k, v in row.iteritems():
                self[k] = v
            callback(self)
        else:
            callback(None)

    def data_callback_modify(self, c, e, callback):
        """After insert return inserted id or count query"""
        if e:
            raise e

        if c.description:
            row = c.fetchone()
            callback(row[0])
        else:
            callback(None)

    def _impl_insert(self, callback):
        f = []
        v = []
        values = []
        for k in self.keys():
            f.append(k)
            v.append('%s')
            values.append(self[k])

        sql = 'INSERT INTO %s (%s) VALUES (%s) RETURNING %s' % (
            self.table_name,
            ', '.join(f),
            ', '.join(v),
            self.primary_key
        )
        self.db.execute(sql, values, callback=lambda x, e: self.data_callback_modify(x, e, callback))

    def _impl_update(self, callback):
        pairs = []
        values = []
        for k in self.keys():
            pairs.append('%s = %%s' % k)
            values.append(self[k])
        values.append(self[self.primary_key])

        sql = 'UPDATE %s SET %s WHERE %s = %%s' % (self.table_name, ', '.join(pairs), self.primary_key)
        self.db.execute(sql, values, callback=lambda x, e: self.data_callback_modify(x, e, callback))

    @classmethod
    @yieldme
    def fetch(cls, _id, **kwargs):
        callback = kwargs.pop('callback')
        _self = cls()
        _self.db.execute(
            'SELECT * FROM %s WHERE id = %%s' % _self.table_name, [_id],
            callback=lambda x, e: _self.data_callback_fetch(x, e, callback)
        )

    @classmethod
    @yieldme
    def find_one(cls, condition, condvars=[], **kwargs):
        callback = kwargs.pop('callback')
        _self = cls()
        _self.db.execute(
            'SELECT * FROM %s WHERE %s LIMIT 1' % (_self.table_name, condition), condvars,
            callback=lambda x, e: _self.data_callback_fetch(x, e, callback)
        )

    @yieldme
    def save(self, **kwargs):
        callback = kwargs.pop('callback')

        if self.primary_key in self.keys() and self[self.primary_key]:
            self._impl_update(callback)
        else:
            self._impl_insert(callback)

    @yieldme
    def pg_insert(self, **kwargs):
        callback = kwargs.pop('callback')
        self._impl_insert(callback)

    @yieldme
    def pg_update(self, **kwargs):
        callback = kwargs.pop('callback')
        self._impl_update(callback)

    @yieldme
    def pg_delete(self, _id, **kwargs):
        callback = kwargs.pop('callback')
        sql = 'DELETE FROM %s WHERE %s = %%s' % (self.table_name, self.primary_key)
        self.db.execute(sql, [_id], callback=lambda x, e: self.data_callback_modify(x, e, callback))

    @classmethod
    @yieldme
    def pg_del_condition(cls, condition=None, condvars=[], **kwargs):
        callback = kwargs.pop('callback')
        sql = 'DELETE FROM %s' % cls.table_name
        if condition:
            sql = sql + ' WHERE ' + condition
        _self = cls()
        _self.db.execute(sql, condvars, callback=lambda x, e: _self.data_callback_modify(x, e, callback))

    @classmethod
    @yieldme
    def pg_rows_count(cls, condition=None, condvars=[], **kwargs):
        callback = kwargs.pop('callback')
        sql = 'SELECT COUNT(*) FROM %s' % cls.table_name
        if condition:
            sql = sql + ' WHERE ' + condition
        _self = cls()
        _self.db.execute(sql, condvars, callback=lambda x, e: _self.data_callback_modify(x, e, callback))


class PGRowset(list, DatabaseMixin):

    def __init__(self, table, **kwargs):
        self.table_name = table
        self._fields = kwargs.get('fields', ['*'])
        self._maxitems = kwargs.get('maxitems')
        self._offset = kwargs.get('offset', 0)
        self._condition = kwargs.get('condition', '')
        self._condvars = kwargs.get('condvars', [])
        self._orderby = kwargs.get('orderby', '')

    def set(self, **kwargs):
        k = kwargs.keys()
        if 'fields' in k:
            self._fields = kwargs['fields']
        if 'maxitems' in k:
            self._maxitems = kwargs['maxitems']
        if 'offset' in k:
            self._offset = kwargs['offset']
        if 'condition' in k:
            self._condition = kwargs['condition']
        if 'condvars' in k:
            self._condvars = kwargs['condvars']
        if 'orderby' in k:
            self._orderby = kwargs['orderby']

    @yieldme
    def pg_load(self, **kw):
        callback = kw.pop('callback')
        sql = ['SELECT', ', '.join(self._fields), 'FROM', self.table_name]

        if len(self._condition) > 0:
            sql.append('WHERE')
            sql.append(self._condition)

        if len(self._orderby) > 0:
            sql.append('ORDER BY')
            sql.append(self._orderby)

        if self._maxitems:
            sql.append('LIMIT')
            sql.append(str(self._maxitems))

        if self._offset:
            sql.append('OFFSET')
            sql.append(str(self._offset))
        self.db.execute(' '.join(sql), self._condvars, callback=lambda x, e: self.data_callback_load(x, e, callback))

    def data_callback_load(self, c, e, callback):
        if e:
            raise e

        colms = [d[0] for d in c.description]

        r = c.fetchone()
        while r:
            row = {}
            for i, column in enumerate(colms):
                try_decode = lambda _v: _v.decode('utf-8') if isinstance(_v, str) else _v

                if isinstance(r[i], list):
                    row[column] = []
                    for x in r[i]:
                        row[column].append(try_decode(x))
                else:
                    row[column] = try_decode(r[i])

            self.append(row)
            r = c.fetchone()

        callback(self)
