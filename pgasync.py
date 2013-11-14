
import momoko
from tornado import gen
from settings import settings


def yieldme(fn):
    def wrapper(*args, **kwargs):
        return gen.Task(fn, *args, **kwargs)
    return wrapper


class PgConnPool(object):
    """PostgreSQL connection pool"""
    pool = momoko.Pool(dsn=settings['database'].get('dsn'), size=settings['database'].get('pool_size', 2))
    
    def __get__(self, obj, owner):
        return self.pool


class PostgresqlMixin:
    db = PgDatabase()


class ActiveRecord(dict, PostgresqlMixin):
    _pkey  = 'id'
    _table = ''

    def fetch_row(self, c):
        r = c.fetchone()
        if r:
            colms = [d[0] for d in c.description]
            row = {}
            for i, column in enumerate(colms):
                
                try_decode = lambda _v: _v.decode('utf-8') if isinstance(_v, str) else _v
                
                if isinstance(r[i], list):
                    row[column] = []
                    for x in r[i]:
                        row[column].append(try_decode(x))
                else:
                    row[column] = try_decode(r[i])
                
            return row
        
        return None
    
    def _data_callback_fetch(self, c, e, callback):
        row = self.fetch_row(c)
        print c.query
        if row:
            for c,v in row.iteritems():
                print c,v
                self[c] = v
            callback(self)
        else:
            callback(None)
        
    def _data_callback_modify(self, c, e, callback):
        '''After insert return inserted id or count query'''
        print c.query
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
            
        sql = 'INSERT INTO %s (%s) VALUES (%s) RETURNING %s' % (self._table, ', '.join(f), ', '.join(v), self._pkey)
        self.db.execute(sql, values, callback=lambda x,e:self._data_callback_modify(x,e,callback))
    
    def _impl_update(self, callback):
        pairs = []
        values = []
        for k in self.keys():
            pairs.append('%s = %%s' % k)
            values.append(self[k])
        values.append(self[self._pkey])
        
        sql = 'UPDATE %s SET %s WHERE %s = %%s' % (self._table, ', '.join(pairs), self._pkey)
        self.db.execute(sql, values, callback=lambda x,e:self._data_callback_modify(x,e,callback))
    
    @classmethod
    @yieldme
    def fetch(klass, _id, **kwargs):
        callback = kwargs.pop('callback')
        _self = klass()
        _self.db.execute('SELECT * FROM %s WHERE id = %%s' % _self._table, [_id], callback=lambda x,e:_self._data_callback_fetch(x,e,callback))
    
    @classmethod
    @yieldme
    def find_one(klass, condition, **kwargs):
        callback = kwargs.pop('callback')
        _self = klass()
        _self.db.execute('SELECT * FROM %s WHERE %s LIMIT 1' % (_self._table, condition), [], callback=lambda x,e:_self._data_callback_fetch(x,e,callback))

    @yieldme
    def save(self, **kwargs):
        callback = kwargs.pop('callback')
        
        if self._pkey in self.keys() and self[self._pkey]:
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
        sql = 'DELETE FROM %s WHERE %s = %%s' % (self._table, self._pkey)
        self.db.execute(sql, [_id], callback=lambda x,e:_self._data_callback_modify(x,e,callback))
    
    @classmethod
    @yieldme
    def pg_rows_count(klass, condition = None, **kwargs):
        callback = kwargs.pop('callback')
        sql = 'SELECT COUNT(*) FROM %s' % klass._table
        if condition:
            sql = sql + ' WHERE ' + condition
        _self = klass()
        _self.db.execute(sql, [], callback=lambda x,e:_self._data_callback_modify(x,e,callback))


class Rowset(list, PostgresqlMixin):
    def __init__(self, table, **kwargs):
        self._table  = table
        self._fields = kwargs.get('fields', ['*'])
        self._maxitems = kwargs.get('maxitems')
        self._offset = kwargs.get('offset', 0)
        self._condition = kwargs.get('condition', '')
        self._orderby = kwargs.get('orderby', '')
    
    def add_condition(self, _cond, _clear = False):
        if _clear:
            self._condition = ''
        
        self._condition = self._condition + _cond
    
    def set_maxitems(self, _max):
        self._maxitems = _max
        
    def set_offset(self, _offset):
        self._offset = _offset
    
    @yieldme
    def pg_load(self, **kw):
        callback = kw.pop('callback')
        sql = ['SELECT', ', '.join(self._fields), 'FROM', self._table]
        
        if len(self._condition) > 0:
            sql.append('WHERE')
            sql.append(self._condition)
        
        if self._maxitems:
            sql.append('LIMIT')
            sql.append(self._maxitems)
            
        if self._offset:
            sql.append('OFFSET')
            sql.append(self._offset)
            
        if len(self._orderby) > 0:
            sql.append('ORDER BY')
            sql.append(self._orderby)
        
        self.db.execute(' '.join(sql), [], callback=lambda x,e:self._data_callback_load(x, e, callback))
        
    def _data_callback_load(self, c, e, callback):
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
