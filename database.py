
import momoko
from settings import settings


class PgDatabase(object):
    pool = momoko.Pool(dsn=settings['database'], size=2)

    def __init__(self):
        if __debug__:
            print 'PostgreSQL connected'

    def __get__(self, obj, owner):
        return self.pool


class DatabaseMixin:
    db = PgDatabase()

    @classmethod
    def fetch_row(cls, c):
        colms = [d[0] for d in c.description]
        r = c.fetchone()
        if r:
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
