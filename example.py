
from tornado import web, gen, httpserver, ioloop
from pgar import PGActiveRecord, PGRowset


class Table1(PGActiveRecord):
    table_name = 'table1'


class BaseHandler(web.RequestHandler):
    pass


class MainHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.write('''
        <p><a href="/">Main</a></p>
        <p><a href="/insert">insert data</a></p>
        <p><a href="/listdata">List data</a></p>
        <p><a href="/find">Find by ID</a></p>
        ''')
        self.finish()


class ListHandler(BaseHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        r = PGRowset('table1')
        data = yield r.pg_load()

        self.write('<a href="/">Back to main</a>')
        self.write('<table border="1">')
        self.write('<tr><td>ID</td><td>Value</td></tr>')
        for x in data:
            self.write('<tr><td>%s</td><td>%s</td></tr>' % (x['id'], x['text1']))
        self.write('</table>')
        self.finish()


class InsertHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.write('<form action="/insert" method="post">')
        self.write('Text: <input name="text"> ')
        self.write('<input type="submit" value="Save">')
        self.write('</form>')
        self.finish()

    @gen.coroutine
    def post(self, *args, **kwargs):
        row = Table1()
        row['text1'] = self.get_argument('text', 'there is nothing')
        new_id = yield row.pg_insert()
        self.write('<a href="/">Back to main</a><br><a href="/insert">Insert new text value</a>')
        self.write('<p>Inserted ID: %d</p>' % new_id)
        self.finish()


class FindHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.write('<form action="/find" method="post">')
        self.write('Id: <input name="id"> ')
        self.write('<input type="submit" value="Find">')
        self.write('</form>')
        self.finish()

    @gen.coroutine
    def post(self, *args, **kwargs):
        row = yield Table1.fetch(int(self.get_argument('id', 0)))
        if row:
            self.write('<p>Found value: %s</p>' % row['text1'])
        else:
            self.write('<p>Nothing found</p>')
        self.write('<a href="/">Back to main</a><br><a href="/find">Try find somethong else</a>')
        self.finish()


if __name__ == '__main__':
    try:
        application = web.Application([
            (r'/', MainHandler),
            (r'/listdata', ListHandler),
            (r'/insert', InsertHandler),
            (r'/find', FindHandler),
        ], debug=True)
        http_server = httpserver.HTTPServer(application)
        http_server.listen(8888, 'localhost')
        ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')
