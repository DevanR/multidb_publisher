# -*- coding: utf-8 -*-
#
# License:          This module is released under the terms of the LICENSE file
#                   contained within this applications INSTALL directory

"""
    Abstracts and simplifies publishing reports (and catalogues) from different databases
    with the insights service API
"""

# -- Coding Conventions
#    http://www.python.org/dev/peps/pep-0008/   -   Use the Python style guide
# http://sphinx.pocoo.org/rest.html          -   Use Restructured Text for
# docstrings

# -- Public Imports

# -- Private Imports

# -- Globals

# -- Exception classes

# -- Functions

# -- Classes


class DBConnector:
    """
        This class takes in a Database config and returns wrapper functions to
        submit queries
    """

    def __init__(self, config):

        self._connection = config['connection']
        self._user = config['user']
        self._passwd = config['passwd']
        self.host = config['host']
        self.name = config['name']
        self.port = config['port']
        self.debug = config['debug']
        self._init_queries = config['init_query']

        if self._connection == 'psycopg2':
            import psycopg2
            self._conn = psycopg2.connect("dbname={} user={} host={} password={} port={}".format(
                self.name, self._user, self.host, self._passwd, self.port))
            self._cur = self._conn.cursor()
        if self._connection == 'mysqldb':
            import MySQLdb
            self._conn = MySQLdb.connect(
                self.host, self._user, self._passwd, self.name)
            self._cur = self._conn.cursor()
        if self._connection == 'safactory':
            from nsa.stack.alchemy.bases import *
            from nsa.stack.alchemy import Base, SAFactory
            engine, Session, TXMSession = SAFactory.create_bindings({'username': self._user,
                                                                     'password': self._passwd,
                                                                     'host': self.host,
                                                                     'port': self.port,
                                                                     'name': self.name
                                                                     })
            self._cur = Session()
        if self._connection == 'netezza':
            import nsa.oss.netezza.client as netezza_client
            netezza_client.configuration['netezza'] = {'username': self._user,
                                                       'password': self._passwd,
                                                       'host': self.host,
                                                       'port': self.port,
                                                       'name': self.name
                                                       }
            self._cur = netezza_client.Session()

        # Set the magic path(s)
        if self._init_queries:
            for query in self._init_queries:
                self._cur.execute(query)

    def fetch(self, query):

        values = []

        if self._connection == 'safactory' or self._connection == 'netezza':
            results = self._cur.execute(query)
            records = results.fetchall()
        else:
            self._cur.execute(query)
            records = self._cur.fetchall()

        # Ignore null records and api does not support this
        for r in records:
            if not r[0] or not r[1]:
                pass  # Skip null values as per nsa.hub.api
            else:
                values.append({"datetime": r[0], "value": str(r[1])})

        return values
