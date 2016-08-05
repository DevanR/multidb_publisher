# -*- coding: utf-8 -*-
#
# License:          This module is released under the terms of the LICENSE file
#                   contained within this applications INSTALL directory

"""
    Abstracts and simplifies publishing reports (and catalogues) from different databases
    with the insights service API
"""

# -- Coding Conventions
# http://www.python.org/dev/peps/pep-0008/   -   Use the Python style guide
# http://sphinx.pocoo.org/rest.html          -   Use Restructured Text for
# docstrings

# -- Public Imports
import os
import yaml
import pytz
from datetime import datetime, timedelta, date

# -- Private Imports
from nsa.hub.api import Insights
from connector import DBConnector

# -- Globals

# -- Exception classes

# -- Functions

# -- Classes


class CatalogueGenerator:
    """
        This class takes in a Database config and catalogue source
        and returns the catalogue with values
    """

    def __init__(self, config):

        self.path = config['path']
        self.dbconnector = DBConnector(config)

        # Load catalogues
        self._files = [f for f in os.listdir(
            self.path) if os.path.isfile(os.path.join(self.path, f))]

    def catalogue(self):
        """
        This function yields a catalogue, complete with its values
        """

        for filename in self._files:
            self.cur_file = filename
            dirname = os.path.dirname(__file__)
            filepath = os.path.join(self.path, filename)
            stream = open(filepath, 'r')
            self.catalogues = yaml.load(stream)

            for cat in self.catalogues:
                # Get data
                cat['values'] = self.dbconnector.fetch(cat['query'])

                # TODO for localhost DB reports only
                # Converting integer to timestamp for Cyclical reports
                if cat['resolution'] == 'hour of day':
                    then = datetime.utcnow() - timedelta(days=1)
                    then = then.replace(
                        hour=0, minute=0, second=0, microsecond=0)
                    for i in range(len(cat['values'])):
                        cat['values'][i]['datetime'] = pytz.UTC.localize(
                            then.replace(hour=i))

                yield cat


class ReportGenerator:
    """
        This class takes in a Database config and catalogue source
        and returns the report with values
    """

    def __init__(self, path):

        self.path = path

        # Load reports
        self._files = [f for f in os.listdir(
            self.path) if os.path.isfile(os.path.join(self.path, f))]

    def report(self):
        """
        This funtion yields a report based in config provided in input/reports/
        """

        for filename in self._files:
            self.cur_file = filename
            dirname = os.path.dirname(__file__)
            filepath = os.path.join(self.path, filename)
            stream = open(filepath, 'r')
            self.reports = yaml.load(stream)

            for report in self.reports:
                yield report


class Publisher:
    """
        This class takes in a catalogue and publishes it
        to the target destination.
    """

    def __init__(self, config):

        self._env = config['env']
        self._username = config['username']
        self._password = config['password']
        self.target = Insights(
            self._username, self._password, environment=self._env)

    def publish_context(self, path):
        """
        This function publishes the report context provided in inputs/report_context/
        """
        stream = open(path, 'r')
        contexts = yaml.load(stream)
        context_dict = {}

        for i in contexts:
            context_dict[i['classification']] = i['contexts']

        cs = self.target.contextsequence

        for c in cs:
            c['contexts'] = context_dict[c['classification']]
            c.save()

    def publish_catalogue(self, meta_catalogue):
        """
        This function publishes a catalogue to the defined target
        """
        type = meta_catalogue['type']
        origin = meta_catalogue['origin']
        metric = meta_catalogue['metric']
        tag = meta_catalogue['tag']
        resolution = meta_catalogue['resolution']
        aggregation = meta_catalogue['aggregation']
        name = meta_catalogue['name']

        # Get catalogue
        cats = self.target.catalogues.get(
            type=type,
            origin=origin,
            metric=metric,
            tag=tag,
            resolution=resolution,
            aggregation=aggregation,
            name=name
        )

        if cats:
            catalogue = cats[0]
        else:
            try:
                # Create Catalogue
                catalogue = self.target.catalogues.create(
                    type=type,
                    origin=origin,
                    metric=metric,
                    tag=tag,
                    resolution=resolution,
                    aggregation=aggregation,
                    name=name
                )

            except Exception as e:
                print "Failed creating catalogue: {}, error: {}".format(meta_catalogue, e)
                pass

        # Clear  and delete Catalogue
        series = catalogue.series.get()
        series.delete()

        try:
            if meta_catalogue['values']:
                series = catalogue.series.create(*meta_catalogue['values'])
        except Exception as e:
            print "Failed creating series: {}, error: {}".format(meta_catalogue, e)
            pass

    def publish_report(self, meta_report):
        """
        This function publishes a report to the defined target
        """

        type = meta_report['type']
        classification = meta_report['classification']
        context = meta_report['context']
        name = meta_report['name']
        description = meta_report['description']
        privilege = meta_report["privilege"]
        extras = meta_report["extras"]

        report = self.target.reports.get(
            type=type,
            classification=classification,
            context=context,
            name=name,
            extras=extras,
            description=description,
            privilege=privilege
        )

        if not report:
            try:
                report = self.target.reports.create(
                    type=type,
                    classification=classification,
                    context=context,
                    name=name,
                    extras=extras,
                    description=description,
                    privilege=privilege
                )
            except Exception as e:
                print "Failed creating report: {} error: {}".format(meta_report['name'], e)
                pass
        else:
            report[0].catalogues = []
            report = report[0]

        cat_to_add = []
        for cat_id in meta_report['catalogue']:
            try:
                catalogue = self.target.catalogues.get(
                    origin=cat_id['origin'],
                    metric=cat_id['metric'],
                    resolution=cat_id['resolution'],
                    aggregation=cat_id['aggregation'],
                    tag=cat_id['tag'],
                )
                cat_to_add = cat_to_add + catalogue
            except Exception as e:
                print "Failed getting catalogue: {} for application report id: {} error: {}".format(cat_id, meta_report['name'], e)
                pass

        # Assign Catalogue to Report
        report.catalogues = cat_to_add
