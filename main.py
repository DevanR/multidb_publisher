import os
import yaml
import multiprocessing

from api import CatalogueGenerator, ReportGenerator, Publisher

with open("configs/database.local.yaml", 'r') as ymlfile:
    db_config = yaml.load(ymlfile)

with open("configs/target.local.yaml", 'r') as ymlfile:
    tgt_config = yaml.load(ymlfile)

insights_app = Publisher(tgt_config['insights'])
rep_gen= ReportGenerator("inputs/reports/")

def publish_catalogues(cat_gen):
    for catalogue in cat_gen.catalogue():
        print "Publishing catalogue {}:{}".format(cat_gen.cur_file, catalogue['id'])
        insights_app.publish_catalogue(catalogue)

def publish_reports(rep_gen):
    for report in rep_gen.report():
        print "Publishing report {}:{}".format(rep_gen.cur_file, report['name'])
        insights_app.publish_report(report)


if __name__ == '__main__':
    jobs = []

    # Publish all catalogues
    for db in db_config:
        cat_gen = CatalogueGenerator(db_config[str(db)])
        p = multiprocessing.Process(target=publish_catalogues, args=(cat_gen,))
        jobs.append(p)
        p.start()

    # Wait for all catalogue processes to finish
    for p in jobs:
        p.join()

    # Publish all reports
    publish_reports(rep_gen)
