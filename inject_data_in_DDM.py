#!/usr/bin/env python
import  sys, time, json, urllib2, subprocess
from dbs.apis.dbsClient import DbsApi

from optparse import OptionParser

description = """
The tool is designed to extract a list of datasets registered in DBS
for a given run. Only datasets that match specified data tiers are
considered. The dataset size is estimated based on all runs registered
in DBS, i.e. it's the total dataset size, not only the input run.
"""

parser = OptionParser(usage = "\n\t%prog [options]", description = description, epilog= ' ')
parser.add_option("-t", "--tier", dest="tiers", metavar="LIST", default = "AOD,MINIAOD",
                  help="comma separated list of data tiers without whitespaces. Default: %default")
parser.add_option("-r", "--run", dest="run", metavar="NUMBER",
                  help="run number")
parser.add_option("-s", "--size", dest="size", metavar="NUMBER", default=10000,
                  help="Projected size. Default: %default GB")
parser.add_option("-p", "--pattern", dest="pattern", metavar="TEXT",
                  help="Dataset pattern. Example: '/*/Run2015*-PromptReco-v*/AOD'")
parser.add_option("-i", "--injector", dest="injector", metavar="PATH",
                  default = '/afs/cern.ch/user/d/dmytro/DDM/IntelROCCS/DataDealer/assignDatasetToSite.py',
                  help="full path to assignDatasetToSite.py script. Default: %default")
parser.add_option("-n", "--copies", dest="copies", metavar="NUMBER",
                  default = 4,
                  help="Number of copies to subscribe. Default: %default")
parser.add_option("--exec", dest="execute", action="store_true", default=False,
                  help="tell injector to execution subscription. By default perform a dry run.")

(options, args) = parser.parse_args()

if not options.injector:
     print "ERROR: injector path is not set"
     parser.print_help()
     sys.exit(1)

if (not options.run or not options.tiers) and not options.pattern:
     parser.print_help()
     sys.exit(1)

if options.pattern:
     print "ERROR: not implemented yet"
     sys.exit(1)

datatiers = None
if options.tiers:
     datatiers = options.tiers.split(',')

# DBS reader
url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
api = DbsApi(url=url)

datasets = api.listDatasets(run_num=options.run, detail=True)
print "Total number of datasets associated with the run: %d" % len(datasets)

for ds in datasets:
     if datatiers and not ds['data_tier_name'] in datatiers:
          continue
     print "\nDatset:",ds['dataset'],
     blocks = api.listBlockSummaries(dataset = ds['dataset'])
     ds_size = blocks[0]['file_size']/pow(2,30)
     print " \t %0.0f GB" % (ds_size)
     if options.size:
          ds_size = options.size
     
     command = "%s --dataset=%s --nCopies=%d --expectedSizeGb=%d" % (options.injector,ds['dataset'],options.copies,int(ds_size))
     if options.execute:
          command = command + " --exec"
     print command
     subprocess.call(command, shell=True)
