#!/usr/bin/env python
import  sys, time, json, urllib2, subprocess, pprint, re, pycurl
from dbs.apis.dbsClient import DbsApi

from optparse import OptionParser
from StringIO import StringIO    
from ast import literal_eval

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
parser.add_option("-s", "--size", dest="size", metavar="NUMBER",
                  help="Projected size. For open datasets use 10000 as a starting value unless you have a better estimate. If nothing is given current size is used.")
parser.add_option("-e", "--era", dest="era", metavar="TEXT",
                  help="Look for all datasets corresponding to a given production Era. Example: 'Run2015C'")
parser.add_option("-i", "--injector", dest="injector", metavar="PATH",
                  default = './assignDatasetToSite.py',
                  help="full path to assignDatasetToSite.py script. Default: %default")
parser.add_option("-n", "--copies", dest="copies", metavar="NUMBER",
                  default = 4,
                  help="Number of copies to subscribe. If zero change the group assignment to AnalysisOps without doing new subscriptions. Default: %default")
parser.add_option("-p","--phedex", dest="phedex", action="store_true", default=False,
                  help="Check PhEDEx for number of copies already assigned under AnalysisOps before trying to inject datasets in DDM")
parser.add_option("--exec", dest="execute", action="store_true", default=False,
                  help="tell injector to execution subscription. By default perform a dry run.")
parser.add_option("--check", dest="check", action="store_true", default=False,
                  help="Just test datasets for subscription consistency without trying to inject anything")
parser.add_option("--log", dest="log", action="store_true", default=False,
                  help="Write a log file. File name will be: [era/run number]-[tiers]-[processing type]-[timestamp].log")

(options, args) = parser.parse_args()

proxyCertificate = "/tmp/x509up_u11792"
capath = "/etc/grid-security/certificates"

if not options.injector:
     print "ERROR: injector path is not set"
     parser.print_help()
     sys.exit(1)

if not options.run and not options.era:
     print "ERROR: need either RUN number or Era name specified"
     parser.print_help()
     sys.exit(1)

if not options.tiers:
     print "ERROR: specify tiers"
     parser.print_help()
     sys.exit(1)

datatiers = None
if options.tiers:
     datatiers = options.tiers.split(',')

logfile = None
if options.log:
     tiers = re.sub(',',"_",options.tiers)
     period = None
     if options.run: period = options.run
     if options.era: period = options.era
     processing_type = "full_check"
     if options.execute:
          processing_type = "execute"
     if options.check:
          processing_type = "simple_check"
     logfile = "%s-%s-%s-%d.log" % (
          period, tiers, processing_type, int(time.time()))
     print "All output is redirected to %s" % logfile
     sys.stdout = open(logfile, 'w')

summary = {"NotInPhedex":[],
           "NoCompleteCopyAnywhere":[],
           "MayGetLost":[],
           "NotFullyInjected":[]}

def form_subscription_report(subscription):
     return "node: %-20s fraction: %-3s%%  custodial: %1s group %-20s" % (
          subscription['node'],subscription['percent_bytes'],subscription['custodial'],subscription['group'])

def get_subscription_information(dataset):
     report = {'nAnalysisOps':0,'nComplete':0,'PhEDEx':False,'nAnalysisOpsComplete':0}
     storage = StringIO()
     curl = pycurl.Curl()
     url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=%s" % dataset
     # print url
     curl.setopt(pycurl.URL, str(url))
     curl.setopt(pycurl.SSL_VERIFYPEER, 1)
     curl.setopt(pycurl.SSL_VERIFYHOST, 2)
     curl.setopt(pycurl.CAINFO, proxyCertificate)
     curl.setopt(pycurl.CAPATH, capath)
     curl.setopt(pycurl.SSLKEY, proxyCertificate)
     curl.setopt(pycurl.SSLCERT, proxyCertificate)
     curl.setopt(curl.WRITEFUNCTION, storage.write)
     curl.perform()
     # make a dictitionary out of the json
     data = json.loads(storage.getvalue())
     # pprint.pprint(data)
     try:
          datasets = data['phedex']['dataset']
          if len(datasets)==0:
               summary["NotInPhedex"].append(dataset)
               print "No information in PhEDEx about this dataset"
               return report
          if len(datasets)!=1:
               raise Exception("Unpexpected number of datasets is returned")
          report['PhEDEx'] = True
          subscriptions = datasets[0]['subscription']
          for subscription in subscriptions:
               incomplete = (subscription['percent_bytes']==None) or (subscription['percent_bytes'] < 100)
               if not incomplete: report['nComplete'] += 1
               if subscription['group']=='AnalysisOps': 
                    report['nAnalysisOps'] += 1
                    if not incomplete: report['nAnalysisOpsComplete'] += 1
               print form_subscription_report(subscription)
     except KeyError:
          pass
     return report

def run_command(command):
     # flash stdout to keep order of messages right
     sys.stdout.flush()
     exit_code = subprocess.call(command, shell=True)
     return exit_code
     

# DBS reader
url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
api = DbsApi(url=url)

datasets = []

if options.run:
     datasets = api.listDatasets(run_num=options.run, detail=True)
if options.era:
     datasets = api.listDatasets(acquisition_era_name=options.era, detail=True)

print "Total number of datasets: %d" % len(datasets)
nDatasetsToCheck = 0
for ds in datasets:
     if datatiers and not ds['data_tier_name'] in datatiers:
          continue
     nDatasetsToCheck += 1
print "Number of datasets to check: %d" % nDatasetsToCheck


for ds in datasets:
     if datatiers and not ds['data_tier_name'] in datatiers:
          continue
     print "\nDatset:",ds['dataset'],
     blocks = api.listBlockSummaries(dataset = ds['dataset'])
     ds_size = blocks[0]['file_size']/pow(2,30)
     print " \t %0.0f GB" % (ds_size)
     if options.size:
          ds_size = options.size

     if options.phedex:
          report = get_subscription_information(ds['dataset'])
          if report['nComplete']==0:
               summary["NoCompleteCopyAnywhere"].append(ds['dataset'])  
          if report['nAnalysisOpsComplete']==0:
               summary["MayGetLost"].append(ds['dataset'])  
          if int(options.copies)>0:
               n = report['nAnalysisOps']
               if n >= int(options.copies):
                    continue
               else:
                    print "Need to inject the dataset in DDM: %d out of %d copies are found" % (n,int(options.copies))
                    summary["NotFullyInjected"].append(ds['dataset'])

     if options.check:
          continue

     command = "%s --dataset=%s --nCopies=%d --expectedSizeGb=%d" % (options.injector,ds['dataset'],int(options.copies),int(ds_size))
     if options.execute:
          command = command + " --exec"
     print command
     if run_command(command):
          print "Command failed. Sleep for 5 mins and retry"
          time.sleep(300)
          if run_command(command):
               print "ERROR: permanent error. Cannot proceed."
               sys.exit(1)
# pprint.pprint(summary)
print "NotInPhedex:"
pprint.pprint(summary["NotInPhedex"])
print "NoCompleteCopyAnywhere:"
pprint.pprint(summary["NoCompleteCopyAnywhere"])
print "MayGetLost:"
pprint.pprint(summary["MayGetLost"])

print "Number of datasets registered in DBS, but missing in PhEDEx: %d" % (len(summary["NotInPhedex"]))
print "Number of datasets without complete copy: %d" % (len(summary["NoCompleteCopyAnywhere"]))
print "Number of datasets that may get lost: %d" % (len(summary["MayGetLost"]))
print "Number of datasets that are not fully injected in DDM:  %d" % (len(summary["NotFullyInjected"]))
