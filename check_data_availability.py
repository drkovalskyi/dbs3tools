#!/usr/bin/env python
import  sys, time, json, urllib2, subprocess, pprint, re, pycurl
from dbs.apis.dbsClient import DbsApi

from optparse import OptionParser
from StringIO import StringIO    
from ast import literal_eval

description = """
The tool is designed to find datasets in valid state in DBS that 
are effectively not available according to the subscription 
information in PhEDEx.
"""

parser = OptionParser(usage = "\n\t%prog [options]", description = description, epilog= ' ')
parser.add_option("-t", "--tier", dest="tiers", metavar="LIST",
                  help="comma separated list of data tiers without whitespaces.")
parser.add_option("-r", "--run", dest="run", metavar="NUMBER",
                  help="run number")
parser.add_option("-e", "--era", dest="era", metavar="TEXT",
                  help="Look for all datasets corresponding to a given production Era. Example: 'Run2015C'")
parser.add_option("-i", "--ignore", dest="ignore", metavar="NUMBER",
                  default = 7,
                  help="Ignore datasets that had the first subscription request with last N days. Default: %default") 
parser.add_option("-l", "--lost", dest="lost_fraction", metavar = "NUMBER",
                  default = 90,
                  help="Fraction of the original dataset size that defines a threashold of what we call lost dataset. Default: %default")

(options, args) = parser.parse_args()

proxyCertificate = "/tmp/x509up_u11792"
capath = "/etc/grid-security/certificates"

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

tiers = re.sub(',',"_",options.tiers)
period = None
if options.run: period = options.run
if options.era: period = options.era
logfile_prefix = "reports/consistency_check-%s-%s-%s" % (
     period, 
     time.strftime("%Y-%m-%d", time.localtime()),
     tiers)
logfile = logfile_prefix+".log"
log = open(logfile, 'w')

summary = {"NotInPhedex":[],
           "NoCompleteCopyAnywhere":[],
           "NoCompleteCopyAnalysisOps":[],
           "Lost":[]
           }

def form_subscription_report(subscription):
     return "node: %-20s fraction: %-3s%%  custodial: %1s group %-20s" % (
          subscription['node'],subscription['percent_bytes'],subscription['custodial'],subscription['group'])

def get_availability(percentage):
     availability = "complete"
     if percentage == None:
          availability = "missing"
     else:
          if percentage < 100:
               if percentage > options.lost_fraction:
                    availability = "incomplete"
               else:
                    availability = "missing"
     return availability


def get_subscription_information(dataset):
     report = {'nComplete':0,'PhEDEx':False,'nAnalysisOpsComplete':0,'nIncomplete':0,'firstSubscription':None}
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
               print >>log, "No information in PhEDEx about this dataset"
               return report
          if len(datasets)!=1:
               raise Exception("Unpexpected number of datasets is returned")
          report['PhEDEx'] = True
          subscriptions = datasets[0]['subscription']
          for subscription in subscriptions:
               if report['firstSubscription']==None or report['firstSubscription']>subscription['time_create']:
                    report['firstSubscription'] = subscription['time_create']
               availability = get_availability(subscription['percent_bytes'])
               if availability == "complete": 
                    report['nComplete'] += 1
                    if subscription['group']=='AnalysisOps': 
                         report['nAnalysisOpsComplete'] += 1
               elif availability == "incomplete": 
                    report['nIncomplete'] += 1
               print >>log, form_subscription_report(subscription)
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

nDatasetsToCheck = 0
for ds in datasets:
     if datatiers and not ds['data_tier_name'] in datatiers:
          continue
     nDatasetsToCheck += 1
print >>log, "Number of datasets to check: %d" % nDatasetsToCheck
print "Number of datasets to check: %d" % nDatasetsToCheck

for ds in datasets:
     if datatiers and not ds['data_tier_name'] in datatiers:
          continue
     print >>log, "\nDatset:",ds['dataset'],
     blocks = api.listBlockSummaries(dataset = ds['dataset'])
     ds_size = blocks[0]['file_size']/pow(2,30)
     print >>log, " \t %0.0f GB" % (ds_size)

     report = get_subscription_information(ds['dataset'])
     if options.ignore and report['firstSubscription']!=None and (time.time()-report['firstSubscription'])<86400*options.ignore:
          print >>log, "Skip the dataset availability check since the first subscription is very recent"
          continue
     if report['nComplete']==0:
          summary["NoCompleteCopyAnywhere"].append(ds['dataset'])  
          if report['nIncomplete']==0:
               summary["Lost"].append(ds['dataset'])
     if report['nAnalysisOpsComplete']==0:
          summary["NoCompleteCopyAnalysisOps"].append(ds['dataset'])  

pprint.pprint(summary)

print "Number of valid datasets registered in DBS missing in PhEDEx: %d" % (len(summary["NotInPhedex"]))
if len(summary["NotInPhedex"])>0:
     with open("%s-not_in_phedex.txt"%logfile_prefix,'w') as f:
          for ds in summary["NotInPhedex"]:
               f.write(ds+"\n")

nMissingData = len(summary["NoCompleteCopyAnywhere"])-len(summary["Lost"])
print "Number of datasets with missing data (not complete, but above %s%%): %d" % (options.lost_fraction,nMissingData)
if nMissingData > 0:
     with open("%s-above_%0.0f-below_100.txt"%(logfile_prefix,options.lost_fraction),'w') as f:
          for ds in summary["NoCompleteCopyAnywhere"]:
               if not ds in summary["Lost"]:
                    f.write(ds+"\n")

print "Number of datasets that are lost (no copy with greater than %s%% availability): %d" % (options.lost_fraction,len(summary["Lost"]))
if len(summary["Lost"])>0:
     with open("%s-below_%0.0f.txt"%(logfile_prefix,options.lost_fraction),'w') as f:
          for ds in summary["Lost"]:
               f.write(ds+"\n")

nWarning = len(summary["NoCompleteCopyAnalysisOps"])-len(summary["Lost"])
print "Number of datasets that can disappear (no complete copy subscribed under AnalysisOps): %d" % (nWarning)
if nWarning>0:
     with open("%s-warning.txt"%(logfile_prefix),'w') as f:
          for ds in summary["NoCompleteCopyAnalysisOps"]:
               if not ds in summary["Lost"]:
                    f.write(ds+"\n")


print "For details see produced files"               
     
