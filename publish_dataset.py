#!/usr/bin/env python
from dbs.apis.dbsClient import DbsApi
import sys,time,uuid,commands,re,pprint,os
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from optparse import OptionParser

description = """
Simple tool to publish a set of files in DBS3. Minimal support for
issues and complex cases. Dataset name format:
                                             
/PrimaryDataset/Campaign-v[number]/Tier
                                          
Version number is auto-assigned by based on already published dataset
names.
"""
parser = OptionParser(usage = "\n\t%prog [options]", description = description, epilog= ' ')
parser.add_option("-l", "--list", dest="files", help="Comma separated list of logical file names to publish without whitespaces.",
                  metavar="FILES")
parser.add_option("-f", "--file", dest="file", help="File that contains a list of files to be published.",
                  metavar="FILE")
parser.add_option("-p", "--primary", dest="primary_ds", metavar="PD",
                  help="Primary dataset name (first part of the dataset name). By default common part of the file names is used.")
parser.add_option("-c", "--campaign", dest="campaign", metavar="CAMPAIGN", default="RunIIWinter15pLHE-MCRUN2-LHE",
                  help="Campaign name used as a reference (middle part of the dataset name). Default: %default")
parser.add_option("-t", "--tier", dest="tier", metavar="TIER", default="USER",
                  help="Data tier (last part of the dataset name). Default: %default")
parser.add_option("-r", "--dryrun", dest="dryrun", action="store_true", default=False,
                  help="Dry run without publishing anything.")
cmssw_version = ''
if 'CMSSW_VERSION' in os.environ:
     cmssw_version = os.environ['CMSSW_VERSION']
parser.add_option("--release", dest="release", metavar="CMSSW", default=cmssw_version,
                  help="CMSSW release version. Default: %default")

(options, args) = parser.parse_args()

if not options.file and not options.files:
     parser.print_help()
     sys.exit()

# ==========================================================================

# gets files to be published
files = []
if options.files:
    files = options.files.split(',')
if options.file:
    with open(options.file,'r') as fIN:
         for file in fIN:
              if re.search('\S',file):
                   files.append(file.strip('\n'))
if len(files)==0:
     raise Exception("Failed to get file names for publication")

print "Files to publish:"
pprint.pprint(files)

# form output dataset name
primary_dataset_name = ""
# dataset = "%s/%s-v*/%s" % (
sys.exit()

# 
# Info
#
# Missing: file_lumi_list
# Example:
# 'file_lumi_list': [{u'lumi_section_num': 4027414, u'run_num': 1}, 
#                  {u'lumi_section_num': 26422, u'run_num': 2},
#                  41{u'lumi_section_num': 29838, u'run_num': 3}]
# https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/
# https://github.com/dmwm/AsyncStageout/blob/master/src/python/AsyncStageOut/PublisherWorker.py#L743
# xrd eoscms stat /store/user/dmytro/lhe/DM_ttbar01j/DMScalar_ttbar01j_mphi_200_mchi_150_gSM_1p0_gDM_1p0.root
# xrd cms-xrd-global.cern.ch stat /store/user/dmytro/lhe/DM_ttbar01j/DMScalar_ttbar01j_mphi_200_mchi_150_gSM_1p0_gDM_1p0.root
# https://svnweb.cern.ch/trac/CMSDMWM/browser/DBS/trunk/Client/tests/dbsclient_t/unittests/blockdump.dict
# 

dbsWriter = DbsApi(url="https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter/")
dbsReader = DbsApi(url="https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader/")
# dbsReader = DbsApi(url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/")

# datasets = dbsReader.listDatasets(dataset='/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', detail=True)
# print datasets

dataset_name = "/DMScalar_ttbar01j_mphi_200_mchi_150_gSM_1p0_gDM_1p0/RunIIWinter15pLHE-MCRUN2-LHE-v1/USER"
# acquisition_era_name 
# release_version $CMSSW_VERSION 
# global_tag
# max_files_per_block

# =======================================================================================================


sys.argv = [] # clear up list of arguments to avoid confusing ROOT
import ROOT, array
ROOT.gROOT.SetBatch(True)

def get_file_size(lfn):
    result = commands.getoutput("xrd cms-xrd-global.cern.ch stat %s"%lfn)
    match = re.search('Size:\s*(\d+)',result)
    if match:
        return int(match.group(1))
    raise Exception("Failed to get file size for file %s" % lfn)

def get_nevents(lfn):
    f = ROOT.TFile.Open('root://cms-xrd-global.cern.ch/%s'%lfn)
    tree = f.Get("Events")
    return tree.GetEntries()

def get_run_lumi_list(lfn):
    file_lumi_list = []
    f = ROOT.TFile.Open('root://cms-xrd-global.cern.ch/%s'%lfn)
    tree = f.Get("LuminosityBlocks")
    for entry in tree:
        file_lumi_list.append({'lumi_section_num':entry.LuminosityBlockAuxiliary.luminosityBlock(),
                               'run_num':entry.LuminosityBlockAuxiliary.run()})
    return file_lumi_list

empty, primary_ds_name, proc_name, ds_tier =  dataset_name.split('/')

datasets = dbsReader.listPrimaryDatasets(primary_ds_name=primary_ds_name)
print datasets

# Find files already published in this dataset.
existingDBSFiles = dbsReader.listFiles(dataset = dataset_name, detail = True)
existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
existingFilesValid = [f['logical_file_name'] for f in existingDBSFiles if f['is_file_valid']]
print "Dataset %s already contains %d files" % (dataset_name, len(existingFiles)),
print " (%d valid, %d invalid)." % (len(existingFilesValid), len(existingFiles) - len(existingFilesValid))

# Get a list of files that need to be acted on
files_to_publish = []
files_to_change_status = []
for file in files:
    if file not in existingFiles:
        files_to_publish.append(file)
    elif file not in existingFilesValid:
        files_to_change_status.append(file)
if len(files_to_publish)==0 and len(files_to_change_status)==0:
    print "Everything is already published and up to date"
    sys.exit()
print "Found %d files not already present in DBS which will be published." % len(files_to_publish)
print "Found %d files that require status change." % len(files_to_change_status)


# ============================================================================

campaign = 'RunIIWinter15pLHE'

output_config = {'release_version': 'CMSSW_7_4_1_patch2',
                 'pset_hash': 'NoHash',
                 'app_name': 'crab',
                 'output_module_label': 'o',
                 'global_tag': 'NoTag',
                 }

dataset_config = {'dataset': dataset_name,
                  'processed_ds_name': proc_name,
                  'data_tier_name': ds_tier,
                  'dataset_access_type': 'VALID', 
                  'physics_group_name': 'NoGroup',
                  'last_modification_date': int(time.time()),
                  }

block_name = "%s#%s" % (dataset_name, str(uuid.uuid4()))

block_config = {'block_name': block_name, 
                'origin_site_name': 'T2_CH_CERN', 
                'open_for_writing': 0}

acquisition_era_config = {
    'acquisition_era_name':campaign, 
    'start_date':0
    }
processing_era_config = {
    'processing_version': 1, 
    'description': 'LHE_Injection'
}
primds_config = {
    'primary_ds_type': 'mc', 
    'primary_ds_name': primary_ds_name
}

files = []
for lfn in files_to_publish:
    file = {'logical_file_name':lfn,
            'event_count':get_nevents(lfn),
            'file_size':get_file_size(lfn),
            'check_sum': 'NOTSET',
            'adler32':'deadbeef',
            'file_type': 'EDM',
            'file_lumi_list':get_run_lumi_list(lfn)
            }
    files.append(file)
            
  
blockDict = {
    'dataset_conf_list': [output_config],
    'file_conf_list': [],
    'files': files,
    'processing_era': processing_era_config,
    'primds': primds_config,
    'dataset': dataset_config,
    'acquisition_era': acquisition_era_config,
    'block': block_config,
    'file_parent_list': []
    }

blockDict['block']['file_count'] = len(files)
blockDict['block']['block_size'] = sum([int(file['file_size']) for file in files])

pprint.pprint(blockDict)

# Insert primary dataset name. It's safe to do it for already existing primary datasets
primds_config = {'primary_ds_name': primary_ds_name, 'primary_ds_type': 'mc'}
dbsWriter.insertPrimaryDataset(primds_config)

# Insert block of files
try:
    dbsWriter.insertBulkBlock(blockDict)
except HTTPError, he:
    print he
