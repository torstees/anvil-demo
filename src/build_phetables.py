#!/usr/bin/env python

'''
Overview:
1.  Input:
    a.  File with: ID, code, vocabulary_id, index_age
    b.  Minimum code count for cases (default 2)
2.  Output:
    a.  File with ID, and phecodes as columns
'''

import sys
import csv
import collections
import bisect
import datetime
import gzip
import re
import os
import argparse

from pathlib import Path


"""
We can probably minimize memory using ICD codes sorted by ID (read/write a single individual 
at once. This may also be done using bash commands like sort
"""

data_dir = Path(os.path.abspath(__file__)).resolve().parent / "data"
print(data_dir)

# icd_code_list = "icd_code_list.txt"

# GRID,ICD_DATE,ICD_CODE,CODE_DESC,ICD_FLAG,AGE_AT_ICD
icd_input_file = f"{data_dir}/icd_codes-sorted.csv.gz"

# icd,icd_era,desc,phecode,chapter
phewas_translate_file = f"{data_dir}/phecode_map.csv"

phewas_rollup = f"{data_dir}/phecode_rollup_map.csv"

# Minimum count to call a phecode a case
MIN_COUNT = 2

# Note things once that may be of interest to the user, but aren't show stoppers
observation_logs = set()
def LogObservation(key, message):
    if key not in observation_logs:
        print(message)
        observation_logs.add(key)
        
class Timer:
    def __init__(self):
        self.start = datetime.datetime.now()
        self.last_period = self.start

    def diff(self):
        return (datetime.datetime.now() - self.start).seconds
        
    def period(self):
        now = datetime.datetime.now()
        seconds = (now - self.start).seconds
        self.last_period = now
        return seconds

    def report(self, msg):
        return f"{msg} {self.period()}s"

t = Timer()
 
def report(msg):
    global t
    print(t.report(msg), file=sys.stderr)
    
# Just match either 9 or 10 in case they didn't use ICDX*CM or whatever
erax = re.compile("(?P<era>9|10)", re.I)
class Codes:
    '''Aggregate all of our ICD codes into appropriate ICD and phecodes'''
    def __init__(self, male_code='M', female_code='F'):
        """icd_fn contains all ICD codes
           translate_fn contains details relating to icd => phecode mapping
        """
        
        self.phecode_list = set()
        self.icd_codes = []
             
        # First pass, let's try just using dictionaries
        self.icd9_codes = {}
        self.icd10_codes = {}
        self.icd9_count = 0
        self.male_only = set()
        self.female_only = set()
        self.male_code = male_code
        self.female_code = female_code
        
        self.observed_pheidx = set()
        # phecode_idx => [idx1, idx2]       (parent => child)
        # Ideally, we will not keep identities
        self.rollups = collections.defaultdict(list)
        # child => parents
        self.ru_parents = collections.defaultdict(list)
        
    def add_gender_restrictions(self, line):
        phecode = line['phecode']
        if line['male_only'] == "TRUE":
            self.male_only.add(phecode)
        if line['female_only'] == "TRUE":
            self.female_only.add(phecode)

    def add_phecode(self, line):
        icd = line['code']
        phecode = line['phecode']
        era = erax.search(line['vocabulary_id'])
        
        if phecode.strip() != "":
            if era is not None:
                era = era.group('era')
                self.phecode_list.add(phecode)
                if era == '9':
                    self.icd9_codes[icd] = phecode
                elif era == '10':
                    self.icd10_codes[icd] = phecode
                else:
                    report(f"Unrecognized era {line['icd_era']}")
                    return False    
                return True
            # no 9 or 10 in era
        return False
            
    def finalize_table(self):
        self.phecode_list = sorted(self.phecode_list)
        self.icd9_count = len(self.icd9_codes)
        self.icd_codes = sorted(self.icd9_codes.keys()) + sorted(self.icd10_codes.keys())
        self.phe_map = [0] * len(self.icd_codes)


        idx =0
        for code in self.icd_codes:
            try:
                if idx < self.icd9_count:
                    phecode = self.icd9_codes[code]
                else:
                    phecode = self.icd10_codes[code]
                    
                self.phe_map[idx] = self.get_pheindex(phecode)         
                idx += 1
            except:
                print(f"Well, something went wrong with {phecode}")
                sys.exit(1)
        self.icd_count = len(self.icd_codes)
        
        # clear out temp data
        self.icd9_codes = {}
        self.icd10_codes = {}        
        
    def load_rollup(self, rollup_map):
        reader = csv.DictReader(rollup_map, delimiter=',', quotechar='"')
        # "","code","phecode_unrolled"
        for line in reader:
            self.add_rollup(line['phecode_unrolled'], line['code'])
              
    def add_rollup(self, parent, child):
        # Currently, we assume that the identity is always true
        if parent != child:
            pidx = self.get_pheindex(parent)
            cidx = self.get_pheindex(child)
            self.rollups[pidx].append(cidx)
            self.ru_parents[cidx].append(pidx)
                
    def load_translation(self, translate_fn):
        reader = csv.DictReader(translate_fn, delimiter=',', quotechar='"')

        for line in reader:
            self.add_phecode(line)

        self.finalize_table()        
        
    def get_counts_by_index(self, pheidx, count_store):
        """Return the counts, including rollup counts as well"""
        
        global MIN_COUNT
        count = 0
        
        if pheidx in count_store:
            count = count_store[pheidx]
        
        if pheidx in self.rollups:
            for idx in self.rollups[pheidx]:
                count += count_store[idx]
                
        if count == 0:
            for idx in self.ru_parents.keys():
                if count_store[idx] > 0:
                    return -1
        return count

    def get_pheindex(self, phecode):
        idx = bisect.bisect_left(self.phecode_list, phecode)
        
        if self.phecode_list[idx] != phecode:
            raise ValueError
        return idx
        
    def get_pheindex_by_icd(self, icd, era):
        pheindex = self.phe_map[self.icd_index(icd, era)]
        self.observed_pheidx.add(pheindex)
        return pheindex
        
    def gender_restricted(self, phecode, gender):
        "returns True if the phecode doesn't apply to gender (M/F)"
        if gender is None:
            return False
        
        # If the phecode is only for female, return TRUE only if this isn't a woman
        if phecode in self.female_only():
            return gender != self.female_code
        if phecode in self.male_only():
            return gender != self.male_code

        if gender not in [self.female_code, self.male_code]:
            report(f"Unrecognized gender '{gender}'. Should be {self.male_code}/{self.female_code}")
        return False

    def icd_index(self, icd, era):
        '''Determine which index where a given ICD code is found'''
        base_index = 0
        search_space = self.icd_codes

        if era == 10:
            base_index = self.icd9_count

        i = bisect.bisect_left(search_space, icd)
        if i != len(search_space) and search_space[i] == icd:
            return i

        #print(f"Who knows what this is: {icd} {era} {i} {len(search_space)} ")
        raise ValueError

    def phe_list(self):
        phelist = []
        
        for pheidx in range(len(self.phecode_list)):
            addme = False
            if pheidx in self.observed_pheidx:
                addme = True
            if pheidx in self.rollups:
                for idx in self.rollups[pheidx]:
                    addme = addme or idx in self.observed_pheidx
            if pheidx in self.ru_parents:
                for idx in self.ru_parents[pheidx]:
                    addme = addme or idx in self.observed_pheidx
            if addme:
                phelist.append(self.phecode_list[pheidx])
                
        return phelist

    def phe_header(self, idcolname):
        return [idcolname] + self.phe_list()

    def write_phecode_header(self, file, idcolname):
        file.writerow(self.phe_header(idcolname))

class Subject:
    idcolname = "GRID"
    agecolname = "AGE_AT_ICD"
    icdcolname = "ICD_CODE"
    eracolname = "ICD_FLAG"
    sex = None              # Set this to M/F if there is demographics available
    min_age = 0             # Default to accept everyone
    codes = None            # Assign this to the codes data store
    missing_icd_codes = collections.defaultdict(int)        # Keep track of them here for reporting later
    
    def __init__(self, id, sex=None):
        global codes
        self.id = id
        self.gender = sex

        #self.icd_observations = [0] * codes.icd_count
        self.icd_observations = collections.defaultdict(int)
    def parse_icd(self, row):
        # For now, we assume one entry per date
        #date = datetime.datetime.strptime(row['ICD_DATE'], '%Y-%m-%d')
        
        # For our emerge ICD data, I'm seeing ages that don't exist, and neither does anything else
        # (except for ID). For now, I'm just going to use the missing age a simple surrogate to weird
        # missing data.
        try:
            age = float(row[Subject.agecolname])
        except ValueError:
            LogObservation("Invalid Age", f"Age value, '{row[Subject.agecolname]}' is invalid. Setting it to 0")
            age = 0
        except:
            LogObservation("AGE", f"Age column, '{Subject.agecolname}' not found in header: {','.join(row.keys())}. Setting age to 100.")
            age = 100
            
        icd = row[Subject.icdcolname]
        
        era = row[Subject.eracolname]
        
        # If there isn't an era, let's just ignore the row
        if era.strip() != "":
            era = int(erax.search(row[Subject.eracolname]).group('era'))
      
            if age >= Subject.min_age:
                try:
                    icd_idx = Subject.codes.get_pheindex_by_icd(icd, era)
                    self.icd_observations[icd_idx] += 1
                except:
                    Subject.missing_icd_codes[f"{era}\t{icd}"]+= 1


    def get_phecodes(self, min_count):
        phe_row = [self.id]

        idx = 0
        for phe in Subject.codes.phe_list():
            # -1 will fall through to NA
            observations = Subject.codes.get_counts_by_index(idx, self.icd_observations)
            
            if self.sex is not None:
                if Subject.codes.gender_restricted(phe, self.sex):
                    phe_row.append("NA")

            else:
                if observations >= min_count:
                    phe_row.append('TRUE')
                elif observations == 0:
                    phe_row.append('FALSE')
                else:
                    phe_row.append('NA')
            idx += 1
        return phe_row

def GetID(grid):
    return int(grid.replace("R", ""))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate the PheTable for use in subsequent PheWAS analyses')
    parser.add_argument("-i", "--icd-codes", type=argparse.FileType('rt', encoding="utf-8-sig"), required=True, help='CSV file containing id, icd_code, icd_era and age')
    parser.add_argument("-t", "--translation-map", type=argparse.FileType('rt'), default=phewas_translate_file, help="CSV file containing mapping between ICD code and PheCode")
    parser.add_argument("-r", "--rollup-map", type=argparse.FileType('rt'), default=phewas_rollup, help="CSV File containing rollup information")
    parser.add_argument("-m", "--min-count", type=int, default=2, help='Minimum phecode count for a case')
    parser.add_argument("-a", "--min-age", type=int, default=18, help='Minimum age for icd code to be counted toward case status')
    parser.add_argument("--demo", type=argparse.FileType('rt'), required=False, help='CSV file containing demographic data for gender/sex')
    parser.add_argument("--id-col", type=str, default='id', help='Column name for the subject ID')
    parser.add_argument("--age-col", type=str, default='AGE_AT_ICD', help='Column name for the age or index column')
    parser.add_argument("--icd-col", type=str, default='ICD_CODE', help='Column name for the ICD column name')
    parser.add_argument("--icd-era", type=str, default='ICD_FLAG', help='Column name for the ICD era flag')
    parser.add_argument("--sex-col", type=str, default='SEX', help='Column name for the sex flag inside the demographics file')
    parser.add_argument("--demo-id-col", type=str, default='id', help='Column name for ID in demo (only required if different from id-col and demographics are in use')
    parser.add_argument("--out", type=str, default='phetable', help='Prefix for all output from script')
    parser.add_argument("--compress-output", type=bool, default=True, help='Compress output table with gzip')
    parser.add_argument("--male-code", type=str, default='M', help='Encoding used to represent male')
    parser.add_argument("--female-code", type=str, default='F', help='Encoding used to represent female')
    args = parser.parse_args()

    Subject.idcolname = args.id_col
    Subject.agecolname = args.age_col
    Subject.icdcolname = args.icd_col
    Subject.eracolname = args.icd_era
    
    report("Loading Codes")
    Subject.codes = Codes(male_code=args.male_code, female_code=args.female_code)
    Subject.codes.load_translation(args.translation_map)
    report(f"Codes loaded.")
    Subject.codes.load_rollup(args.rollup_map)
    subjects = {}

    observed_grids = set()
    
    if args.icd_codes.name.split(".")[-1] == "gz":
        filename = args.icd_codes.name
        args.icd_codes.close()
        args.icd_codes = gzip.open(args.icd_codes, 'rt')

    if args.demo is not None:
        idcol = args.id_col
        
        if args.demo_id_col is not None:
            idcol = args.demo_id_col
        reader = csv.DictReader(args.demo, delimiter=',', quotechar='"')
        
        for line in reader:
            id = line[idcol]
            subjects[id] = Subject(id, line[args.sex_col]) 

    reader = csv.DictReader(args.icd_codes, delimiter=',', quotechar='"')
    #reader.fieldnames = [reader.fieldnames[x].strip() for x in reader.fieldnames]
    lines_counted = 0
    cur = None
    for line in reader:       
        id = line[Subject.idcolname].strip()
        if id not in subjects:
            subjects[id] = Subject(id)
        subjects[id].parse_icd(line)
        lines_counted += 1
        
        if lines_counted % 10000000 == 0:
            report(f"-> {lines_counted} {len(subjects)}")

    if args.compress_output:
        outf = gzip.open(f"{args.out}-phetable.txt.gz", 'wt')
    else:
        outf = open(f"{args.out}-phetable.txt", 'wt')
    
    writer = csv.writer(outf, delimiter=',', quotechar='"')
    Subject.codes.write_phecode_header(writer, Subject.idcolname)
    for id in sorted(subjects.keys()):
        writer.writerow(subjects[id].get_phecodes(MIN_COUNT))
    
    outf.close()
    
    with open(f"{args.out}-no-matching-phe.txt", 'wt') as outf:
        writer = csv.writer(outf, delimiter='\t', quotechar='"')
        writer.writerow(["ICD Code", "Count"])
        
        for id in sorted(Subject.missing_icd_codes.keys()): 
            writer.writerow([id, Subject.missing_icd_codes[id]])
