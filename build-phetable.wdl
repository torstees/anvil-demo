# Using ICD data, build the phetable to be used for PheWAS analyses. 

# 
workflow build_phetable {
    # Three required arguments:
    #   icd_csv: ID, code, vocabulary, age/index
    #   min_code_count count (default 2)
    #   out -- Prefix for filenames produced during run
    #
    # Optional Arguments:
    #   min-age:    (0 ignores age. Appropriate for index)
    #   id-col:     name of column where the ID is found (default: "SubjID")
    #   age-col:    name of column where the AGE is found (default: "AGE_AT_ICD")
    #   icd-col:    name of column where the ICD code is found (default: "ICD_CODE")
    #   icd-era:    name of column where the vocaubulary is found (default: ICD_FLAG)
    #   compress-output:    Set to False if you want uncompressed (gzip) output
    #   translation_fn: Name of the file containing translation codes
    
    
    File icd_csv
    Int min_code_count = 2
    String out
  
    Int min_age = 18
    String id_col = "SubjID"
    String age_col = "AGE_AT_ICD"
    String icd_col = "ICD_CODE"
    String icd_era = "ICD_FLAG"
    File translation_fn = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/pub/src/data/phecode_map.csv"
    File rollup_fn = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/pub/src/data/phecode_rollup_map.csv"
    File gender_rest_fn = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/pub/src/data/gender_restriction.csv"
    
    File build_phe = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/pub/src/build_phetables.py"
    
    call translate{ input:
        icd_csv=icd_csv,
        min_code_count=min_code_count,
        out=out,
        min_age=min_age,
        id_col=id_col,
        age_col=age_col,
        icd_col=icd_col,
        icd_era=icd_era,
        translation_fn=translation_fn,
        rollup_fn=rollup_fn,
        gender_rst_fn=gender_rest_fn,
        build_phe=build_phe
    }    
}


# The singular task of extracting data
task translate {
    File icd_csv
    Int min_code_count
    String out
    
    Int min_age
    String id_col
    String age_col
    String icd_col
    String icd_era
    File translation_fn
    File build_phe
    File gender_rst_fn
    File rollup_fn
    
    # In my observations, 3 million subjects with a combined 100+ million (filtered on adults 18 or over)
    # 2 Gigs were observed. 
    Int memory_gb = 4
    
    # My 1 gig icd code list, compressed needed about 100K compressed for phetable
    Int diskspace_bg = ceil(size(translation_fn,   "G") + size(icd_csv, "G") * 1.25) +1 
   
    command {
        python3 ${build_phe}   -i ${icd_csv} \
                    -t ${translation_fn} \
                    -r ${rollup_fn} \
                    -m ${min_code_count} \
                    -a ${min_age} \
                    --id-col ${id_col} \
                    --age-col ${age_col} \
                    --icd-col ${icd_col} \
                    --icd-era ${icd_era} \
                    --out ${out}
    }
     
    # define the output files. The job will report failure if every one of them don't exist.
    output {
        File phetable = "${out}-phetable.txt.gz"
        File code_log = "${out}-no-matching-phe.txt"
    }

    # The runtime describes job details which include those parameters required by
    # the cloud to create and run the job. 
    
    # We'll need enough space for the script (few k), the translation (few megs) and the dataset
    # I'll 
    runtime {
        # https://cloud.docker.com/u/bioithackathon/repository/docker/bioithackathon/infercnv/tags
        docker: "bioithackathon/infercnv:0-99-5_v1"
        memory: "${memory_gb} GB"
        disks: "local-disk ${diskspace_bg} HDD"  
        bootDiskSizeGb: 12
        cpu: 1
        preemptible: 1
    }

    meta {
        author: "Eric Torstenson"
    }
}
