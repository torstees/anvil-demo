# Extract a subset of loci from a VCF file using bcftools. This version assumes you'll 
# be running each chromosome separately

# Workflow represents the wrapper for the given task. For this simple task, we have
# a single task we'll perform, extract() 

workflow locus_extract {
	# We'll have two arguments, loci_of_interest and the VCF, which we'll provide a 
	# default value. Eventually, this would be an array of VCFs
	# be extracted
	File loci_of_interest


	# Since it seems wasteful to run this in parallel, since the task itself is crazy short, but
	# the copy will take a long time...so, maybe it's worth it?
	File vcf_file = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/data/emerge_rc_200.vcf.gz"

	# We'll call our task with the input from the user assigned to function's argument, loci_of_interest
	call extract{input: loci_of_interest=loci_of_interest, vcf_file=vcf_file}
}


# The singular task of extracting data
task extract {
	# We don't have the entire dataset right now, so we'll assume a single VCF file for 
	# simplicities sake
	File vcf_file
	String vcf = basename(vcf_file)

	# Currently, the index files reside outside the original VCF file's location
	File vcf_index = "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/data/emerge-tbi/${vcf}.tbi"


	# This will be the file containing the loci to be extracted from the vcf(s)
	File loci_of_interest
	
	# Since we have only one vcf, we use that to create our output file. 
	String file_prefix = sub(vcf, ".vcf.gz", "-loi.vcf")
    
    Int disk_size = ceil(2 * size(vcf_file, "GB"))
    # command is bash
    command{	
		echo "VCF File: ${vcf}"
		ls -ltrh 
        # Copy the index file over to the path associated with the vcf
        cp ${vcf_index} /cromwell_root/fc-secure-a523ce7e-6a39-4bd0-9b4e-85ddc20ff9cc/eMERGE_1_2_3_Imputation_v3/Merged_VCF_files_by_chr/
		bcftools view -R ${loci_of_interest} ${vcf_file} -o ${file_prefix}
        ls -ltrh
		echo "Job completed." 
    }

		# define the output files. These files MUST exist for the job to succeed. 
		# They will be copied up to the run directory
    output {
        File out="${file_prefix}"
    }

		# The runtime describes job details which include those parameters required by
		# the cloud to create and run the job. 

		# I've no clue how much temp space is used by bcftools. I do hope that it won't attempt top copy 
		# the dataset VCF files locally when running...I'm starting with 100 for local disk and a minimal 
		# amount of ram and a single CPU. 
		# 
		# preemptible=1 probably makes it cheaper?
    runtime {
        docker: "tmajarian/bcftools_htslib:v0.1"
        cpu: "1"
        memory: "1024MB"
        disks: "local-disk ${disk_size} HDD"
        preemptible: "1"
    }

    meta {
        author: "Eric Torstenson"
    }
}

