# Extract a subset of loci from a VCF file using bcftools

# Workflow represents the wrapper for the given task. For this simple task, we have
# a single task we'll perform, extract() 

workflow merge_loci {
	String output_prefix

	# Since it seems wasteful to run this in parallel, since the task itself is crazy short, but
	# the copy will take a long time...so, maybe it's worth it?
	Array[File] vcf_files = ["gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/e4d10b77-b90f-41e3-b51c-27bef2b4deae/call-extract/chr11.dose.emerge_ids.consented.merged-loi.vcf", "gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/955c75b1-10db-4573-87d1-2deca4c795e9/call-extract/chr13.dose.emerge_ids.consented.merged-loi.vcf"]

	# We'll call our task with the input from the user assigned to function's argument, loci_of_interest
	call merge{input: vcf_files=vcf_files, output_prefix=output_prefix}
}


# The singular task of extracting data
task merge {
	# We don't have the entire dataset right now, so we'll assume a single VCF file for 
	# simplicities sake
	Array[File] vcf_files

	# Since we have only one vcf, we use that to create our output file. 
	String output_prefix 

    # command is bash
    command{	
    	bcftools concat ${sep=' ' vcf_files} --allow-overlaps --output-type v --output ${output_prefix}.vcf
    	which gsutil
		# Concatenate these files
		# --allow-overlaps should permit files being out of order
		# --output-type v -- vcf
		# 
      	echo "Job completed." 
    }

		# define the output files. These files MUST exist for the job to succeed. 
		# They will be copied up to the run directory
    output {
        File out="${output_prefix}.vcf"
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
        memory: "512MB"
        disks: "local-disk 100 HDD"
        preemptible: "1"
    }

    meta {
        author: "Eric Torstenson"
    }
}
