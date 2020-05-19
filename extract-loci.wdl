# Extract a subset of loci from a VCF file using bcftools

# Workflow represents the wrapper for the given task. For this simple task, we have
# a single task we'll perform, extract() 

workflow locus_extract {
	# We'll have two arguments, loci_of_interest and the VCF, which we'll provide a 
	# default value. Eventually, this would be an array of VCFs
	# be extracted
	File loci_of_interest

	String file_prefix

	# Since it seems wasteful to run this in parallel, since the task itself is crazy short, but
	# the copy will take a long time...so, maybe it's worth it?
	Array[String] vcf_files = ["gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/data/emerge_rc_200.vcf.gz"]

	# We'll call our task with the input from the user assigned to function's argument, loci_of_interest
	call extract{input: loci_of_interest=loci_of_interest, vcf_files=vcf_files, file_prefix=file_prefix}
}


# The singular task of extracting data
task extract {
	# We don't have the entire dataset right now, so we'll assume a single VCF file for 
	# simplicities sake
	Array[String] vcf_files

	# This will be the file containing the loci to be extracted from the vcf(s)
	File loci_of_interest
	
	# Since we have only one vcf, we use that to create our output file. 
	String file_prefix 

    # command is bash
    command{	
    	export file_list = ()
		for vcf_source in ${sep=' ' vcf_files}
		do
			gsutil cp vcf_source .
			vcf=$(basename $vcf_source)
			# We'll copy the index file over. It doesn't exist currently in the VCF's directory, so we've 
			# stashed copies of them in our own bucket for the time being
			gsutil cp gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/data/emerge-tbi/$vcf.tbi .
			echo "-> bcftools view -R ${loci_of_interest} $vcf -o prt-$vcf.vcf"
    		bcftools view -R ${loci_of_interest} $vcf -o prt-$vcf.vcf
    		# We don't want to keep these VCF files around, since they can get really large
    		rm $vcf
    		file_list += prt-$vcf.vcf
		done
		# Concatenate these files
		# --allow-overlaps should permit files being out of order
		# --output-type v -- vcf
		# 
		bcftools concat $file_list --allow-overlaps --output-type v --output ${file_prefix}.vcf
      	echo "Job completed." 
    }

		# define the output files. These files MUST exist for the job to succeed. 
		# They will be copied up to the run directory
    output {
        File out="${file_prefix}.vcf"
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
