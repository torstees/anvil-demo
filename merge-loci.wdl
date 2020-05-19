# Extract a subset of loci from a VCF file using bcftools

# Workflow represents the wrapper for the given task. For this simple task, we have
# a single task we'll perform, extract() 

workflow merge_loci {
	String output_prefix

	# Since it seems wasteful to run this in parallel, since the task itself is crazy short, but
	# the copy will take a long time...so, maybe it's worth it?
	Array[File] vcf_files = ["gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/4a29e547-43f6-4ea2-939a-d13420541779/call-extract/chr1.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/7c8d1442-ed14-4b54-b641-f6885490c031/call-extract/attempt-2/chr2.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/d9b49e24-2128-4a19-93b9-8dc1d8cbaa34/call-extract/chr3.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/1838a5f1-5d74-4b92-8d85-dcd1604977ec/call-extract/attempt-2/chr4.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/5da61ce0-2d8a-4cbf-bb80-870b1291a9da/locus_extract/ee3aefe5-4558-47ad-9ed2-dd09bf128a62/call-extract/chr5.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/6769e7db-37c4-466a-a6dd-661f02009ea6/locus_extract/d488101e-2a08-4c5b-9a5d-a70ed3546eec/call-extract/chr6.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/b2d881ab-1d6e-4595-8e74-455b688a3e8a/call-extract/chr7.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/6769e7db-37c4-466a-a6dd-661f02009ea6/locus_extract/b8bc1298-476f-4acc-92bc-5c70da9c16a9/call-extract/attempt-2/chr8.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/4c65c124-65e9-49c5-be6d-ecd604caf64a/locus_extract/c8a69f1a-75b9-4c63-864b-f4a3863ca136/call-extract/attempt-2/chr9.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/6f97cf2d-a6dc-4943-9c40-bd55916308a8/call-extract/chr10.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/e4d10b77-b90f-41e3-b51c-27bef2b4deae/call-extract/chr11.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/f3fbbc98-ec5d-4d74-acb8-7ff026b41180/locus_extract/510c4086-c0c6-4e33-96f0-7cd887095a1d/call-extract/chr12.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/955c75b1-10db-4573-87d1-2deca4c795e9/call-extract/chr13.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/9533106a-2215-4cc5-ad91-184fa6485c4e/call-extract/attempt-2/chr14.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/dcb3f345-e632-4227-8f22-799abce56e8b/call-extract/chr15.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/6895859d-a3b0-45e3-8c5b-cb2f3c581064/call-extract/chr16.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/53b4826d-db57-4b18-b7bf-176911a78835/call-extract/chr17.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/a0d1bcde-ca89-4f9f-ab8d-0168fd067bee/call-extract/chr18.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/79dabd9a-71ef-4fd4-8ebf-712543bf8147/call-extract/chr19.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/12781c9c-5574-4477-9e28-fe35dfbe6827/call-extract/cacheCopy/chr20.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/707174f3-c463-47b4-a59e-a69520539174/call-extract/cacheCopy/chr21.dose.emerge_ids.consented.merged-loi.vcf","gs://fc-f863c5a2-db68-4521-9f30-03debe9e49ab/b828a8c6-b402-4493-a8be-8a87a7df4cb1/locus_extract/b4772eaf-23bc-4eff-8a51-5aa7895b2169/call-extract/cacheCopy/chr22.dose.emerge_ids.consented.merged-loi.vcf"]

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
