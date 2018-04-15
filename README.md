# PageRank in Spark

Data: /input (bz2 files. Local run = simple, EMR run = other 4)

Spark programs: /pagerank
AWS output: aws-output

Files:
- MyParser: decompresses bz2 file and parses Wikipages on each line
- PageRankSpark: calculates the page ranks for 10 iteratations and the top 100 page ranks


1. Running locally (cmdline & loading in eclipse) - Spark Programs:
    - Open the pagerank project in Eclipse IDE (File, Import, Existing Maven Project)
    - create an input folder under the project root folder, and pull dataset(s) and put in the input folder
    - In the MAKEFILE, set the following for your env (and EMR):
        - spark.root
        - aws.emr.release
        - aws.emr.release
        - aws.region
        - aws.bucket.name
        - aws.instance.type
    - run command: make switch-standalone
    - run command: make alone
    - after run is complete, check console and output folder

2. Running on EMR:
    - Create bucket on S3 with data in an input folder
	- Edit MAKEFILE: bucket.name, aws.subnet.id, aws.instance.type (currently set to m4.large)
	- run: make cloud
	- Once completed, check syslog, controller, and output files
    - Through EMR Web Interface:
    	- go to EMR, create cluster as follows (US-EAST-1, N Virgina):
    	- follow Joe's instructions for creating cluster: https://docs.google.com/document/d/1-UjNVFasTSzhAaqLtKSmeie6JZMinhtVEqCEZwUkxeE/edit
    - Download output files with the following command: make download-output-aws
