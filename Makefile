# Makefile for Spark Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/usr/local/Cellar/apache-spark/2.2.1
app.name=PageRankSpark
jar.name=PageRankSpark.jar
#jar.path=target/${jar.name}
maven.jar.name=PageRankSpark-1.0-SNAPSHOT.jar
job.name=PageRankSpark
local.master=local[4]
local.input=input
local.output=output
local.log=logs
num.iter=10
k=100

# AWS EMR Execution
aws.release.label=emr-5.11.1
aws.region=us-east-1
aws.bucket.name=cs6240-hw4-emilyjean
aws.subnet.id=subnet-23f40b2c
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone.
local: jar clean-local-output
	spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output}

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Page Rank Spark 11" \
		--release-label ${aws.release.label} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Package for release.
distro:
	rm SparkPageRank.tar.gz
	rm SparkPageRank.zip
	rm -rf build
	mkdir -p build/deliv/SparkPageRank/main/scala/pagerank
	cp -r src/main/scala/pagerank/* build/deliv/SparkPageRank/main/scala/pagerank
	cp pom.xml build/deliv/SparkPageRank
	cp Makefile build/deliv/SparkPageRank
	cp README.txt build/deliv/SparkPageRank
	tar -czf SparkPageRank.tar.gz -C build/deliv SparkPageRank
	cd build/deliv && zip -rq ../../SparkPageRank.zip SparkPageRank
