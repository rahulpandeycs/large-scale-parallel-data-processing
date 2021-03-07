# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/home/rahul/Documents/Fall2020/LSPDP/Softwares/hadoop-2.9.1
jar.name=mr-demo-1.0.jar
jar.path=target/${jar.name}
jobRJ.name=twitter.TwitterReplicatedJoin
jobRS.name=twitter.TwitterReduceSideJoinWithMaxFilter
jobAnalysis.name=twitter.CountAnalysisTwitterReduceSideJoinWith2HopPathCount
local.input=input
local.output=output
local.outputRS=outputRS2
local.outputRJ=outputRJ2
local.outputAnalysis=outputAnalysis

local.intermediate=intermediateOutput

# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output

# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.region=us-east-1
#s3://fall2020-hw2-lspdp/input
aws.bucket.name=fall2020-hw2-lspdp
 #s3://fall2020-lspdp/input/
aws.input=input
#s3://fall2020-lspdp/output/
aws.output=output
aws.outputRS=outputRS2
aws.outputRJ=outputRJ2
aws.outputAnalysis=outputAnalysis
aws.intermediate=intermediateOutput
#s3://fall2020-lspdp/log/
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}
	rm -rf ${local.intermediate}*

clean-local-outputRS:
	rm -rf ${local.outputRS}
	rm -rf ${local.intermediate}*

clean-local-outputRJ:
	rm -rf ${local.outputRJ}
	rm -rf ${local.intermediate}*

clean-local-outputAnalysis:
	rm -rf ${local.outputAnalysis}*
	rm -rf ${local.intermediate}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.output}

localRS: jar clean-local-outputRS
	${hadoop.root}/bin/hadoop jar ${jar.path} ${jobRS.name} ${local.input} ${local.outputRS}

localRJ: jar clean-local-outputRJ
	${hadoop.root}/bin/hadoop jar ${jar.path} ${jobRJ.name} ${local.input} ${local.outputRJ}

localAnalysis: jar clean-local-outputAnalysis
	${hadoop.root}/bin/hadoop jar ${jar.path} ${jobAnalysis.name} ${local.input} ${local.outputAnalysis}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

download-outputRS-hdfs: clean-local-outputRS
	mkdir ${local.outputRS}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.outputRS}

download-outputRJ-hdfs: clean-local-outputRJ
	mkdir ${local.outputRJ}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.outputRJ}

download-outputAnalysis-hdfs: clean-local-outputAnalysis
	mkdir ${local.outputAnalysis}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.outputAnalysis}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.intermediate}*"

delete-outputRS-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.outputRS}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.intermediate}*"


delete-outputRJ-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.outputRJ}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.intermediate}*"

delete-outputAnalysis-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.outputAnalysis}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.intermediate}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "HW2-TwitterJoin" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

awsRS: jar upload-app-aws delete-outputRS-aws
	aws emr create-cluster \
		--name "HW2-RS-TwitterJoin" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${jobRS.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.outputRS}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

awsRJ: jar upload-app-aws delete-outputRJ-aws
	aws emr create-cluster \
		--name "HW2-RJ-TwitterJoin" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${jobRJ.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.outputRJ}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate


awsAnalysis: jar upload-app-aws delete-outputAnalysis-aws
	aws emr create-cluster \
		--name "HW2-Analysis-TwitterJoin" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${jobAnalysis.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.outputAnalysis}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate


# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Download output from S3.
download-outputRS-aws: clean-local-outputRS
	mkdir ${local.outputRS}
	aws s3 sync s3://${aws.bucket.name}/${aws.outputRS} ${local.outputRS}

# Download output from S3.
download-outputRJ-aws: clean-local-outputRJ
	mkdir ${local.outputRJ}
	aws s3 sync s3://${aws.bucket.name}/${aws.outputRJ} ${local.outputRJ}

# Download output from S3.
download-outputAnalysis-aws: clean-local-outputAnalysis
	mkdir ${local.outputAnalysis}
	aws s3 sync s3://${aws.bucket.name}/${aws.outputAnalysis} ${local.outputAnalysis}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f MR-Demo.tar.gz
	rm -f MR-Demo.zip
	rm -rf build
	mkdir -p build/deliv/MR-Demo
	cp -r src build/deliv/MR-Demo
	cp -r config build/deliv/MR-Demo
	cp -r input build/deliv/MR-Demo
	cp pom.xml build/deliv/MR-Demo
	cp Makefile build/deliv/MR-Demo
	cp README.txt build/deliv/MR-Demo
	tar -czf MR-Demo.tar.gz -C build/deliv MR-Demo
	cd build/deliv && zip -rq ../../MR-Demo.zip MR-Demo
