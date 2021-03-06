# CS6240-MR-template
Template for MR Java projects

Fall 2018

Code author
-----------
Joe Sackett

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
8) Standalone Hadoop: (Reduce side Join)
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make localRS
9) Standalone Hadoop: (Replicated Join)
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make localRJ
10) Standalone Hadoop: (analysis)
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make localAnalysis
11) AWS EMR Hadoop: (Reduce Side Join) (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make awsRS					-- check for successful execution with web interface (aws.amazon.com)
	download-outputRS-aws			-- after successful execution & termination
12) AWS EMR Hadoop: (Replicated Join)(you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make awsRJ					-- check for successful execution with web interface (aws.amazon.com)
	download-outputRJ-aws			-- after successful execution & termination
13) AWS EMR Hadoop: (Analysis) (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make awsAnalysis					-- check for successful execution with web interface (aws.amazon.com)
	download-outputAnalysis-aws			-- after successful execution & termination			