README

Authors: Alan Paul Paragas, Jacob Joseph, Cynthia Hsieh

-----SUFS Application-----

The Seattle University File System (SUFS) is a distributed file system similar to the Hadoop File System (HDFS). The SUFS will allow users to create (and write) new files, read a specific file, list the data nodes (DNs) that store replicas of each block of a file. The data nodes will be fault tolerant. With 3 replicas of each block of a file, the system will be able to handle the concurrent failure of 2 data nodes. However, the SUFS will differ from HDFS, mainly in the range of functionality it offers. The SUFS will not support deletion of nodes, handling a name node (NN) failure, checksumming data blocks, cluster rebalancing, configurable block size and replication factor, authentication, nor the tracking of file edits. The NN and DNs will run on AWS EC2 instances, and the client program will support writing files stored in AWS S3 buckets the client has access to. 

Prerequisites

EC2 Instance Running
T2.micro spot instance type
1 vCPU
Linux 2 Image (Dependency libs: python3 w/pip3, flask, flask-restful,
boto3, apscheduler==2.1.2, http.client)
8GB memory storage
Inbound open Traffic allow open ports(5000, 80, 22)],
S3 Storage

Running SUFS Application

- spin up EC2 Instances
	- 1 Name Node
	- N Data Nodes
	
-------Client----------

	Manual: python3 client.py [name node ip] [name node port]
		
-------Name Node-------

	Manual:
		ssh into EC2
		cd into SUFS dir
		sudo killall flask (stop and previously running flask)
		execute: python3 name_node.py
	Automate:
		Allow instance to use run User-Data script<
		#!/bin/bash
		export FLASK_APP=PATH/TO/name_node.py
		flask run --host=0.0.0.0>
		
-------Data Node-------

	Manual:
		ssh into EC2
		cd into SUFS dir
		sudo killall flask (stop and previously running flask)
		execute: python3 data_node.py
	Automate:
		Allow instance to use run User-Data script<
		#!/bin/bash
		export IP_HOST=******INSERT HOST IP********
		export FLASK_APP=PATH/TO/data_node.py
		flask run --host=0.0.0.0>
	To test replication feature (stop application/shutdown node)

STOPPING APPLICATION(NODE)
- ctrl + c during manual instance
- terminate ec2 instance

DEBUGGING AUTOMATION
-if any issue occurs during automation check cloud logs
	- execute : cat /var/log/cloud-init-output.log
