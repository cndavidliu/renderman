# Spark Cloud Computing

## Class Definition
- Job:
	- id: primary key
	- name: string
	- state: in truple:[finish,fail,running,wait]
	- user: foreign key user.id
	- source file: string path from local to HDFS
	- JobType: [1,2] 
	- started_at: Time
	- finished_at: Time
	- description: string
	- extraInfo: string, some other info need to store
	- instance_mem: Integer, memory Spark using
	- instance_cores: Integer, cores Spark using
- User:
	- id: primary key
	- name: nicky name string
	- password: string
	- sex: male or female
	- email: string (need email format detect)
	- age: Integer
	- description: string
- JobManager:
	- Job Queue: New Job Queue And Failed Job Queue
	- Runing Job Thread Array
	- any API needed by WebServer

## Cluster
	- Three computer with Spark 0.90 and Hadoop 1.0.4
	- each Computer with 32g mem and 6 cores 
