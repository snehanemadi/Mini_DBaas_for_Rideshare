#!/usr/bin/python
from flask import Flask, request, jsonify, abort, Response
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

from requests.exceptions import HTTPError

from apscheduler.schedulers.background import BackgroundScheduler

from kazoo.client import KazooClient
from kazoo.client import KazooState


import os
import json
import string
import requests
import datetime
import time
import pika
import sys
import uuid
import docker
import time
import math
import logging


logging.basicConfig()

#to check if watch function is being called on deletion of znode during scaling down
global scaling_down_happening
scaling_down_happening = False

#to count the number of read requests
global counter
counter = 0

#to count the number of slaves currently running
global no_slaves
no_slaves = 1

#to count the number of children znodes under the parent /producer znode
global no_children
no_children=0

#############################################################-----  ZOOKEEPER CODE  -----##########################################################################

#instantiating a kazoo client

zk = KazooClient(hosts='zoo:2181')
zk.start()
zk.ensure_path('/producer')

#function to create a slave, called on failure

def create_slave_on_failure():
	print("slave created on failure!!!!!!!!!!!!!!!!!!!")
	cont= client.containers.run(
        privileged=True,
		image = 'worker:latest',
		command = 'sh -c "sleep 20 && python3 worker.py"',
		environment = ["WORKER=SLAVE"],
		links={'rabbitmq':'rmq','zookeeper':'zoo'},
		network='ubuntu_default',
		detach=True
		)
	
	return 1

#ChildrenWatch API as a decorator for the watch function
#watch function to handle restart on slave failure

@zk.ChildrenWatch('/producer')
def watch_func(children):
	print("++++++++++++++++++++++++++++++++++++++++++++++++++inside children watch+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	global no_children
	global scaling_down_happening
	global no_slaves

	if(len(children)>no_children): #if watch function is called on addition of a new znode under /producer i.e when a new slave is created
		if(type(children)==list):
			no_children=no_children+1
		print("new child has been added!")
		print("current children are...")
		print(no_children)
		print(children)

	elif(len(children)<no_children): #if watch function is called on the deletion of a znode under /producer i.e when a slave is killed
		no_children=len(children)
		print("a child has been deleted")
		print("current children are...")
		print(children)
		print(no_children)
	
		if(not scaling_down_happening): #if the slave killed is not for scaling down then creating new slave.
			res=create_slave_on_failure()
			print("new slave created successfully")

		else: #if the slave killed is for scaling down,we need not create new slave.
			if(len(children)==no_slaves):
				scaling_down_happening=False
			print('not a failure....scaling down is happening')
	
	return True

#to set the watch function on the parent znode /producer

c = zk.get_children("/producer", watch=watch_func)


#############################################################-----  ZOOKEEPER CODE END -----##########################################################################

#############################################################----- FLASK APP & RABBITMQ -----#######################################################################


#instantiating the flask app and docker SDK clients

app = Flask(__name__)

client  = docker.DockerClient(base_url='unix:///var/run/docker.sock')
client1 = docker.APIClient(base_url='unix:///var/run/docker.sock') 

#class to declare the read and response queue and send and recieve messages from it

class read(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, message):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(message))
        while self.response is None:
            self.connection.process_data_events()
        return (self.response)

#class to declare the write queue and publish messages to it

class write(object):
	def __init__(self):
		self.connection = pika.BlockingConnection(
            		pika.ConnectionParameters(host='rmq'))

		self.channel = self.connection.channel()
		self.channel.queue_declare(queue='writeQ',durable=True)
	def call(self,message):
		self.channel.basic_publish(
        		exchange='',
        		routing_key='writeQ',
        		body=json.dumps(message),
        		properties=pika.BasicProperties(
            		delivery_mode=2,  # make message persistent

        		))

#function to create a slave, called when master is killed

def create_slave_on_kill_master():
	print("slave created on kill of master!!!!!!!!!!!!!!!!!!!")
	cont= client.containers.run(
        privileged=True,
		image = 'worker:latest',
		command = 'sh -c "sleep 20 && python3 worker.py"',
		environment = ["WORKER=SLAVE"],
		links={'rabbitmq':'rmq','zookeeper':'zoo'},
		network='ubuntu_default',
		detach=True
		)
	no_slaves = no_slaves+1
	return 1

#job_function called every two minutes for scaling

def job_function():
	print("in job junction.................")
	currentDT = datetime.datetime.now()
	print(currentDT)

	global counter
	global no_slaves
	temp = counter
	check = 1 #to check for number of slaves to be scaled up or down

	if((0<temp<=20) != True):#to calculate check based on number of requests
		temp1 = math.floor(temp/20)
		check = check+temp1

	print("# slaves should be there vs # slaves that are existing")
	print(check)
	print(no_slaves)

	if(no_slaves < check):#if number of slaves exisiting is less than number of slaves required

		for i in range(0,(check-no_slaves)):
			print("Scaling UP........************........")
			cont= client.containers.run(
					privileged=True,
					image = 'worker:latest',
					command = 'sh -c "sleep 20 && python3 worker.py"',
					environment = ["WORKER=SLAVE"],
					links={'rabbitmq':'rmq','zookeeper':'zoo'},
					network='ubuntu_default',
					detach=True
					)
			no_slaves = no_slaves+1

	print("# slaves should be there vs # slaves are existing, if they are same then no scale down")
	print(check)
	print(no_slaves)

	if(no_slaves > check): #if number of slaves exisiting is more than number of slaves required, then need to scale down

		global scaling_down_happening
		scaling_down_happening = True
		cont_pid=[]
		for container in client.containers.list():

			if(container.name != 'orchestrator' and container.name != 'zookeeper' and  container.name != 'rabbitmq' and container.name != 'master'):
				d={}
				d['0']=container.id
				d['1']=container.attrs['State']['Pid']
				cont_pid.append(d)
		print(cont_pid)
		for i in range(0,(no_slaves-check)):

			print("scaling DOWN..........*********..........")
			sorted_list=sorted(cont_pid,key = lambda x:x['1'],reverse=True)
			max_id = sorted_list[0]['0']  #killing the slave with maximum pid
			client1.kill(max_id)
			cont_pid.remove(sorted_list[0]) #remove from the local list

			no_slaves=no_slaves-1

	print("# slaves should be there and # slaves are existing ..... both should be same")
	print(check)
	print(no_slaves)

	counter = 0 #each time counter is setting to 0 to decrease and increase the number of slaves for each 2 minutes
global first_request 
first_request= True

@app.route("/api/v1/db/read", methods=["POST"])#read API
def read_from_db():
	global counter
	global first_request
	counter = counter + 1
	if(counter ==1 and first_request):
		sched = BackgroundScheduler(daemon=True)
		sched.add_job(job_function,'interval',minutes=2)
		sched.start()
		first_request=False
		
	message = request.get_json()
	instance = read()
	response = instance.call(message)
	print("waiting for response")
	print(response)
	print(" [.] Got %r" % response)
	data = json.loads(response)
	return jsonify(data)  


@app.route("/api/v1/db/write", methods=["POST","DELETE"])#write API
def write_to_db():
	
	message = request.get_json()
	message['method'] = request.method

	instance=write()
	instance.call(message)
	print(" [x] Sent %r" % message)
	return {"status":"done"}

@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	#global no_slaves
	cont_pid=[]
	for container in client.containers.list():
		if(container.name != 'orchestrator' and container.name != 'zookeeper' and  container.name != 'rabbitmq' and container.name != 'master'):
			d={}
			d['0']=container.id
			d['1']=container.attrs['State']['Pid']
			cont_pid.append(d)
	#max_id = max(cont_pid)
	sorted_list=sorted(cont_pid,key = lambda x:x['1'],reverse=True)
	max_id = sorted_list[0]['0']
	client1.kill(max_id)
	#no_slaves = no_slaves-1
	l=[]
	l.append(sorted_list[0]['1'])

	return jsonify(l)

@app.route("/api/v1/get_slaves",methods=["GET"])
def get_slave():
	children = zk.get_children("/producer")
	print(children)
	return {"status":"done"}	


@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
	master_pid=0
	for container in client.containers.list():
		if(container.name=='master'):
			master_pid = container.attrs['State']['Pid']
			client1.kill(container.id)
			status_func = create_slave_on_kill_master()
	l=[]
	l.append(master_pid)
	return jsonify(l)

@app.route("/api/v1/worker/list",methods=["GET"])
def list_workers():
	cont_pid=[]
	for container in client.containers.list():
		if(container.name != 'orchestrator' and container.name != 'zookeeper' and  container.name != 'rabbitmq'):
			#print(container.name)
			cont_pid.append(container.attrs['State']['Pid'])
	cont_pid.sort()
	print(cont_pid)
	return jsonify(cont_pid)



print("########################################")
print(no_slaves)
print("#######################################")
#print(cont_pid)


print("CURRENT RUNNING CONTAINERS ARE")

for container in client.containers.list():
    print (container.name)

print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11")


print("CREATING CONTAINER 1 - MASTER")
container1 = client.containers.run(
    privileged=True,
    image = 'worker:latest',
    name = 'master',
    command = 'sh -c "sleep 20 && python3 worker.py"',
    environment = {'WORKER':'MASTER'},
    links={'rabbitmq':'rmq','zookeeper':'zoo'},
    network='ubuntu_default',
    detach=True
)

print("CURRENT RUNNING CONTAINERS ARE")

for container in client.containers.list():
    print (container.name)


container2 = client.containers.run(
    privileged=True,
    image = 'worker:latest',
    name = 'slave1',
    command = 'sh -c "sleep 20 && python3 worker.py"',
    environment = ["WORKER=SLAVE"],
    links={'rabbitmq':'rmq','zookeeper':'zoo'},
    network='ubuntu_default',
    detach=True
)


print("CURRENT RUNNING CONTAINERS ARE")

for container in client.containers.list():
    print (container.name)


app.run(debug=False, host='0.0.0.0', port=80, threaded=True, use_reloader=False)
