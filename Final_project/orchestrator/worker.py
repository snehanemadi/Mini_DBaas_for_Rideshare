#!/usr/bin/python
import os
import json
import string
import requests
import datetime
import time
import pika
import random
import uuid
import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState


from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker

from flask import Flask, request, jsonify, abort, Response, send_file
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

from requests.exceptions import HTTPError
from apscheduler.schedulers.background import BackgroundScheduler


logging.basicConfig()

app = Flask(__name__)
ma=Marshmallow(app)

engine = create_engine('sqlite:///user.db', echo=True)
Base = declarative_base()

#user table
class User(Base):
	__tablename__ = "user"
	username = Column(String, unique=True, primary_key=True)
	password = Column(String, unique=True)

	def __init__(self, username, password):
		self.username = username
		self.password = password

Base.metadata.create_all(engine)

#marshmallow schema to convert table format to json
class UserSchema(ma.Schema):
	class Meta:
		fields = ('username','password')

user_schema=UserSchema()
users_schema=UserSchema(many=True)

#ride table
class Ride(Base):
	__tablename__ = "ride"
	id = Column(Integer, unique=True, primary_key=True)
	created_by = Column(String)
	timestamp = Column(String)
	source = Column(Integer)
	destination = Column(Integer)
	users = Column(String)

	def __init__(self, created_by, timestamp, source, destination, users):
		self.created_by = created_by
		self.timestamp = timestamp
		self.source = source
		self.destination = destination
		self.users = users

Base.metadata.create_all(engine)

#marshmallow schema to convert table format to json
class RideSchema(ma.Schema):
	class Meta:
		# Fields to expose
		fields = ('id','created_by', 'timestamp', 'source', 'destination', 'users')

ride_schema = RideSchema()
rides_schema = RideSchema(many=True)


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()


#sync object to use for syncing data between master and slaves after every write.Using Rabbitmq Fanout exchange for this purpose
class sync(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            		pika.ConnectionParameters(host='rmq'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange = 'syncQ',exchange_type = 'fanout')

    def call(self,message):
        self.channel.basic_publish(
            exchange='syncQ',
            routing_key='',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))

#copy object to copy the whole data written to master db till the current time to the new slave spawned 
class copy(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
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

	def call(self):
		print("called func")
    	
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
            exchange='',
            routing_key='copy_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps({'nouse':'abc'}))
		while self.response is None:
			self.connection.process_data_events()
		return (self.response)
	
	def close(self):
		self.channel.close()


#on_request function called in slave when a read request is published into the readQ.The output of this is sent into ResponseQ

def on_request(ch, method, props, body):
	Session = sessionmaker(bind=engine)
	session = Session()
	data=json.loads(body)

	table=data['table']
	if(table == 'User'):
		if(data['col']==1):
			username=data['username']
			existing_username = session.query(User).filter(User.username == username).all()
			response = users_schema.dump(existing_username)
		
		elif(data['col']==2):
			password=data['password']
			existing_password = session.query(User).filter(User.password == password).all()
			response = users_schema.dump(existing_password)
		
		elif(data['col']==3):
			all_users = session.query(User).all()
			response = users_schema.dump(all_users)

	elif(table == 'Ride'):
		if(data['col']==0):
			ride_id = data['ride_id']
			records = session.query(Ride).filter(Ride.id == ride_id).all()
			response = ride_schema.dump(records[0])
			'''
			res=records[0]

			response={}
			response["ride_id"]=res["id"]
			response["created_by"]=res["created_by"]
			response["timestamp"]=res["timestamp"]
			response["source"]=res["source"]
			response["destination"]=res["destination"]
			response["users"]=res["users"]
			'''
		
		elif(data['col']==1):
			ride_id=data['ride_id']
			existing_ride_id = session.query(Ride).filter(Ride.id == ride_id).all()
			response = rides_schema.dump(existing_ride_id)
		
		elif(data['col']==2):
			src = data['source']
			dst = data['destination']
			src=int(src)
			dst=int(dst)

			result=session.query(Ride).filter(Ride.source == src).filter(Ride.destination == dst).all()
			result_in_json = rides_schema.dump(result)
			print(result_in_json)
			response={}
		
			i=0
			for res in result_in_json:
				response[i]=res
				i=i+1

		elif(data['col']==3):
			allrides = session.query(Ride).all()
			response = rides_schema.dump(allrides)	


	ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=json.dumps(response))
	ch.basic_ack(delivery_tag=method.delivery_tag)

#callback function is called in master when a write request is made.The given contents are written into master's db.
def callback(ch, method, properties, body):
	Session = sessionmaker(bind=engine)
	session = Session()
	data=json.loads(body)

	if(data['method']=='POST'):
		if(data['table'] == 'User'):
			if(data['new']==1):
				username = data['username']
				password = data['password']
				new_user = User(username, password)
				session.add(new_user)
				session.commit()
			else:
				column = data['column']
				value = data['value']
				username = data['username']
				user = session.query(User).get(username)
				user.password = value
				session.commit()

		elif(data['table'] == 'Ride'):
			if(data['new']==1):
				created_by = data['created_by']
				timestamp = data['timestamp']
				source = data['source']
				destination = data['destination']
				users = data['users']

				new_ride = Ride(created_by, timestamp, source, destination, users)
				session.add(new_ride)
				session.commit()
			else:
				column = data['column']
				value = data['value']
				ride_id = data['ride_id']
				ride = session.query(Ride).get(ride_id)
				if(column == 'users'):
					ride.users = value
				elif(column == 'source'):
					ride.source = value
				elif(column == 'destination'):
					ride.destination = value
				session.commit()

	elif(data['method']=='DELETE'):
		if(data['table'] == 'User'):
			if(data['col']==1):
				username = data['username']
				user = session.query(User).get(username)
				session.delete(user)
				session.commit()
			elif(data['col']==2):
				session.query(User).delete()
				session.commit()
		elif(data['table'] == 'Ride'):
			if(data['col']==1):
				ride_id = data['ride_id']
				ride = session.query(Ride).get(ride_id)
				session.delete(ride)
				session.commit()
			elif(data['col']==2):
				session.query(Ride).delete()
				session.commit()
	
	instance1=sync()  #after writing in the master's db a request is sent to syncQ by invoking call() function.
	instance1.call(data)


#sync_callback is function at the consuming end of the sync requests.As soon as it gets the sync request it commits the writes request even in slaves's db .
def sync_callback(ch,method,props,body):
	print(" in sync now")
	Session = sessionmaker(bind=engine)
	session = Session()
	data=json.loads(body)

	if(data['method']=='POST'):
		if(data['table'] == 'User'):
			if(data['new']==1):
				username = data['username']
				password = data['password']
				new_user = User(username, password)
				session.add(new_user)
				session.commit()
			else:
				column = data['column']
				value = data['value']
				username = data['username']
				user = session.query(User).get(username)
				user.password = value
				session.commit()

		elif(data['table'] == 'Ride'):
			if(data['new']==1):
				created_by = data['created_by']
				timestamp = data['timestamp']
				source = data['source']
				destination = data['destination']
				users = data['users']

				new_ride = Ride(created_by, timestamp, source, destination, users)
				session.add(new_ride)
				session.commit()
			else:
				column = data['column']
				value = data['value']
				ride_id = data['ride_id']
				ride = session.query(Ride).get(ride_id)
				if(column == 'users'):
					ride.users = value
				elif(column == 'source'):
					ride.source = value
				elif(column == 'destination'):
					ride.destination = value
				session.commit()

	elif(data['method']=='DELETE'):
		if(data['table'] == 'User'):
			if(data['col']==1):
				username = data['username']
				user = session.query(User).get(username)
				session.delete(user)
				session.commit()
			elif(data['col']==2):
				session.query(User).delete()
				session.commit()
		elif(data['table'] == 'Ride'):
			if(data['col']==1):
				ride_id = data['ride_id']
				ride = session.query(Ride).get(ride_id)
				session.delete(ride)
				session.commit()
			elif(data['col']==2):
				session.query(Ride).delete()
				session.commit()

#writedb_inslave is the function on the consuming end of the response queue associated with copy queue in slave.It commits the response in slaves'db sent by master on start of slave.
def writedb_inslave(responsedb):
	Session = sessionmaker(bind=engine)
	session = Session()
	print("in writetodb in slave")
	data=json.loads(responsedb)
	status=data['status']
	user_data=data['user']
	ride_data=data['ride']
	print("result in slave")
	print(status)
	#print(res)
	for u in user_data:
		username = u['username']
		password = u['password']
		new_user = User(username, password)
		session.add(new_user)  #adding the tuple of column values in this format(by using marshmallow schema) 
		session.commit()   #adding to the db

	for r in ride_data:
		created_by = r['created_by']
		timestamp = r['timestamp']
		source = r['source']
		destination = r['destination']
		users = r['users']

		new_ride = Ride(created_by, timestamp, source, destination, users)
		session.add(new_ride)
		session.commit()
		'''
		user=u["username"]
		pwd=u["password"]
		u1=User(user,pwd)
		session.add(u1)
		session.commit()
		'''
	return 1

#This function in master is at consuming end of copy queue where requests are sent by slave as soon as it is spawned for copying the master's db.
def on_request_copy_master(ch, method, props, body):
	Session = sessionmaker(bind=engine)
	session = Session()
	data=json.loads(body)
	
	#send this data to new slave 
	user_data=session.query(User).all()
	user_data_list = users_schema.dump(user_data)
	ride_data=session.query(Ride).all()
	ride_data_list = rides_schema.dump(ride_data)
	response={'status':'ok','user':user_data_list,'ride':ride_data_list}
	ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=json.dumps(response))
	ch.basic_ack(delivery_tag=method.delivery_tag)

#checking the environmnet variable set while creating container to know if its slave or master
if(os.environ['WORKER'] == 'MASTER'):

	channel.queue_declare(queue='copy_queue', durable=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='copy_queue', on_message_callback=on_request_copy_master)

	channel.queue_declare(queue='writeQ', durable=True)
	channel.basic_consume(queue='writeQ', on_message_callback=callback,auto_ack=True)

	channel.start_consuming()


elif(os.environ['WORKER'] == 'SLAVE'):
	#slave is set up and on start its sending request to copy the master's db.

	instance = copy()
	responsedb = instance.call()
	print(" [.] Got %r" % responsedb)
	writedb_inslave(responsedb)

	#connecting to zookeeper and creating up znode.
	###################creating a kazoo client
	zk = KazooClient(hosts='zoo:2181')
	zk.start()

	# Ensure a path, create if necessary
	zk.ensure_path("/producer")

	letters = string.ascii_lowercase
	name = ''.join(random.choice(letters) for i in range(4))
	nodename="/producer/"+name
	
	#################znode creation in slave
	# Create a node with data
	if zk.exists(nodename):
		print("Node already exists")
	else:
		zk.create(nodename, b"demo producer node",ephemeral=True)

	# Print the version of a node and its data
	data, stat = zk.get(nodename)
	print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

	#################
	#connecting to the task queue where read requests are published to.
	channel.queue_declare(queue='task_queue', durable=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='task_queue', on_message_callback=on_request)

	#connecting to the syncQ
	channel.exchange_declare(exchange = 'syncQ',exchange_type = 'fanout')
	result = channel.queue_declare(queue='',exclusive=True)
	queue_name = result.method.queue
	channel.queue_bind(exchange='syncQ', queue=queue_name)
	channel.basic_consume(queue=queue_name, on_message_callback=sync_callback, auto_ack=True)
	print(" [x] Awaiting RPC requests")
	channel.start_consuming()
else:
	print("no worker variable set")



