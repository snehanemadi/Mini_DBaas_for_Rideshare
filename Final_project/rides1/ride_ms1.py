from flask import Flask, request, jsonify, abort, Response
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os
import json
import string
import requests
import datetime
import time
from requests.exceptions import HTTPError
from flask_track_usage import TrackUsage
from flask_track_usage.storage.sql import SQLStorage


app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'ride1.sqlite')
db = SQLAlchemy(app)
ma = Marshmallow(app)


pstore = SQLStorage(db = db)
t=TrackUsage(app, [pstore])
'''
class Ride(db.Model):
	id = db.Column(db.Integer, unique=True, primary_key=True)
	created_by = db.Column(db.String)
	timestamp = db.Column(db.String)
	source = db.Column(db.Integer)
	destination = db.Column(db.Integer)
	users = db.Column(db.String)

	def __init__(self, created_by, timestamp, source, destination, users):
		self.created_by = created_by
		self.timestamp = timestamp
		self.source = source
		self.destination = destination
		self.users = users


class RideSchema(ma.Schema):
	class Meta:
		# Fields to expose
		fields = ('id','created_by', 'timestamp', 'source', 'destination', 'users')

ride_schema = RideSchema()
rides_schema = RideSchema(many=True)



@t.exclude
@app.route("/api/v1/db/read", methods=["POST"])#API9
def read_from_db():
	table =  request.get_json()['table']#to choose which table to read from
	if(table == 'Ride'):
		
		if(request.get_json()['col']==0):
			ride_id = request.json['ride_id']
			res = Ride.query.get(ride_id) #get all row values of a row with that ride_id(primary key)

			response_dict={}
			response_dict["ride_id"]=res.id
			response_dict["created_by"]=res.created_by
			response_dict["timestamp"]=res.timestamp
			response_dict["source"]=res.source
			response_dict["destination"]=res.destination
			response_dict["users"]=res.users

		elif(request.get_json()['col']==1):
			ride_id = request.json['ride_id']
			existing_ride_id = Ride.query.filter(Ride.id == ride_id).all()
			in_json = rides_schema.dump(existing_ride_id)
			return jsonify(in_json)

		elif(request.get_json()['col']==2): #to get details when src and dst is given
			src = request.get_json()['source']
			dst = request.get_json()['destination']
			src=int(src)
			dst=int(dst)

			result=Ride.query.filter(Ride.source == src).filter(Ride.destination == dst).all()
			result_in_json = rides_schema.dump(result)
			response_dict={}
		
			i=0
			for res in result_in_json:
				response_dict[i]=res
				i=i+1
		elif(request.get_json()['col']=='data'):
			ride_id = request.json['ride_id']
			existing_ride = Ride.query.filter(Ride.id == ride_id).all()
			result_in_json = rides_schema.dump(existing_ride)
			return jsonify(result_in_json)

		elif(request.get_json()['col']==3):
			allrides=Ride.query.all()
			result = rides_schema.dump(allrides)
			return jsonify(result)

	return jsonify(response_dict)

@t.exclude
@app.route("/api/v1/db/write", methods=["POST","DELETE"])#API8
def write_to_db():
	table =  request.get_json()['table']#to choose which table to to write to

	if(request.method == 'POST'):
		if(table == 'Ride'):
			new = request.get_json()['new']
			if(new == 1):#inserting new row given details
				created_by = request.get_json()['created_by']
				timestamp = request.get_json()['timestamp']
				source = request.get_json()['source']
				destination = request.get_json()['destination']
				users = request.get_json()['users']

				new_ride = Ride(created_by, timestamp, source, destination, users)

				db.session.add(new_ride)
				db.session.commit()

			else:#to update users or source or details given column name and updated value
				column = request.get_json()['column']
				value = request.get_json()['value']
				ride_id = request.get_json()['ride_id']
				ride = Ride.query.get(ride_id)
				if(column == 'users'):
					ride.users = value
				elif(column == 'source'):
					ride.source = value
				elif(column == 'destination'):
					ride.destination = value
				db.session.commit()

		
	elif(request.method == 'DELETE'):
		if(table == 'Ride'):
			if(request.get_json()['col']==1):
				ride_id = request.get_json()['ride_id']
				ride = Ride.query.get(ride_id)
				db.session.delete(ride)
				db.session.commit()
			elif(request.get_json()['col']==2):
				Ride.query.delete()
				db.session.commit()
				return {"deletion":"done"}
	return {"status":"done"}
'''


#create a ride
@app.route("/api/v1/rides", methods=["POST"]) #API3
def create_ride():

	createdby = request.get_json()['created_by']
	timestamp = request.get_json()['timestamp']
	source = request.get_json()['source']
	destination = request.get_json()['destination']
	username=createdby
	
	source = int(source)
	destination = int(destination)

	url = 'http://35.171.38.208/api/v1/db/write'

	if(request.method != 'POST'):
		abort(400, description="this method is not allowed")

	else:
		try:
			datetime.datetime.strptime(timestamp, '%d-%m-%Y:%S-%M-%H')
		except ValueError:
			abort(400,description="timestamp format not correct")
			#raise ValueError("Incorrect data format, should be dd-mm-yyyy:ss-mm-hh")

	my_time = datetime.datetime.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")

	urlr = 'http://35.171.38.208/api/v1/db/read'
	
	#existing_username = requests.post(url=urlr, json={"table":"User","col":1,"username":username})

	headers = {'Origin': 'http://52.7.85.48:80'}#change
	allusers = requests.get(url = 'http://project-load-balancer-795597639.us-east-1.elb.amazonaws.com/api/v1/users', headers=headers)#public dns of load balancer should come here
	#print(allusers)
	allusers1 = allusers.json()
	#print(allusers1)
	

	present=False
	for name in allusers1:
		if(name==username):
			present=True


	if (datetime.datetime.today() - my_time).days > 0:
		abort(400, description="you can't create ride for date that is over")
		
	elif (source==destination):
		abort(400,description="same source and destination is not allowed")
	elif (source<1 or source>198):
		abort(400,description="source is invalid")
	elif (destination<1 or destination>198):
		abort(400,description="destination is invalid")

	elif present:
		response = requests.post(url=url, json={"table": "Ride", "new": 1, "created_by": createdby, "timestamp": timestamp , "source":source , "destination":destination , "users":""})
        
		return jsonify(),201
	else:
		abort(400, description="username doesn't exist")


#query string: 0.0.0.0:80/api/v1/rides?source=13&destination=15
@app.route("/api/v1/rides", methods=["GET"])#API4
def list_upcompingrides():
	print("inside upcomin")
	src = request.args.get('source')
	dst = request.args.get('destination')
	src=int(src)
	dst=int(dst)

	urlr = 'http://35.171.38.208/api/v1/db/read'

	response_ride = requests.post(url=urlr, json={"table": "Ride", "source": src, "destination":dst, "col":2 })

	if(src<1 or src>198):
		abort(400,description="source is invalid")

	elif(dst<1 or dst>198):
		abort(400,description="destination is invalid")

	elif(src==dst):
		abort(400,description="same source and destination is not allowed")

	elif response_ride.json():

		res1=response_ride.json()
		res=[]
		i=0
		d_val = res1.values()
		current_time = datetime.datetime.today()
		for item in d_val:
			if((current_time - datetime.datetime.strptime(item['timestamp'],'%d-%m-%Y:%S-%M-%H')).days < 0):
				item1 ={}
				item1['rideId'] = item['id']
				item1['username'] = item['created_by']
				item1['timestamp'] = item['timestamp']
				res.append(item1)
		
		if(res==[]):
			return Response(res1, status=204, mimetype='application/json')#no rides between that source and destination

		else:
			return jsonify(res), 200
	else:
		return jsonify(), 204

	

@app.route("/api/v1/rides/<int:ride_id>", methods=["GET"])#API5
def list_ride_details(ride_id):
	print("here")

	urlr = 'http://35.171.38.208/api/v1/db/read'
	existing_ride_id = requests.post(url=urlr, json={"table":"Ride","col":1,"ride_id":ride_id})

	if existing_ride_id.json():
		response_ride = requests.post(url=urlr, json={"table": "Ride", "ride_id": ride_id, "col":0 })
        	# If the response was successful, no Exception will be raised
		res = response_ride.json()
		print(res)
		users_string = res["users"]
		l=[]
		if ',' in users_string:
			l=users_string.split(',')
		else:
			l.append(users_string)
		res1={}
		res1["rideId"] = res["id"]
		res1["created_by"] = res["created_by"]
		res1["users"]=l
		res1["timestamp"]=res["timestamp"]
		res1["source"]=res["source"]
		res1["destination"]=res["destination"]


		return jsonify(res1)
			
	else:
		abort(400, description="there are no rides with this ride_id")


@app.route("/api/v1/rides/<int:ride_id>", methods=["POST"])#API6
def join_ride(ride_id):

	username = request.json['username']
	urlr = 'http://35.171.38.208/api/v1/db/read'
	
	#existing_username = requests.post(url=urlr, json={"table":"User","col":1,"username":username})

	headers = {'Origin': 'http://18.232.238.230:80'}#change
	allusers = requests.get(url = 'http://project-load-balancer-795597639.us-east-1.elb.amazonaws.com/api/v1/users', headers=headers)#public dns of load balancer should come here
	#print(allusers) #prints response<200>
	allusers1 = allusers.json()
	
	present=False
	for name in allusers1:
		if(name==username):
			present=True

	if present:
		existing_rides = requests.post(url=urlr, json={"table":"Ride","col":1,"ride_id":ride_id})
		
		url = 'http://35.171.38.208/api/v1/db/write'
		if existing_rides.json():
			delim=","
			existing_rides_data = requests.post(url=urlr, json={"table":"Ride","col":1,"ride_id":ride_id})

			for row in existing_rides_data.json():
				s = row['users']
				created_user = row['created_by'] 

			if(created_user == username):
				abort(400,"this user has created the ride, he cannot join the ride again")

			elif(username in s):
				abort(400,"this user has already joined this ride")

			else:
				if s=="":
					delim=""
				ans = s +delim+username
				d = {}
				d['value'] = ans
				d['table'] = "Ride"
				d['new']=0
				d['column']="users"
				d['ride_id']=ride_id
				response = requests.post(url = url,json = d)
				d1 = response.text
				#return Response({}, status = 200, mimetype='application/json')
				return jsonify(),200
		else:
			abort(400,"ride_id does not exist")
	else:
		abort(400, description="username doesn't exists")


#delete a ride given ride id	
@app.route("/api/v1/rides/<int:ride_id>", methods=["DELETE"])#API7
def delete_ride(ride_id):
	#existing_ride_id = Ride.query.filter(Ride.id == ride_id).all()

	urlr = 'http://35.171.38.208/api/v1/db/read'
	url = 'http://35.171.38.208/api/v1/db/write'
	existing_ride_id = requests.post(url=urlr, json={"table":"Ride","col":1,"ride_id":ride_id})
	
	if existing_ride_id.json():
		requests.delete(url = url,json={"table":"Ride","ride_id":ride_id,"col":1})
		#return Response({"status":"done"}, status = 200, mimetype='application/json')
		return jsonify(),200
	else:
		abort(400, description="ride_id does not exist")

@t.exclude
@app.route("/api/v1/db/clear", methods=["POST"])#ASSIGNMENT2 API 2
def clear_db():

	url = 'http://35.171.38.208/api/v1/db/write'
	requests.delete(url = url, json = {"table":"Ride","col":2})
	requests.delete(url = url, json = {"table":"User","col":2})
	return jsonify(),200
'''
@t.exclude
@app.route("/api/v1/allrides", methods=["GET"])
def get_ride():

    all_rides = Ride.query.all()
    result = rides_schema.dump(all_rides)
    return jsonify(result)
'''

#api to get count in list
@t.exclude
@app.route("/api/v1/_count", methods = ["GET"])
def get_requests():
	query = db.engine.execute('select status from flask_usage')
	#print(query)
	c=0
	for record in query:
		#print(record)#output is tuple : (number,)
		for i in record:
			if(i!=404):
				c=c+1
	l=[]
	l.append(c)
	return json.dumps(l),200

#api to reset counter variable
@t.exclude
@app.route("/api/v1/_count", methods = ["DELETE"])
def reset_requests_counter():
	query = db.engine.execute('delete from flask_usage')
	return jsonify(),200

#api to to get rides count
@app.route("/api/v1/rides/count", methods = ["GET"])
def count_rides():
	url = 'http://35.171.38.208/api/v1/db/write'
	result = requests.post(url=url, json={'table':'Counting'})

	urlr = 'http://35.171.38.208/api/v1/db/read'

	response_ride = requests.post(url=urlr, json={"table": "Ride", "col":3 })
	all_rides = response_ride.json()
	
	res=[]
	c=0
	#d_val = all_users.values()
	#print(all_rides)
	for item in all_rides:
		c=c+1
	res.append(c)
	return json.dumps(res),200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80, threaded=True)
