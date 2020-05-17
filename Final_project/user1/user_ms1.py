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
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'user1.sqlite')
db = SQLAlchemy(app)
ma = Marshmallow(app)


pstore = SQLStorage(db = db)
t=TrackUsage(app, [pstore])
'''

class User(db.Model):
	username = db.Column(db.String, unique=True, primary_key=True)
	password = db.Column(db.String, unique=True)

	def __init__(self, username, password):
		self.username = username
		self.password = password


class UserSchema(ma.Schema):
	class Meta:
		# Fields to expose
		fields = ('username', 'password')


user_schema = UserSchema()
users_schema = UserSchema(many=True)


@t.exclude
@app.route("/api/v1/db/read", methods=["POST"])#API9
def read_from_db():
	table =  request.get_json()['table']#to choose which table to read from
	if(table == 'User'):

		if(request.get_json()['col']==0):
			username = request.json['username']
			res = User.query.get(username)#to get all row values of a row with that username(primary key)

			response_dict={}
			response_dict["username"]=res.username
			response_dict["password"]=res.password

		elif(request.get_json()['col']==1):
			username = request.json['username']
			existing_username = User.query.filter(User.username == username).all()
			in_json = users_schema.dump(existing_username)
			return jsonify(in_json)

		elif(request.get_json()['col']==2):
			password = request.json['password']
			existing_password = User.query.filter(User.password == password).all()
			in_json = users_schema.dump(existing_password)
			return jsonify(in_json)

		elif(request.get_json()['col']==3):
			all_users = User.query.all()
			result = users_schema.dump(all_users)
			return jsonify(result)

	return jsonify(response_dict)

@t.exclude
@app.route("/api/v1/db/write", methods=["POST","DELETE"])#API8
def write_to_db():
	table =  request.get_json()['table']#to choose which table to to write to

	if(request.method == 'POST'):
		if(table == 'User'):
			new = request.json['new']
			if(new == 1):#inserting new row given details
				username = request.get_json()['username']
				password = request.get_json()['password']

				new_user = User(username, password)

				db.session.add(new_user)
				db.session.commit()

			else:#to update users or source or details given column name and updated value
				column = request.get_json()['column']
				value = request.get_json()['value']
				username = request.get_json()['username']
				user = User.query.get(username)
				if(column == 'password'):
					user.password = value
				db.session.commit()

	elif(request.method == 'DELETE'):
		if(table == 'User'):
			if(request.get_json()['col']==1):
				username = request.get_json()['username']
				user = User.query.get(username)
				db.session.delete(user)
				db.session.commit()
			if(request.get_json()['col']==2):
				User.query.delete()
				db.session.commit()
				return {"deletion":"done"}
	return {"status":"done"}

'''
#add a user given username and password
@app.route("/api/v1/users", methods=["PUT"])#API1
def add_user():

	username = request.json['username']
	password = request.json['password']
	password=password.lower()
	d={}
	d["table"]="User"
	d["new"]=1
	d["username"]=username
	d["password"]=password

	url = 'http://35.171.38.208/api/v1/db/write'
	urlr = 'http://35.171.38.208/api/v1/db/read'
	
	existing_username = requests.post(url=urlr, json={"table":"User","col":1,"username":username})
	existing_password = requests.post(url=urlr, json={"table":"User","col":2,"password":password})

	not_hexa=False
	for ch in password:
		if ch not in string.hexdigits:
			not_hexa=True

	if existing_username.json():
		abort(400, description="username already exists")

	elif existing_password.json():
		abort(400, description="password already exists")
		
	elif(len(password)!= 40 or not_hexa):
		abort(400, description="password is not SHA1 hash hex")
		
	else:
		response = requests.post(url=url, json=d)
		d1=response.text
		#return Response(d1, status=201, mimetype='application/json')
		return jsonify(),201


#delete a user given username
@app.route("/api/v1/users/<username>", methods=["DELETE"])#API2
def remove_user(username):

	urlr = 'http://35.171.38.208/api/v1/db/read'
	existing_username = requests.post(url=urlr, json={"table":"User","col":1,"username":username})
	url = 'http://35.171.38.208/api/v1/db/write' 
	if existing_username.json():
		requests.delete(url = url,json={"table":"User","username":username,"col":1})
		#return Response({"status":"done"}, status = 200, mimetype='application/json')
		return jsonify(),200
	else:
		abort(400, description="username does not exists")

	
@app.route("/api/v1/users", methods=["GET"])#ASSIGNMENT2 API 1
def list_all_users():
	url = 'http://35.171.38.208/api/v1/db/write'
	result = requests.post(url=url, json={'table':'Counting'})

	urlr = 'http://35.171.38.208/api/v1/db/read'

	response_ride = requests.post(url=urlr, json={"table": "User", "col":3 })
	all_users= response_ride.json()
	res=[]
	i=0
	#d_val = all_users.values()
	for item in all_users:
		res.append(item['username'])
	if res==[]:
		return json.dumps(res),204
	return json.dumps(res),200

@t.exclude
@app.route("/api/v1/db/clear", methods=["POST"])#ASSIGNMENT2 API 2
def clear_db():
	
	url = 'http://35.171.38.208/api/v1/db/write'
	requests.delete(url = url, json = {"table":"User","col":2})
	requests.delete(url = url, json = {"table":"Ride","col":2})
	return jsonify(),200
'''
@t.exclude
@app.route("/api/v1/allusers", methods=["GET"])
def get_user():
    all_users = User.query.all()
    result = users_schema.dump(all_users)
    return jsonify(result)
'''
#api get count in list
@t.exclude
@app.route("/api/v1/_count", methods = ["GET"])
def get_requests():
	query = db.engine.execute('select status from flask_usage')#output is tuple : (number,)
	#print(query)
	c=0
	for record in query:
		print(record)
		for i in record:
			if(i!=404):
				c=c+1
	l=[]
	l.append(c)
	return json.dumps(l),200

#count reset api
@t.exclude
@app.route("/api/v1/_count", methods = ["DELETE"])
def reset_requests_counter():
	query = db.engine.execute('delete from flask_usage')
	return jsonify(),200


if __name__ == '__main__':
    app.run(debug=True,  host='0.0.0.0', port=80, threaded=True)
