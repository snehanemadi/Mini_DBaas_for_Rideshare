# Mini_DBaas_for_Rideshare
This repo contains the the code for all the three assignments and final project for the course Cloud Computing - UE17CS352

Contributors:
Sneha Nemadi,
Swathi M,
Prakruti Rao,
Pushpavathi K N

Following are the instructions to run each of the assignments and project

->Project:

	terminal1 :	cd user1
			sudo docker-compose build
			sudo docker-compose up


	terminal2 :	cd ride1
			sudo docker-compose build
			sudo docker-compose up

	terminal3 :	cd worker
			sudo docker-compose build
			cd ..
			cd orchestrator
			sudo docker-compsoe build
			sudo docker-compose up


-> Assignment1 : 

    -> For basic deployment on inbuilt flask server: 

	pip3 install requirements.txt
	sudo python3 assignment1.py
  
    -> For deployment on gunicorn3 with nginx as reverse proxy:
	
	A. pip3 install nginx

	B. pip3 install gunicorn3

	C. sudo nginx -t

	D. cd /etc/nginx/sites-enabled/

	E. sudo rm default

	F. ls       //default file should be removed

	G. cd /etc/nginx/conf.d/

	H. sudo nano virtual.conf

	//add the below code to that file

	server
	{
		listen 80;
		server_name 52.55.148.36;
		location /
		{
			proxy_pass http://172.31.85.156:8080;
		}
	}

	I. sudo ln -s /etc/nginx/conf.d/virtual.conf /etc/nginx//sites-enabled/virtual.conf

	M. sudo nginx -t      //to apply changes

	N. sudo service nginx restart

	O. sudo service nginx status      //to check if nginx is active

	->in assignment1.py:
	change all ip addresses to internal ip address, port 8080 i.e 172.31.85.156:8080

	P. sudo gunicorn3 -w 10 cc_assignment17:app -b 172.31.85.156:8080 //to start the app server with 10 threads


->Assignment 2:

	terminal1 :	cd user
			sudo docker-compose build
			sudo docker-compose up


	terminal2 :	cd ride
			sudo docker-compose build
			sudo docker-compose up

->Assignment 3:

	terminal1 :	cd user1
			sudo docker-compose build
			sudo docker-compose up


	terminal2 :	cd ride1
			sudo docker-compose build
			sudo docker-compose up





	    
