from flask import Flask, make_response, request
from flask_restful import Resource, Api
from os import environ
import os
import http.client
import json
from apscheduler.scheduler import Scheduler # pip3 install apscheduler==2.1.2

app = Flask(__name__)
api = Api(app)

host_ip = os.environ.get('IP_HOST') # nn ip
block_list = [] # block list

# RequestHandler takes in read and write requests from client
class RequestHandler(Resource):
	# reads
	def get(self):
		block_name = request.headers['Block-Name'] # block name

		# 404s if file does not exist
		if not os.path.exists(block_name):
			return None, 404

		# reads block & returns it to client
		with open(block_name, 'rb') as file:
			block = file.read()

		# creates response of the block and returns it to the client
		rsp = make_response(block, 200)
		return rsp

	# writes into local memory
	def put(self):
		block_name = request.headers['Block-Name'] # block name
		block = request.data # block content

		# writes block to a file
		with open(block_name, 'wb') as file:
			file.write(block)
		block_list.append(block_name)

		# sends block report
		send_block_report()

class BlockCopyHandler(Resource):
	# will handle NN request to replicate data
	def post(self):
		target_node = request.headers['Target-Ip'] # block name
		block_name = request.headers['Block-Name'] # block ID

		# request headers
		headers = {'Block-Name':block_name,'Content-Type':'application/json'}

		if os.path.exists(block_name):
			# writes block to a file
			with open(block_name, 'rb') as file:
				send_block = file.read() #get block info
				try:
					# opens connections
					conn = http.client.HTTPConnection(target_node, 5000)
					# sends request
					conn.request('PUT', '/', send_block, headers)
					conn.close()
				except Exception as e:
					print('Error Sending Replication Message.')
					conn.close()

def send_block_report():
	# opens connection with name node
	conn = http.client.HTTPConnection(host_ip, '5000')

	headers = {'Content-Type':'application/json'} # specifies json body content
	block_list_json = json.dumps(block_list) # creates json out of block list

	try:
		conn.request('POST', '/BlockReports', block_list_json, headers)
	except Exception as e:
		print('Error sending report to name node.')

	conn.close()
	print('Block list: ' + str(block_list))

api.add_resource(RequestHandler, '/') # request handler resource
api.add_resource(BlockCopyHandler, '/BlockCopyHandler') # request handler resource

send_block_report() # sends initial block report

sched = Scheduler() # creates background scheduler for block report
sched.start()

def heartbeat():
	print('Sending block report.')
	send_block_report()

# adds 5 second interval for block report
sched.add_interval_job(heartbeat, seconds=5)

if __name__ == '__main__':
	app.run(host='0.0.0.0',debug=True, port=5000, use_reloader=False)
