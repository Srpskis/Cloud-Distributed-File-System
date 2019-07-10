import os
import sys
import http.client
import json
import boto3
import botocore

BLOCK_SIZE = 64000000 # block size 64MB

# reads input from user
def read_input():
	cmd = input('> ')
	cmd_list = cmd.split()
	return cmd_list

# gets s3 bucket the key is in
def get_bucket():
	bucket = input('From which S3 bucket? > ')
	return bucket

# prints help menu
def print_help():
	print('Available commands:')
	print('write S3_key  - writes file from S3 bucket to system.')
	print('read filename - reads file into local file (only file name needed).')
	print('list filename - lists data nodes storing each block of file.')

# reads blocks of file from data nodes --> concatenates them into a single file
def read(s3_key, ip, port):
	# opens connection with ip & port
	conn = http.client.HTTPConnection(ip, port)
	filename = s3_key.rsplit('/', 1)[-1]

	# sends get request with file name as header
	try:
		conn.request('GET', '/', None, {'File-Name':filename})
	except Exception as e:
		print('Error requesting from name node.')
		conn.close()
		exit(1)

	# gets response
	response = conn.getresponse()

	# sends off requests to data notes
	if response.status == 200:
		rsp = json.load(response) # response as dictionary
		content = bytearray() # block contents

		# TODO - only sends get request to first ip in list for now
		for key in rsp:
			ips = rsp[key].split() # list of ip to request for a given block
			read = False

			for ip in ips:
				if read:
					break
				else:
					# opens connection to data node
					data_node_conn = http.client.HTTPConnection(ip, 5000)

					headers = {'Block-Name':key} # request header

					# sends request
					try:
						data_node_conn.request('GET','/', None, headers)
					except Exception as e:
						print('Error requesting from data node.')
						conn.close()
						exit(1)

					if response.status == 200:
						# gets response
						data_response = data_node_conn.getresponse()
						content += data_response.read()
						data_node_conn.close()
						read = True

			if not read:
				print('Error reading file blocks.')
				exit(1)

		# writes results of request to file
		with open('read_' + filename, 'wb') as file:
			file.write(content)
	else:
		# if file requested isnt found, prints this to user
		print('File not found.')

	conn.close()

# sends block of file to data nodes
def send_to_data_nodes(rsp, s3_key, bucket, size):
	# gets file from s3 & reads it into a variable
	try:
		s3 = boto3.resource('s3')
		obj = s3.Object(bucket, s3_key)
		file = obj.get()['Body'].read()
	except botocore.exceptions.ClientError as e:
		print('Error retrieving file from S3.')
		exit(1)

	filename = s3_key.rsplit('/', 1)[-1] # gets filename from s3 key

	# splits file
	for i in range(0, len(rsp)):
		# list of ips to send the current block to
		ip_list = rsp[filename + str(i)].split()
		# block name
		blockname = filename + str(i)

		# start & end points for current block
		start = BLOCK_SIZE * i
		end = min((BLOCK_SIZE * i) + BLOCK_SIZE, len(file))

		# creates block out of substring of file contents
		block = file[start:end]

		# sends block to ips
		for ip in ip_list:
			headers = {'Block-Name':blockname}
			conn = http.client.HTTPConnection(ip, 5000)

			# sends request
			try:
				conn.request('PUT', '/', block, headers)
			except Exception as e:
				print('Error writing block to data node.')
				conn.close()
				exit(1)

			conn.close()

# writes file to system
def write(s3_key, bucket, ip, port):
	try:
		# gets file size
		s3 = boto3.client('s3')
		response = s3.head_object(Bucket=bucket, Key=s3_key)
		file_size = response['ContentLength']
	except botocore.exceptions.ClientError as e:
		print('Error finding file in S3')
		exit(1)

	filename = s3_key.rsplit('/', 1)[-1] # gets file name from s3 key

	#turns dictionary of file name & file size into json
	write_data = {'File-Name':filename, 'File-Size':file_size}
	write_data_json = json.dumps(write_data)

	# request header specifies json format
	headers = {'Content-Type':'application/json'}

	# opens connection with ip & port
	conn = http.client.HTTPConnection(ip, port)

	# sends put request with write data json
	try:
		conn.request('PUT', '/', write_data_json, headers)
	except Exception as e:
		print('Error retrieving write metadata from name node.')
		conn.close()
		exit(1)

	# gets response
	response = conn.getresponse()

	if response.status == 200:
		rsp = json.load(response) # response as dictionary
		send_to_data_nodes(rsp, s3_key, bucket, file_size)
	elif response.status == 409:
		print('File already exists.')
	else:
		print('Error retrieving data needed to write.')

	conn.close()

# lists data node IPs that store each block
def list(s3_key, ip, port):
	# opens connection with ip & port
	conn = http.client.HTTPConnection(ip, port)
	filename = s3_key.rsplit('/', 1)[-1]

	# sends get request with file name as header
	try:
		conn.request('GET', '/', None, {'File-Name':filename})
	except Exception as e:
		print('Error requesting from name node.')
		conn.close()
		exit(1)

	# gets response
	response = conn.getresponse()

	# sends off requests to data notes
	if response.status == 200:
		rsp = json.load(response) # response as dictionary
		print (filename + ":")
		for key in rsp:
			list = rsp[key].split()
			print(str(key) + ': ' + str(list))
	else:
		print('Error getting file list.')

def main(args):
	if len(args) != 3:
		print('Usage: python3 client.py server port')
	else:
		print('Please enter a command')

		server_ip = args[1]
		server_port = args[2]
		input = read_input()

		if len(input) != 2:
			print_help()
		else:
			if input[0].lower() == 'read':
				read(input[1], server_ip, server_port)
			elif input[0].lower() == 'write':
				bucket = get_bucket()
				write(input[1], bucket, server_ip, server_port)
			elif input[0].lower() == 'list':
				list(input[1], server_ip, server_port)
			else:
				print_help()

if __name__ == '__main__':
	main(sys.argv)
