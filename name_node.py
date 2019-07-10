#!/usr/bin/python
from flask import Flask, request
from flask_restful import Resource, Api
from collections import deque
import http.client
import os
import time
import datetime
import random
from apscheduler.scheduler import Scheduler # pip3 install apscheduler==2.1.2

app = Flask(__name__)
api = Api(app)

block_lists = dict() # dict of file block lists
ip_list = dict() # IP addresses of active data nodes
queue = deque() # queue to prep block_list to delete IPs from it on DN failure
BLOCK_SIZE = 64000000 # block size 64MB
start = 0
rep = 3

class RequestHandler(Resource):
    # reads
    def get(self):
        # gets file size & name from request headers
        file_name = request.headers['File-Name']
        # if requested file is in dictionary, returns its block list
        # sends 404 back otherwise
        if file_name in block_lists.keys():
            return block_lists[file_name]
        else:
            return None, 404

    # writes
    def put(self):
        req = request.get_json() # request as a json
        rsp = {} # response dictionary
            
        # gets file size & name from put request
        file_name = req['File-Name']
        file_size = int(req['File-Size'])

        if file_name in block_lists.keys():
            print('Preventing client from overwriting preexisting file.')
            return None, 409

        # DEBUG prints incoming file size
        print('Client requesting write of size: ' + str(file_size))
            
        ip_keys = list(ip_list) # list of ip_list keys (the ip addresses)
        # returns if there are no data nodes to write to
        if not ip_keys:
            print('No data nodes active.')
            return None, 500

        # overwrites existing file block list, or creates new one
        block_lists[file_name] = {}
        file_block_list = block_lists[file_name]

        global rep # replication factor
        written = 0 # 'bytes written' so far
        id_counter = 0 # block id number
        global start        

        while written < file_size:
            # simulates writing a block to a data node
            written += BLOCK_SIZE
            # limits 'written' variable to file size
            written = written if written < file_size else file_size
    
            # list of ips in response dictionary corresponding to a block id
            block_ips = ''
                
            # creates block list
            for j in range(0, rep):
                # reads just ip/port from file line
                ip = ip_keys[(start + j) % len(ip_keys)]
                block_ips += (ip + ' ')

            # adds block list entry to dictionary
            rsp[file_name + str(id_counter)] = block_ips
            # adds empty block list element to master block list
            file_block_list[file_name + str(id_counter)] = ''

            # increments index (or wraps it around)
            start = (start + 1) % len(ip_keys)
            id_counter += 1

        # returns a block list json to client
        return rsp

class BlockReports(Resource):
    # receives block reports (in progress)
    def post(self):
        ip = str(request.environ['REMOTE_ADDR']) # request ip
        time = str(datetime.datetime.now()) # timestamp
        node_block_list = request.get_json() # block list current node has

        # prints message if data node sending report is new, otherwise updates timestamp
        if ip not in ip_list:
            print('New data node with IP ' + str(ip) + ' added.')
            ip_list[ip] = time # adds ip to dict
            print('Current IP list: ' + str(ip_list))
        else:
            ip_list[ip] = time # updates ip timestamp if already in list

        # updates block list
        for block in node_block_list:
            for block_list in block_lists:
                for key in block_lists[block_list]:
                    ips = block_lists[block_list][key]
                    if str(key) == str(block) and ip not in ips:
                        block_lists[block_list][key] += (ip + ' ') # print('Current Block list: ' + str(block_lists))

def send_DN_Replicate(downed_ip):
    global rep
    send_queue = deque()
    remove_queue = deque()

    for filename in block_lists.keys():
        for key in block_lists[filename]:
            ips = block_lists[filename][key]
            if downed_ip in ips:
                if len(ip_list) >=1 and len(ip_list) < rep  :
                    temp_ip_holder= block_lists[filename][key].replace(downed_ip, '')
                    block_lists[filename][key] = temp_ip_holder
                elif len(ip_list) >= rep : # case only 1 ip left, dont want to send replicaton
                    # remove ip
                    temp_ip_holder = block_lists[filename][key].replace(downed_ip, '')
                    block_lists[filename][key] = temp_ip_holder
                    ip_split = block_lists[filename][key].split()
                    # choose sender & receiver of replicate block
                    send_node = str(ip_split[0])
                    rand_node = send_node
                    while(rand_node in temp_ip_holder):
                        rand_node = random.choice(list(ip_list))
                    rep_info_holder = [send_node, rand_node, key]

                    # append ip and block name and target DN(random)
                    send_queue.append(rep_info_holder)
                else:
                    block_lists[filename][key].replace(downed_ip, '') # remove ip
                    removal_info_holder = filename
                    remove_queue.append(removal_info_holder) # queue for removal

    if remove_queue:
        for q in range(len(remove_queue)):
            rem = remove_queue.popleft()
            if rem in block_lists:
                del block_lists[rem]

    if send_queue:
        for q in range(len(send_queue)):
            queue_arr = send_queue.popleft()
            rep_node = queue_arr[0]
            target_node = queue_arr[1]
            block_name = queue_arr[2]
            headers = {'Target-Ip':target_node,'Block-Name':block_name}
            try:
                conn = http.client.HTTPConnection(rep_node, 5000)
                conn.request('POST', '/BlockCopyHandler', None, headers)
                conn.close()
            except Exception as e:
                print('Error Sending Replication Message.')
                conn.close()

def update_node_list():
    print('Current IP list: ' + str(ip_list))
    print('Current block list: '+ str(block_lists))

    node_down = 10 # seconds without block report until node considered down
    to_del = [] # list of ips to delete

    for ip in ip_list:
        # last report time
        last_rep = datetime.datetime.strptime(ip_list[ip],'%Y-%m-%d %H:%M:%S.%f')
        
        # time since last report
        since_last_rep = (datetime.datetime.now() - last_rep).total_seconds()

        if since_last_rep > node_down:
            to_del.append(ip)

    for ip in to_del:
        ip_list.pop(ip, None)
        send_DN_Replicate(ip)
        print('Data node with IP ' + str(ip) + ' presumed failed.')
        print('New IP list: ' + str(ip_list))
        print('New block list: '+ str(block_lists))

api.add_resource(RequestHandler, '/') # client request handler
api.add_resource(BlockReports, '/BlockReports') # block report handler

# creates scheduler to update node list in the background
sched = Scheduler()
sched.start()
sched.add_interval_job(update_node_list, seconds = 5)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, use_reloader=False)
