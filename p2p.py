import threading
import json
import pickle   #any object to bytes; uesd for sending raw data on socket channels
import random
import time
import socket
from contextlib import closing
import io
import os
import shutil   #remove directory tree
import logging

bootstrap_node_numbers = 6
N = 3
programRunTime = 5*60
nodes_list = []
sleepDuration = 20
sleepInterval = 10
reservedPorts = []
pingInterval = 2
lastHeardLimit = 8

def shootThreads():
    global nodes_list

    for node in nodes_list:
        node.maximizeNeighbors_.start()
        node.pingNeighbors_.start()
        node.listen_.start()
        node.timer_.start()

def ifAlreadyReserved(port):
    for reservedPort in reservedPorts:
        if reservedPort == port:
            return True

    return False

def assignAFreePort():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        if  not ifAlreadyReserved(port):
            reservedPorts.append(port)
            return port
        assignAFreePort()

def delLastRunDatas():
    output_dir = os.path.join(os.getcwd(), 'output')
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    log_path = os.path.join(os.getcwd(), 'myapp.log')
    if os.path.exists(log_path):
        os.remove(log_path)

def printNodesTraces():
    global nodes_list

    for node in nodes_list:
        node.printTracesinJSON()

def shutDownAllThread():
    for node in nodes_list:
        node.isAlive = False

def globalTimer():
    global programRunTime
    global sleepDuration
    global sleepInterval

    start_time = time.time()
    while True:
        elapsed_time = int(time.time() - start_time)
        if elapsed_time >= programRunTime:
            shutDownAllThread()
            printNodesTraces()
            os._exit(1)
        if elapsed_time % sleepInterval == 0:
            aRandomNode = pickARandomNode()
            aRandomNode.isAlive = False
            logger.debug(f"{aRandomNode.id} shutdown")
        time.sleep(1)

def findNodebyID(id):
    global nodes_list

    for node in nodes_list:
        if node.id == id:
            return node

    return False

def send(rawData, UDP_IP, UDP_PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(rawData, (UDP_IP, UDP_PORT))

def obj_dict(obj):
    return obj.__dict__

def pickleSerialize(packet):
    rawData = pickle.dumps(packet)
    return rawData

def pickleDeserialize(rawData):
    packet = pickle.loads(rawData)
    return packet

def pickARandomNode():
    global nodes_list

    rand = random.randrange(0, len(nodes_list), 1)
    return nodes_list[rand]

def shouldgetDropped():
    rand = random.randrange(1, 101, 1)
    if rand <= 5:
        return True

    return False

class Neighbor:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        self.last_heard_of = 0
        self.sent_to_count = 0
        self.recieved_from_count = 0
        self.availability_time = 0 

class Packet:
    def __init__(self, sender_id, sender_ip, sender_port, type, sender_neighbors):
        self.sender_id = sender_id
        self.sender_ip = sender_ip
        self.sender_port = sender_port
        self.type = type
        self.sender_neighbors = sender_neighbors	

class Node:
    def __init__(self):
        self.ip = 'localhost'
        self.port = assignAFreePort()

        self.unidirectionalNeighbors = []
        self.bidirectionalNeighbors = []
        self.bidirectionalNeighborsHistory = []
        self.temporaryNeighbor = None

        self.isAlive = True

        self.maximizeNeighbors_ = threading.Thread(target = self.maximizeNeighbors, args = [])
        self.pingNeighbors_ = threading.Thread(target = self.pingNeighbors, args = [])
        self.listen_ = threading.Thread(target = self.listen, args = [])
        self.timer_ = threading.Thread(target = self.timer, args = [])

    @property
    def id(self):
        return self.ip + str(self.port)

    @property
    def recentlyheardNeighbors(self):
        return self.bidirectionalNeighbors + self.unidirectionalNeighbors

    def maximizeNeighbors(self):
        global N
        global sleepDuration

        while True:		
            if not self.isAlive:
                time.sleep(sleepDuration)
                self.isAlive = True

            aRandomNode = None
            if len(self.bidirectionalNeighbors) < N:
                while aRandomNode is None or not self.isValidChoice(aRandomNode):
                    aRandomNode = pickARandomNode()
                self.temporaryNeighbor = Neighbor(aRandomNode.id, aRandomNode.ip, aRandomNode.port)
                self.ping(aRandomNode)
                logger.debug(f"{self.id} added {aRandomNode.id} to its temporaryNeighbor")
                logger.debug(f"{self.id} pinged {aRandomNode.id}")

    def isValidChoice(self, node):
        if  (node.id in [recentlyheardNeighbor.id for recentlyheardNeighbor in self.recentlyheardNeighbors]) or node.id == self.id:
            return False

        return True

    def timer(self):
        global sleepDuration

        start_time = time.time()
        while True:
            if not self.isAlive:
                time.sleep(sleepDuration)
                self.isAlive = True
                logger.debug(f"{self.id} is back online")
            elapsed_time = int(time.time() - start_time)
            if elapsed_time >= 1:
                for bidirectionalNeighbor in self.bidirectionalNeighbors:
                    bidirectionalNeighbor.availability_time+=1
                for recentlyheardNeighbor in self.recentlyheardNeighbors:
                    recentlyheardNeighbor.last_heard_of+=1
                if self.temporaryNeighbor:
                    self.temporaryNeighbor.last_heard_of+=1      
                start_time = time.time()

    def listen(self):
        global sleepDuration

        serverSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        serverSock.bind((self.ip, self.port))

        while True:
            if not self.isAlive:
                time.sleep(sleepDuration)
                self.isAlive = True
            rawData, addr = serverSock.recvfrom(1024)
            if not shouldgetDropped():
                self.processRecievedPacket(rawData) 

    def lookupMyselfOnNeighbor(self, sender_neighbors):
        if self.id in sender_neighbors:
            return True
        
        return False

    def addToUnidirectionalNeighbors(self, sender_id, sender_ip, sender_port):
        for unidirectionalNeighbor in self.unidirectionalNeighbors:
            if unidirectionalNeighbor.id == sender_id:
                self.keepTrackof(unidirectionalNeighbor, 'recieve')
                unidirectionalNeighbor.last_heard_of = 0
                return

        neighbor = Neighbor(sender_id, sender_ip, sender_port)
        self.unidirectionalNeighbors.append(neighbor)
        logger.debug(f"{self.id} added {neighbor.id} to its unidirectionalNeighbors")

    def addToBidirectionalNeighbors(self, sender_id, sender_ip, sender_port):
        for bidirectionalNeighbor in self.bidirectionalNeighbors:
            if bidirectionalNeighbor.id == sender_id:
                self.keepTrackof(bidirectionalNeighbor, 'recieve')
                bidirectionalNeighbor.last_heard_of = 0
                return

        if  len(self.bidirectionalNeighbors) == 3:
            return
        neighbor = Neighbor(sender_id, sender_ip, sender_port)
        self.bidirectionalNeighbors.append(neighbor)
        logger.debug(f"{self.id} added {neighbor.id} to its bidirectionalNeighbors")
        self.bidirectionalNeighborsHistory.append(neighbor)

    def processRecievedPacket(self, rawData):   
        packet = pickleDeserialize(rawData)
        result = self.lookupMyselfOnNeighbor(packet.sender_neighbors)
        if result:
            self.addToBidirectionalNeighbors(packet.sender_id, packet.sender_ip, packet.sender_port)
        else:
            self.addToUnidirectionalNeighbors(packet.sender_id, packet.sender_ip, packet.sender_port)

    def keepTrackof(self, neighbor, sentOrrecieve):
        if sentOrrecieve == 'sent':
            neighbor.sent_to_count+=1
        elif sentOrrecieve == 'recieve':
            neighbor.recieved_from_count+=1

    def ping(self, node):
        packet = Packet(self.id, self.ip, self.port, 'hello', [recentlyheardNeighbor.id for recentlyheardNeighbor in self.recentlyheardNeighbors])
        rawData = pickleSerialize(packet)
        send(rawData, node.ip, node.port) 

    def availability_percentage(self):
        global programRunTime

        for bidirectionalNeighbor in self.bidirectionalNeighborsHistory:
            bidirectionalNeighbor.availability_time = str(bidirectionalNeighbor.availability_time/programRunTime * 100) + '%' 

    def printTracesinJSON(self):
        self.delRenundantNeighbors()
        self.availability_percentage()

        main_path = os.path.join(os.getcwd(), 'output')
        if not os.path.exists(main_path):
            os.mkdir(main_path)

        file_name = f"node_{self.id}_bidirectionalNeighbors.json"
        output_path = os.path.join(main_path, file_name)
        with open(output_path, 'w') as output:
            jsonString = json.dumps(self.bidirectionalNeighbors, default = obj_dict)
            output.write(jsonString)

        file_name = f"node_{self.id}_bidirectionalNeighborsHistory.json"
        output_path = os.path.join(main_path, file_name)
        with open(output_path, 'w') as output:
            jsonString = json.dumps(self.bidirectionalNeighborsHistory, default = obj_dict)
            output.write(jsonString)

        file_name = f"node_{self.id}_topology.json"
        output_path = os.path.join(main_path, file_name)
        with open(output_path, 'w') as output:
            jsonString = json.dumps({'unidirectionalNeighbors': self.unidirectionalNeighbors}, default = obj_dict)
            output.write(jsonString)
            jsonString = json.dumps({'bidirectionalNeighbors': self.bidirectionalNeighbors}, default = obj_dict)
            output.write(jsonString)

    def delRenundantNeighbors(self):
        for bidirectionalNeighborOutter in self.bidirectionalNeighborsHistory:
            for bidirectionalNeighborInner in self.bidirectionalNeighborsHistory:
                if bidirectionalNeighborInner is bidirectionalNeighborOutter:
                    continue
                if bidirectionalNeighborInner.id == bidirectionalNeighborOutter.id:
                    bidirectionalNeighborOutter.availability_time+=bidirectionalNeighborInner.availability_time
                    self.bidirectionalNeighborsHistory.remove(bidirectionalNeighborInner)

    def delNeighbor(self, neighbor):
        for unidirectionalNeighbor in self.unidirectionalNeighbors:
            if neighbor is unidirectionalNeighbor:
                self.unidirectionalNeighbors.remove(unidirectionalNeighbor)
                return
        
        for bidirectionalNeighbor in self.bidirectionalNeighbors:
            if neighbor is bidirectionalNeighbor:
                self.bidirectionalNeighbors.remove(bidirectionalNeighbor)

    def pingNeighbors(self):
        global sleepDuration
        global lastHeardLimit
        global pingInterval

        while True:
            if not self.isAlive:
                time.sleep(sleepDuration)
                self.isAlive = True
            for recentlyheardNeighbor in self.recentlyheardNeighbors:
                if recentlyheardNeighbor.last_heard_of >= lastHeardLimit:
                    self.delNeighbor(recentlyheardNeighbor)
                    logger.debug(f"{self.id} deleted {recentlyheardNeighbor.id} from its recentlyheardNeighbors")
                else:
                    self.ping(findNodebyID(recentlyheardNeighbor.id))
                    logger.debug(f"{self.id} pinged {findNodebyID(recentlyheardNeighbor.id).id}")
                    self.keepTrackof(recentlyheardNeighbor, 'sent')
            if self.temporaryNeighbor:
                if self.temporaryNeighbor.last_heard_of >= lastHeardLimit:
                    self.temporaryNeighbor = None
                    logger.debug(f"{self.id} deleted {findNodebyID(recentlyheardNeighbor.id).id} from its temporaryNeighbor")
            time.sleep(pingInterval)

#Main
delLastRunDatas()
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('myapp.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

for i in  range(bootstrap_node_numbers):
	nodes_list.append(Node())
shootThreads()
globalTimer()

