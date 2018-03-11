#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFile, NodeID


from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from hashlib import sha256

import socket

class FileStoreHandler:

    def __init__(self, ip, port):
        self.myFiles = {}
        self.fingerTable = []
        self.nodeID = NodeID()
        self.nodeID.port = port
        self.nodeID.ip = ip
        self.id = int(sha256((str(ip) + ':' + str(port)).encode()).hexdigest(), 16)
        self.nodeID.id = sha256((str(ip) + ':' + str(port)).encode()).hexdigest()
        print("{}:{}".format(ip, port))

    def writeFile(self, rfile):
        # get the file key from the hash of "<owner>:<filename>"
        fileKey = sha256(rfile.meta.owner + ':' + rfile.meta.filename).hexdigest()
        # Check if I'm the owner of this file, if not raise an exception (SystemException)
        if self.nodeID != self.findSucc(fileKey):
            raise SystemException("The node, {} does not own this file".format(self.nodeID))
        # If file is found in my files, update its contents, increment its version
        if fileKey in self.myFiles:
            self.myFiles[fileKey].content = rfile.content
            self.myFiles[fileKey].meta.contentHash = rfile.meta.contentHash
            self.myFiles[fileKey].meta.version += 1
        # If the file was not found in myfiles, add it, set its version to 0
        else:
            rfile.meta.version = 0
            self.myFiles[fileKey] = rfile

    def readFile(self, filename, owner):
        fileKey = sha256(owner + ':' + filename).hexdigest()
        # Look for the file in my files, serve if found, raise exception if not found
        if fileKey in self.myFiles:
            return self.myFiles[fileKey]
        else:
            raise SystemException(
                "Cannot find file with name {}:{}. This node may not own this file".format(owner, filename))

    def setFingertable(self, nodeList):
        self.fingerTable = nodeList

    def findSucc(self, key):
        if self.id < int(key, 16) <= int(self.getNodeSucc().id, 16):
            return self.getNodeSucc()
        # 1. The function should call findPred to discover the DHT node that precedes the given id
        pred = self.findPred(key)
        # 2. the function should call getNodeSucc to find the successor of this predecessor node
        client, transport = returnClientAndTransportToMakeRPCTo(pred)
        transport.open()
        successorNode = client.getNodeSucc()
        transport.close()
        return successorNode

    def __closestPrecedingNodeInTable(self, key):
        for i in range(255, 0, -1):
            if int(self.fingerTable[i].id, 16) < int(key, 16):
                return self.fingerTable[i]
        return self.nodeID

    def findPred(self, key):
        # this function should returns the DHT node that immediately precedes this id(key)
        if int(self.nodeID.id, 16) < int(key, 16) <= int(self.getNodeSucc().id, 16):
            return self.nodeID
        else:
            searchingNode = self.__closestPrecedingNodeInTable(key)
            client, transport = returnClientAndTransportToMakeRPCTo(searchingNode)
            transport.open()
            searchingNodeSuccessor = client.getNodeSucc()
            transport.close()
            while not (int(searchingNode.id, 16) < int(key, 16) <= int(searchingNodeSuccessor.id, 16)):
                searchingNode = client.findPred(key)
                client, transport = returnClientAndTransportToMakeRPCTo(searchingNode)
                transport.open()
                searchingNodeSuccessor = client.getNodeSucc()
                transport.close()
            return searchingNode

    def getNodeSucc(self):
        # Returns the closest node to this node (this is stored at the first entry of fingertable)"
        # Raise a SystemException if the table has not been initialized
        if len(self.fingerTable) == 0:
            raise SystemException("The fingertable has not been properly initialized")
        else:
            return self.fingerTable[0]


def returnClientAndTransportToMakeRPCTo(node):
    transport = TSocket.TSocket(node.ip, node.port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)
    return client, transport


def main():
    if len(sys.argv) != 2:
        print("Wrong number of arguments... Please run as such: ./PythonServer.py <port-number>")
        exit(0)

    myIP = socket.gethostbyname(socket.gethostname())
    myPort = int(sys.argv[1])
    handler = FileStoreHandler(myIP, myPort)
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=myPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print("Starting the Server...")
    server.serve()
    print("Done serving")


if __name__ == '__main__':
    main()
