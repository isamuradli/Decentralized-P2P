import asyncio, pickle, threading, socket, random, time

class CentralIndexServer:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 5000
        self.activePeers = {}       # peerName : (IP, Port)
        self.topics = {}            # topic : peerName
        self.lock = asyncio.Lock()  #used to store logs in the file
        self.requests = 0


    async def write_to_file(self, message):
        async with self.lock:
            with open('server.log', 'a') as file:       #appending to the file
                file.write(message)

    async def handlePeerNode(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.requests +=1
        try:
            while True:
                data = await reader.read(200)

                if not data:
                    break

                dataObject = pickle.loads(data)
                requestName = dataObject['requestName']
                print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Request: {dataObject}")

                await self.write_to_file(f"\n[{time.ctime().split(' ')[3][:8]}] : [Request: {dataObject} at {time.ctime().split(' ')[3][:8]}]")

                if requestName == "registerPeer":
                    peerName = dataObject['peerName']
                    peerIP = '127.0.1.1'
                    peerPort = random.randint(5001, 9000) 
                    self.activePeers[peerName] = (peerIP,peerPort)
                    
                    response = {'response': f"Registered Peer: {peerName} at port {peerPort}",
                                'portAssigned' : peerPort}
                    
                if requestName == "unregisterPeer":
                    peerName = dataObject['peerName']
                    nodeToTransfer = dataObject['nodeToTransfer']
                    if nodeToTransfer == "None":
                        del self.activePeers[peerName]
                        self.topics = {k: v for k, v in self.topics.items() if v != peerName}
                        response = {'response': f"{peerName} has been deleted!",
                                    'transfer' : False}
                    elif nodeToTransfer not in self.activePeers:
                        del self.activePeers[peerName]
                        self.topics = {k: v for k, v in self.topics.items() if v != peerName}
                        response = {'response': f"{nodeToTransfer} not is available. {peerName} has been deleted!",
                                    'transfer' : False}
                    else:
                        response = {'response' : f'Node {peerName} has been transferred to {nodeToTransfer}. {peerName} has been deleted\n',
                                    'PeerName': self.activePeers[nodeToTransfer][0],
                                    'PeerPort': self.activePeers[nodeToTransfer][1],
                                    'transfer' : True}
                        del self.activePeers[peerName]
                        
                        for t, m in dataObject['messageBuffer'].items():
                            self.topics[t] = nodeToTransfer

                if requestName == "createTopic":
                    caller = dataObject['caller'] 
                    peerToWrite = dataObject['peerToWrite']
                    topic = dataObject['topic']

                    # if topic already exist
                    if topic in self.topics:
                        response = {'response': f"Topic {topic} already exists!"}
                    #if topic dont exist 
                    elif topic not in self.topics:
                        #if peer dont exist
                        if peerToWrite not in self.activePeers:
                            response = {'response':f"Peer {peerToWrite} does not exist"}
                        if peerToWrite == caller:
                            response = {'response': 'Approved'}
                            self.topics[topic] = caller
                        #if peer exists
                        elif peerToWrite in self.activePeers:
                            self.topics[topic] = peerToWrite
                            #if caller is writing to other node
                            if peerToWrite != caller:
                                response = {'response' : 'forward',
                                            'PeerName': self.activePeers[peerToWrite][0],
                                            'PeerPort': self.activePeers[peerToWrite][1]}
                            else:
                                response = {'response': f"Topic {topic} is added to {peerToWrite}"}

                                      
                if requestName == "deleteTopic":
                    caller = dataObject['caller']
                    topicName = dataObject['topic']

                    if topicName not in self.topics:
                        response = {'response':f"Topic {topicName} does not exist"}
                    else:
                        if caller == self.topics[topicName]:
                            response = {'response':f"Topic {topicName} is deleted from node {caller}!"}
                        else:
                            peerToDeleteFrom = self.topics[topicName]
                            response = {'PeerName': self.activePeers[peerToDeleteFrom][0],
                                        'PeerPort': self.activePeers[peerToDeleteFrom][1],
                                        'PeerCalled' : peerToDeleteFrom}
                        del self.topics[topicName]
                
                if requestName == "subscribe":
                    topicName = dataObject['topic']
                    #check if topic exist 
                    if topicName not in self.topics:
                        response = {'response' : f"Topic {topicName} doesn't exist to subscribe!"}
                    else:
                        topicHolder = self.topics[topicName]
                        response = {'PeerName': self.activePeers[topicHolder][0],
                                    'PeerPort': self.activePeers[topicHolder][1],
                                    'PeerCalled' : topicHolder} 

                if requestName == "send":
                    topicName = dataObject['topic']
                    caller = dataObject['caller']
                    if topicName not in self.topics:
                        response = {'response':f"Topic {topicName} does not exist"}
                    else:
                        topicHolder = self.topics[topicName]
                        if topicHolder != caller:
                            response = {'PeerName': self.activePeers[topicHolder][0],
                                    'PeerPort': self.activePeers[topicHolder][1],
                                    'PeerCalled' : topicHolder}    
                        else:
                            response = {'response': 'Message sent to topic'}
                    
                if requestName == "pull":
                    topicName = dataObject['topic']
                    if topicName not in self.topics:
                        response = {"response" : f"Topic {topicName} does not exist"}
                    else:
                        topicHolder = self.topics[topicName]
                        response = {'PeerName': self.activePeers[topicHolder][0],
                                    'PeerPort': self.activePeers[topicHolder][1],
                                    'PeerCalled' : topicHolder}  

                
                response = pickle.dumps(response)
                writer.write(response)

                await writer.drain()
                
                print(f"\n Topic: {self.topics}")
                print(f"\n Active Users: {self.activePeers}")

        except Exception as e :
            print(f'\{e}\n')
            # pass
        
        finally:    
            writer.close()
            await writer.wait_closed()

    async def runIndexServer(self):
        server = await asyncio.start_server(self.handlePeerNode, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'Server is running on {addr}')

        async with server:
            await server.serve_forever()

    def start_server(self):
        # Create a new thread for the asyncio event loop
        server_thread = threading.Thread(target=self.run_event_loop)
        server_thread.start()

    def run_event_loop(self):
        # Run the asyncio event loop
        asyncio.run(self.runIndexServer())

if __name__ == '__main__':
    server = CentralIndexServer()
    # asyncio.run(server.runIndexServer())
    server.start_server()
