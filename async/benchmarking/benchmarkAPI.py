import asyncio, pickle,  threading, time, sys
import socket
import random, string
class PeerNode:
    def __init__(self, name, apiName):
        self.peerName = name
        self.messageBuffer = {}
        self.subscriptions = {}
        self.readHistory = {}
        self.grbCollectIndex = 0
        self.index_server_host = 'localhost'
        self.index_server_port = 5005
        self.lock = asyncio.Lock()
        self.apiName = apiName
        self.averageResponse = 0

    def garbageCollector(self, topic):
        #check if everyone read the messages
        grbCollectIndex = len(self.messageBuffer[topic])

        for key in self.readHistory.keys():
            #pick up the same topic
            if key[0] == topic and grbCollectIndex != 0:
                grbCollectIndex = min(grbCollectIndex, self.readHistory[key])
                                
        if grbCollectIndex == len(self.messageBuffer[topic]) and len(self.messageBuffer[topic]) !=0:
            self.messageBuffer[topic] = []    #garbage collect 
            #set all indexes to 0 after garbage collected
            for key in self.readHistory.keys():
                if key[0] == topic:
                    self.readHistory[key] = 0
            print(f"Garbage collected!")

    async def write_to_file(self, message):
        async with self.lock:
            with open(f'peer_{self.peerName}.log', 'a') as file:
                file.write(f"\n[{time.ctime().split(' ')[3][:8]}] : {message}")

    async def registerPeerNode(self):
        addToFile = f"[Registering Peer Node: {self.peerName}]"

        print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")


        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        msg = {'requestName': 'registerPeer', 'peerName' : self.peerName}
        message = pickle.dumps(msg)
        writer.write(message)

        await writer.drain()
    
        data = await reader.read(200)
        response = pickle.loads(data)
        addToFile = f"[Response from Index Server: {response['response']}]"

        print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")


        self.peerPort = int(response['portAssigned'])
        writer.close()
        await writer.wait_closed()
        return response
    
    async def queryTopics(self, topic, timeStarted):

        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        msg = {'requestName': 'queryTopic', 'topic' : topic}
        message = pickle.dumps(msg)
        writer.write(message)

        await writer.drain()
    
        data = await reader.read(200)
        response = pickle.loads(data)
        self.averageResponse = time.time() - timeStarted

        
        addToFile = f"[Response from Index Server: {response['response']}]"

        print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")


        writer.close()
        await writer.wait_closed()

    async def unregisterPeerNode(self, nodeToTransfer):
        
        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        request = {'requestName': 'unregisterPeer', 
           'nodeToTransfer' : nodeToTransfer, 
           'peerName' : self.peerName, 
           'messageBuffer' : self.messageBuffer,
           'subscriptions' : self.subscriptions,
            'readHistory' : self.readHistory
           }
        
        addToFile = f"[Node {self.peerName} called request: {request['requestName']}]"
        print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")

        message = pickle.dumps(request)
        writer.write(message)

        await writer.drain()
    
        data = await reader.read(200)
        response = pickle.loads(data)

        addToFile = f"Response from server : {response['response']}"
        print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile} \n")


        if not response['transfer']:
            addToFile = f"[Shutting the node {request['peerName']}]"
            print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
    
        else:
            peerToWriteIP = response['PeerName']
            peerToWritePort = response['PeerPort']
            addToFile = f"[All topics transferred to {request['nodeToTransfer']}]"
            print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
    

            await self.send_messageToPeer(peerToWriteIP, peerToWritePort, request)
            addToFile = f"[Shutting the node {request['peerName']} after transferring topics to {request['nodeToTransfer']}]"
            print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
    

        writer.close()
        await writer.wait_closed()
        return
    
    async def createTopic(self, peerToWrite, topic):
        print("\nCreating topic...")
        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        request = {
                'requestName': 'createTopic', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite, 
                'topic': topic
                }
        
        addToFile = f"[Node {self.peerName} called request : {request}]"
        print(f"\n[{time.ctime().split(' ')[3][:8]}] : f{addToFile}")


        message = pickle.dumps(request)
        writer.write(message)

        await writer.drain()
        
        data = await reader.read(200)
        response = pickle.loads(data)

        if response['response'] == 'Approved':
            self.messageBuffer[topic] = []
            self.subscriptions[topic] = []

            addToFile = f"[Response from server: {response['response']}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile} \n")
    

        elif response['response'] == 'forward':
            peerToWriteIP = response['PeerName']
            peerToWritePort = response['PeerPort']

            addToFile = f"[Response from server: {response['response']} to {peerToWrite}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile} \n")
    

            await self.send_messageToPeer(peerToWriteIP, peerToWritePort, request)
            addToFile=f"[Topic {topic} is created at Peer: {peerToWrite}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    
        else:
            addToFile = f"[Response : {response}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    

        writer.close()
        await writer.wait_closed()

    async def deleteTopic(self, topic):
        print("\nDeleting topic...")
        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        if topic in self.messageBuffer:
            del self.messageBuffer[topic] 
            del self.subscriptions[topic]
        request = {
                'requestName': 'deleteTopic', 
                'caller': self.peerName, 
                'topic': topic
                }
        addToFile = f"[Node {self.peerName} called request : {request}]"
        print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")


        message = pickle.dumps(request)
        writer.write(message)

        await writer.drain()
        
        data = await reader.read(200)
        response = pickle.loads(data)

        if 'response' in response:
            addToFile = f"[Response from server: {response['response']}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    
        else:
            peerToDeleteFromIP = response['PeerName']
            peerToDeleteFromPort = response['PeerPort']

            addToFile = f"[Request forwarded to {response['PeerCalled']}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    

            await self.send_messageToPeer(peerToDeleteFromIP, peerToDeleteFromPort, request)

        writer.close()
        await writer.wait_closed()

    async def send(self, topic, message):
        print(f"\nSending message {message} to {topic}\n")
        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        if topic in self.messageBuffer:
            self.messageBuffer[topic].append(message)
        request = {
            'requestName': 'send',
            'caller' : self.peerName,
            'topic' : topic,
            'message' : message
        }
        messageContent = message

        addToFile = f"[Node {self.peerName} called request : {request}]"
        print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")


        msg = pickle.dumps(request)
        writer.write(msg)

        await writer.drain()

        data = await reader.read(200)
        response = pickle.loads(data)

        if 'response' in response:
            addToFile= f"[Response from server: {response['response']}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    
        else:
            peerToSendTopicMessageIP = response['PeerName']
            peerToSendTopicMessagePort = response['PeerPort']

            addToFile = f"[Response from server : Forwarded to Node {response['PeerCalled']}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    

            await self.send_messageToPeer(peerToSendTopicMessageIP, peerToSendTopicMessagePort, request)
            
            addToFile = f"[Message {messageContent} is send to Topic:{topic} at {[peerToSendTopicMessageIP]}]"
            print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
    

        writer.close()
        await writer.wait_closed()

    async def subscribe(self, topic):
        print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Subscribing to topic {topic}]")
        reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
        if topic in self.messageBuffer:
            print(f"[{time.ctime().split(' ')[3][:8]}] : You can't subscribe to your own topic")
        else:
            request = {
                'requestName': 'subscribe',
                'caller' : self.peerName,
                'topic' : topic
            }

            addToFile = f"[Node {self.peerName} called request : {request}]"
            print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
    

            msg = pickle.dumps(request)
            writer.write(msg)

            await writer.drain()

            data = await reader.read(200)
            response = pickle.loads(data)

            if 'response' in response:
                addToFile = f"[Response from server: {response['response']}]"
                print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
        
            else:
                peerToSubscribeIP = response['PeerName']
                peerToSubscribePort = response['PeerPort']

                addToFile = f"[Response from server : Forwarded to Topic Holder {response['PeerCalled']}]"
                print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
        
                
                await self.send_messageToPeer(peerToSubscribeIP, peerToSubscribePort, request)

            print(f"\nSubscribed function finished!\n")

        writer.close()
        await writer.wait_closed()

    async def pull(self, topic):
        print(f"\nPulling messages from {topic}")
        if topic in self.messageBuffer:
            print("\nYou can't pull from your own messageBuffer!")
        else:
            reader, writer = await asyncio.open_connection(self.index_server_host, self.index_server_port)
            request = {
                    'requestName': 'pull', 
                    'caller': self.peerName, 
                    'topic': topic
                    }

            addToFile = f"[Node {self.peerName} called request : {request}]"
            print(f"[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
    

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()

            data = await reader.read(200)
            response = pickle.loads(data)

            if 'response' in response:
                addToFile = f"[Response from server: {response['response']}]"
                print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
        
            else:
                peerToPullFromIP = response['PeerName']
                peerToPullFromPort = response['PeerPort']
                addToFile = f"[Response from server : Forwarded to {response['PeerCalled']}]"
                print(f"\n\t[{time.ctime().split(' ')[3][:8]}] : {addToFile}\n")
        
                
                await self.send_messageToPeer(peerToPullFromIP, peerToPullFromPort, request)
                
            writer.close()
            await writer.wait_closed()

    async def send_messageToPeer(self, peerIP, peerPort, message):
        reader, writer = await asyncio.open_connection(peerIP, peerPort)
        print("\nConnection established with other peer \n")
        
        request = pickle.dumps(message)
        writer.write(request)
        await writer.drain()

        data = await reader.read(200)
        response = pickle.loads(data)
        print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Node {self.peerName} received reply: {response}]")
        writer.close()
        await writer.wait_closed()

    async def listen_for_messages(self):
        server = await asyncio.start_server(self.handle_incoming_messages, socket.gethostname(), self.peerPort)  # Bind to port
        localIP = server.sockets[0].getsockname()[0]
        port = server.sockets[0].getsockname()[1]
        print(f"[*] Listening for messages on {localIP} {port}")

        async with server:
            await server.serve_forever()

    async def handle_incoming_messages(self, reader, writer):
        try:
            addr = writer.get_extra_info('peername')
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Node {self.peerName} accepted connection from {addr}]")

            while True:
                data = await reader.read(200)
                if not data:
                    break
                dataObject = pickle.loads(data)
                request = dataObject['requestName']
                print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Handling the incoming request : {dataObject}]")


                if request == 'createTopic':
                    topic = dataObject['topic']
                    self.messageBuffer[topic] = []
                    self.subscriptions[topic] = []
                    response = f"Topic {topic} is created!"

                if request == 'deleteTopic':
                    topic = dataObject['topic']
                    if topic in self.messageBuffer:
                        del self.messageBuffer[topic] 
                        del self.subscriptions[topic]
                        response = f"Topic {topic} is deleted!"

                if request == 'send':
                    topic = dataObject['topic']
                    message = dataObject['message']
                    self.messageBuffer[topic].append(message)
                    response = f"Message '{message}' is added to {topic}"

                if request == "subscribe":
                    topic = dataObject['topic']
                    peerName = dataObject['caller']
                    if peerName not in self.subscriptions[topic]:
                        self.subscriptions[topic].append(peerName)
                        self.readHistory[(topic, peerName)] = 0
                        response = {"response" : f"Peer {peerName} is subscribed to {topic}!"}
                    else:
                        response = {"response" : f"Peer {peerName} is already subscribed to {topic}"}
                
                if request == "pull":
                    topic = dataObject['topic']
                    peerName = dataObject['caller']
                    if peerName not in self.subscriptions[topic]:
                        response = f"Peer {peerName} is not subscribed to {topic}"
                    else:
                        indexToStart = self.readHistory[(topic, peerName)]
                        if len(self.messageBuffer[topic]) > indexToStart and len(self.messageBuffer[topic]) != 0:
                            print(self.messageBuffer[topic][indexToStart::])
                            response = {'response' : self.messageBuffer[topic][indexToStart::]}
                            print(response)
                            lastIndexRead = len(self.messageBuffer[topic])
                            self.readHistory[(topic, peerName)] = lastIndexRead
                        elif len(self.messageBuffer[topic]) == indexToStart or len(self.messageBuffer[topic]) == 0:
                            response = {'response' : 'Empty'}
                            print(response)
                        
                        self.garbageCollector(topic)
                    
                    

                if request == "unregisterPeer":
                    for t, m in dataObject['messageBuffer'].items():
                        self.messageBuffer[t] = m
                    for t, p in dataObject['subscriptions'].items():
                        self.subscriptions[t] = p
                    for k, v in dataObject['readHistory'].items():
                        self.readHistory[k] = v

                    response = {'response' : 'All data has been transferred!'}

                print(f"    [[{time.ctime().split(' ')[3][:8]}]] : [Finished incoming request. Response back to peer node: {response}]\n")
                response = pickle.dumps(response)
                writer.write(response)
                await writer.drain()

                # print(f"\nMessage: {self.messageBuffer}")
                # print(f"\nSubscriptions: {self.subscriptions}")

        except Exception as e :
            print(f'\n{e}\n')
            # pass
        
        finally:    
            writer.close()
            await writer.wait_closed()

    async def benchmarkCreateTopic(self):
        startTime = time.time()
        numOfRequests = 0
        latency = 0
        while True:
                requestSend = time.time()
                topic = ''.join(random.choices(string.ascii_letters,k=4))
                timePassed = time.time() - startTime
                if timePassed >= 300:
                    break
                numOfRequests += 1
                print(numOfRequests)
                await self.createTopic(self.peerName, topic)
                requestReceived = time.time()
                latency += (requestReceived-requestSend)

        print(f"\n Number of request : {numOfRequests}, \ntimePassed: 5 mins, \nthroughput : {numOfRequests/300} \nLatency = {latency/numOfRequests}" )
    
    async def benchmarkDeleteTopic(self):
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(self.peerName, topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.deleteTopic(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n Number of request : {numOfRequests}, \ntimePassed: 5 mins, \nthroughput : {numOfRequests/300} \nLatency = {latency/numOfRequests}" )

    async def benchmarkSend(self):
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(self.peerName, topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            msg = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.send(topic, msg)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n Number of request : {numOfRequests}, \ntimePassed: 5 mins, \nthroughput : {numOfRequests/300} \nLatency = {latency/numOfRequests}" )

    async def benchmarkSubscribe(self):
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(self.peerName, topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            msg = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.subscribe(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n Number of request : {numOfRequests}, \ntimePassed: 5 mins, \nthroughput : {numOfRequests/300} \nLatency = {latency/numOfRequests}" )

    async def benchmarkPull(self):
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            msg = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(self.peerName, topic)
            await self.send(topic, msg)
            
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.pull(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n Number of request : {numOfRequests}, \ntimePassed: 5 mins, \nthroughput : {numOfRequests/300} \nLatency = {latency/numOfRequests}" )

    async def benchmarkQuery(self, topic):
        numOfRequests = 0
        latency = 0
        startTime = time.time()
        for x in range(100000):
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            numOfRequests += 1
            await self.queryTopics(topic, requestSend)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        
        
        print(f"\n Average respones time = {self.averageResponse/numOfRequests} \n" )


    async def userInput(self, apiName):
        print(f"\nStarting benchmarking {apiName}\n")
        if apiName =="createTopic":
            await self.benchmarkCreateTopic()
        if apiName =="deleteTopic":
            await self.benchmarkDeleteTopic()
        if apiName =="send":
            await self.benchmarkSend()
        if apiName =="subscribe":
            await self.benchmarkSubscribe()
        if apiName =="pull":
            await self.benchmarkPull()
        if apiName =="query":
            topic = ''.join(random.choices(string.ascii_letters,k=4)) 
            await self.benchmarkQuery(topic)

    def run_async_function(self, coroutine):
        asyncio.run(coroutine)

    async def run(self):
        try:
            portValue = await self.registerPeerNode()
            thread_one = threading.Thread(target=self.run_async_function, args=(self.listen_for_messages(),))
            thread_two = threading.Thread(target=self.run_async_function, args=(self.userInput(self.apiName),))
            thread_one.start()
            thread_two.start()

            thread_one.join()
            thread_two.join()

            print("thread finished")
        except Exception as e:
            print(f"Error occured: {e}")



if __name__ == "__main__":
   if len(sys.argv) > 2:
        peerName = sys.argv[1]
        apiName = sys.argv[2]
        peer_node = PeerNode(peerName, apiName)
        asyncio.run(peer_node.run())