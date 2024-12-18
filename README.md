Welcome

Project : Peer-to-Peer publish-subscribe system

This project will support communication of central index server and multiple peer nodes. Communication is asyncronous.
Peer nodes can simultaneously publish and subscribe to a topic.

Main files are in 1-server/indexServer.py and 2 - nodes/mainNode.py

System support 6 APIs that user can call.
{CreateTopic, DeleteTopic, Send, Subscribe, Pull and Unregister}

First please install tmux. To do so just do to the main folder of this project and run following:

1. make initial

Peer nodes once connected to the index server, server assigns them a Port number. Peer node will use it for listening incoming
messages from other peers if so.

After instaling tmux, we can go ahead.

1. Open the terminal in the main folder and type tmux
   After that tmux plugin will be open.

2. Now you can split the screen into to part.
   If you want to split vertically, please press Ctrl+B and after that press % (dont keep holding Ctrl B)
   If you want horizontally, please press Ctrl+B and after that press " (dont keep holding Ctrl B)

   To switch between panes, you can press Ctrl+B and use left, right, up, down arrow to navitage.
   Once you split the screen, now we can get started.

3. First we need to run server. If you run client first, connection will be refused.

   You can run server by typing: make runServer
   Server will be waiting for clients to connect.

4. Once server listening, you can run the client. You will need to specify the peer node name when calling for the file to run.
   You can run client by typing :
   make runPeer peername=username

   P.S. In place of username you can specify name for your peer

5. From client side you will see "Which function do you want to call?"
   There are 6 different functions to call. Last option is 7 or exit.
   You just need to choose from 1 to 7 according to what you want to do.

6. If you choose:

   2. CreateTopic -> terminal will ask the peer name where you want to create it and topic name
      It will also be updates and added to the index server so it knows who is the holder of that topic

   3. Delete topic -> terminal will ask topic name to delete.
   4. Send Message -> terminal will prompt to ask for topic name and message.
   5. Subscribe -> it will subscribe you to the topic. Termninal will ask for the topic name. You have to be subscribed to
      be able to read messages from that topic.
      Note : you can't subscribe to your own topic
   6. Pull -> will get all unread messages from the topic. Terminal will ask you for the topic name.
      If you have previously read all messages from the topic, you will get Empty.
      Note: you can't pull messages from your topics as you are not subscriber. You are already an owner, what else needed xd
   7. Unregister -> Terminal will prompt to ask
      1 -> Delete user and all topics it has
      2 -> Transfer topics to other peer node and delete current node.
      Note : If you specify peer node name that doesn't exist to transfer, your topics will be gone.
      Please please use Unregister before using Ctrl-C, doing so server will remove the peer node from the list of active peers.

When you run the server and peer nodes, we will keep all logs of the operations!!!!

7.  To delete log files, please run: make clean
    Note: Logs are appending mode, if you clean logs from previous session, it will appends on top of data that already in the file

8.  Benchmarking:
    If you want to benchmark the APIs and see the number of requests handled by server, please do :

    1. Open the tmux in main folder and split the pane into 2 (vertically - Ctrl B + %, horizontally Ctrl B + ")
    2. In one pane please go to folder benchmarking. Inside folder benchmarking, please run python3 serverForBenchmarking.py
       It will run the server
    3. In other pane, (inside main folder) run make benchmark
       It will open a new session and run a peer node in the background session.
       If you want to see background sessions of peer, press Ctrl B + S
    4. There will be 2 sessions : windows (you are in this session) and first_session ( the peer node running in the background)

    If you want to run multiple peer nodes, please uncomment couple lines and specify the API name inside benchmarkAPI.sh.
    IMPORTANT NOTE: For benchmarking some APIs such as subscribe, pull, send ( all of these APIs require some topics to exist),
    I first create 25000 topics before benchmarking those APIs.

9.  For average response time:
    This is similar to the benchmarking steps. Just need to do the following:
    1. Uncomment lines from 10 to 17 in serverForBenchmarking.py
    2. Run python3 serverForBenchmarking.py
       Note: Give some time for the server to create topics.
    3. Choose "query" as apiName in the benchmarkAPI.sh. There's already a sample for you.
    4. Call make benchmark from the main folder
