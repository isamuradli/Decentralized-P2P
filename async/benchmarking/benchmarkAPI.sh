tmux new-session -d -s first_session
# tmux new-session -d -s second_session
# tmux new-session -d -s third_session]


tmux send-keys -t first_session:0 "python3 benchmarking/benchmarkAPI.py Peer1 createTopic" C-m
# tmux send-keys -t second_session:0 "python3 benchmarking/benchmarkAPI.py Peer2 deleteTopic" C-m
# tmux send-keys -t third_session:0 "python3 benchmarking/benchmarkAPI.py Peer3 query" C-m

#Please choose on of the APIs
#createTopic
#deleteTopic
#send
#subscribe
#pull
#query