import socket 
import threading 
import random

HEADER = 64
M_PORT = 6060
P_PORT = 0 
SERVER = socket.gethostbyname(socket.gethostname())
IP_ADDRESS = ""
ADDR = (SERVER, M_PORT)
FORMAT = 'utf-8'
DISCONNECT_MSG = "disconnect"
PEER_INFO = {}
DHT_SET_UP = False
LEADER_NAME = None

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def teardown_complete(peer_name):
    global DHT_SET_UP

    # Check if peer is the leader of the DHT
    if peer_name != PEER_INFO.get('leader'):
        return "FAILURE: Peer is not the leader of the DHT"

    # Change the state of each peer involved in maintaining the DHT to "Free"
    for peer, info in PEER_INFO.items():
        if info.get('STATE') == 'InDHT':
            PEER_INFO[peer]['STATE'] = 'Free'

    # Reset DHT_SET_UP flag
    DHT_SET_UP = False

    return "SUCCESS"

def teardown_dht(peer_name):
    global DHT_SET_UP

    # Check if peer is the leader of the DHT
    if peer_name != PEER_INFO.get('leader'):
        return "FAILURE: Peer is not the leader of the DHT"

    # Initiating the teardown process by sending a teardown command to the leader's right neighbor
    

    # Deleting local hash table
    

    # Sending message for teardown-complete command to the server
    

    return "SUCCESS"


def deregister(peer_name):
    # Check if peer state is InDHT
    if PEER_INFO.get(peer_name, {}).get('STATE') == 'InDHT':
        return "FAILURE: Peer state is InDHT"
    
    # Remove peer's state information
    if peer_name in PEER_INFO:
        del PEER_INFO[peer_name]
    
    return "SUCCESS"


def dht_rebuilt(peer_name, new_leader): #might have error here
    global DHT_SET_UP

    # Check if peer name matches the one initiating leave-dht or join-dht
    if peer_name not in [PEER_INFO.get(peer, {}).get('NAME') for peer in ['leave-dht', 'join-dht']]:
        return "FAILURE: Peer name does not match the one initiating leave-dht or join-dht"

    # Set state of peer as appropriate
    if peer_name == 'leave-dht':
        PEER_INFO[peer_name]['STATE'] = 'Free'
    elif peer_name == 'join-dht':
        PEER_INFO[peer_name]['STATE'] = 'InDHT'

    # Handle any necessary state changes, such as assigning a new leader

    return "SUCCESS"


def join_dht(peer_name):
    global DHT_SET_UP

    # Check if DHT exists
    if not DHT_SET_UP:
        return "FAILURE: DHT does not exist"

    # Check if peer is in the "Free" state
    if PEER_INFO.get(peer_name, {}).get('STATE') != 'Free':
        return "FAILURE: Peer is not Free"

    # Mark peer for joining the DHT
    PEER_INFO[peer_name]['STATE'] = 'join-dht'
    return "SUCCESS"


def leave_dht(peer_name):
    global DHT_SET_UP

    # Check if DHT exists
    if not DHT_SET_UP:
        return "FAILURE: DHT does not exist"

    # Check if peer is maintaining the DHT
    if PEER_INFO.get(peer_name, {}).get('STATE') != 'InDHT':
        return "FAILURE: Peer is not maintaining the DHT"

    # Mark peer for leaving the DHT
    PEER_INFO[peer_name]['STATE'] = 'leave-dht'
    return "SUCCESS"


def query_dht(peer_name):
    global DHT_SET_UP

    # Check if DHT setup has been completed
    if not DHT_SET_UP:
        return "FAILURE: DHT setup has not been completed"

    # Check if the peer is registered and in the "Free" state
    if peer_name not in PEER_INFO:
        return "FAILURE: Peer is not registered"
    if PEER_INFO[peer_name]['STATE'] != 'Free':
        return "FAILURE: Peer is not Free"

    # Choose a random peer maintaining the DHT
    dht_peers = [peer for peer in PEER_INFO.keys() if PEER_INFO[peer]['STATE'] == 'InDHT']
    selected_peer = random.choice(dht_peers)
    selected_peer_info = (selected_peer, PEER_INFO[selected_peer]['IP_ADDRESS'], PEER_INFO[selected_peer]['P_PORT'])

    return "SUCCESS", selected_peer_info


def setup_dht(peer_name, n, year):
    global DHT_SET_UP, LEADER_NAME

    # Check if DHT has already been set up
    if DHT_SET_UP:
        return "FAILURE: DHT has already been set up"

    # Check if peer-name is registered
    if peer_name not in PEER_INFO:
        return "FAILURE: Peer name is not registered"

    # Check if n is at least three
    if n < 3:
        return "FAILURE: n must be at least three"

    # Check if there are enough registered users
    if len(PEER_INFO) < n:
        return "FAILURE: Not enough users registered with the manager"

    # Set up DHT
    leader_info = (peer_name, PEER_INFO[peer_name]['IP_ADDRESS'], PEER_INFO[peer_name]['P_PORT'])
    dht_peers = [leader_info]
    selected_peers = random.sample(list(PEER_INFO.keys()), n-1)
    for peer in selected_peers:
        peer_info = (peer, PEER_INFO[peer]['IP_ADDRESS'], PEER_INFO[peer]['P_PORT'])
        dht_peers.append(peer_info)

    # Update states of peers
    PEER_INFO[peer_name]['STATE'] = 'Leader'
    for peer in selected_peers:
        PEER_INFO[peer]['STATE'] = 'InDHT'

    DHT_SET_UP = True
    LEADER_NAME = peer_name
    return "SUCCESS"


def handle_client(conn, addr):
    print(f"[new connection] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            msg, *params = msg.split()

            if msg == DISCONNECT_MSG:
                connected = False

            elif msg.lower() == "register" and len(params) == 4:
                peer_name, ip_address, m_port, p_port = params
                REGISTER = True

                if peer_name in PEER_INFO:
                    response = "FAILURE: Peer name already registered"
                else:   # Store peer information
                    PEER_INFO[peer_name] = {
                        'IP_ADDRESS': ip_address,
                        'M_PORT': m_port,
                        'P_PORT': p_port
                    }
                    PEER_INFO[peer_name]['STATE'] = 'Free'
                    response = "SUCCESS: Peer registered successfully"
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "setup-dht" and len(params) == 3:
                peer_name, n, year = params
                n = int(n)
                year = int(year)
                response = setup_dht(peer_name, n, year)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "dht-complete" and len(params) == 1:
                leader_name = params[0]
                if leader_name == LEADER_NAME:
                    conn.send("SUCCESS".encode(FORMAT))
                else:
                    conn.send("FAILURE: Not the leader of the DHT".encode(FORMAT))

            elif msg.lower() == "query-dht" and len(params) == 1:
                peer_name = params[0]
                response = query_dht(peer_name)
                if response[0] == "SUCCESS":
                    conn.send(response[0].encode(FORMAT))
                    conn.send(str(response[1]).encode(FORMAT))  # Sending selected peer info
                else:
                    conn.send(response.encode(FORMAT))

            elif msg.lower() == "leave-dht" and len(params) == 1:
                peer_name = params[0]
                response = leave_dht(peer_name)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "join-dht" and len(params) == 1:
                peer_name = params[0]
                response = join_dht(peer_name)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "dht-rebuilt" and len(params) == 2:
                peer_name, new_leader = params
                response = dht_rebuilt(peer_name, new_leader)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "deregister" and len(params) == 1:
                peer_name = params[0]
                response = deregister(peer_name)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "teardown-dht" and len(params) == 1:
                peer_name = params[0]
                response = teardown_dht(peer_name)
                conn.send(response.encode(FORMAT))

            elif msg.lower() == "teardown-complete" and len(params) == 1:
                peer_name = params[0]
                response = teardown_complete(peer_name)
                conn.send(response.encode(FORMAT))


            else:
                conn.send("Invalid command".encode(FORMAT))

            print(f"[{addr}] {msg}")

    conn.close()

def start():
    server.listen()
    print(f"[listening] Server is running on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[active connection] {threading.active_count()-1}")

print("[starting]")
start()
