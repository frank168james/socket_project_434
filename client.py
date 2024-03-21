import socket
import csv 
import hashlib 
import json

HEADER = 64
M_PORT = 6060 #changed later *
P_PORT = 50001 #changed later *
FORMAT = 'utf-8'
DISCONNECT_MSG = "disconnect"
SERVER = socket.gethostbyname(socket.gethostname()) #get server ip
ADDR_M = (SERVER, M_PORT)
COMMAND = "Register Frank 192.168.56.1 6060 5050"
RESPONSE_LENGTH = 2048
FILE_NAME = r"C:/Users/frank/Downloads/1990-1992/processed/details-1992.csv"
#"C:/Users/frank/Downloads/1950-1952 (1)/1950-1952/details-1950.csv" 
LOCAL_HASH_TABLE = {}
DHT_SET_UP = False
PEER_INFO = {}
LEADER_NAME={}

#connection with server
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR_M)


# Function to read storm event data from the CSV file
def read_storm_events(filename):
    events = []
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            event = {
                'event_id': int(row[0]),
                'state': row[1],
                'year': int(row[2]),
                'month_name': row[3],
                'event_type': row[4],
                'cz_type': row[5],
                'cz_name': row[6],
                'injuries_direct': int(row[7]),
                'injuries_indirect': int(row[8]),
                'deaths_direct': int(row[9]),
                'deaths_indirect': int(row[10]),
                'damage_property': row[11],
                'damage_crops': row[12],
                'tor_f_scale': row[13]
            }
            events.append(event)
    return events

# Function to compute the hash table size
def compute_hash_table_size(num_events):
    # Find the first prime number larger than 2 * num_events
    prime = 2 * num_events + 1
    while not is_prime(prime):
        prime += 1
    return prime

# Function to compute the identifier using the second hash function
def compute_identifier(event_id, hash_table_size, ring_size):
    pos = event_id % hash_table_size
    return pos % ring_size

# Function to check if a number is prime
def is_prime(n):
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

# Function to store the record in the local hash table
def store_locally(record):
     LOCAL_HASH_TABLE[record['event_id']] = record
    

def send_store_command(record, right_neighbor_port):
    # Establish a connection with client1 using its P_PORT
    neighbor_addr = (SERVER, right_neighbor_port)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    neighbor_client.connect(neighbor_addr)

    # Send the storm event record to the right neighbor
    message = "store_record:" + json.dumps(record)  # Serialize the record as JSON
    message = f"{len(message):<{HEADER}}" + message  # Prefix the message with its length
    neighbor_client.send(message.encode(FORMAT))

    # Close the connection
    neighbor_client.close()

# Main function to distribute storm events
def distribute_storm_events(filename, ring_size, hash_table_size):
    events = read_storm_events(filename)
    for event in events:
        # Compute identifier using the second hash function
        identifier = compute_identifier(event['event_id'], hash_table_size, ring_size)
        
        # If the identifier is for the current node, store the record locally
        if identifier == 0:  # Assuming the leader is always the node with identifier 0
            store_locally(event)
        else:
            # Send a store command to the right neighbor
            send_store_command(event, P_PORT)  


def teardown_dht(peer_name):
    global DHT_SET_UP, LOCAL_HASH_TABLE

    if not 'LOCAL_HASH_TABLE' in globals():
        return "FAILURE: LOCAL_HASH_TABLE is not initialized"

    # Check if peer is the leader of the DHT
    if peer_name != LEADER_NAME:
        return "FAILURE: Peer is not the leader of the DHT"

    # Establish a connection with the right neighbor using its P_PORT
    neighbor_addr = (SERVER, P_PORT)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    neighbor_client.connect(neighbor_addr)

    # Send the teardown command to the right neighbor
    command = "teardown-dht"
    message = f"{command} {peer_name}"
    message = f"{len(message):<{HEADER}}" + message  # Prefix the message with its length
    neighbor_client.send(message.encode(FORMAT))

    # Close the connection with the neighbor
    neighbor_client.close()

    # Handle the teardown process locally
    if peer_name == LEADER_NAME:
        # If the peer is the leader, delete its local hash table and send a message for the teardown-complete command to the server
        del LOCAL_HASH_TABLE
        send(f"teardown-complete {peer_name}")
    else:
        # If the peer is not the leader, simply delete its local hash table
        del LOCAL_HASH_TABLE[peer_name]

    return "SUCCESS"


def join_dht(peer_name):
    command = f"join-dht {peer_name}"
    response = send(command)
    if "SUCCESS" in response:
        # Extract new leader information from the response
        parts = response.split()
        if len(parts) == 3 and parts[0] == "SUCCESS":
            new_leader = parts[1]
            # Send a message for the dht-rebuilt command to the manager specifying the new leader
            send(f"dht-rebuilt {peer_name} {new_leader}")
    return response


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


# Function to query the DHT for a storm event
def query_dht(event_id):
    command = f"query-dht {event_id}"
    send(command)


def setup_dht(peer_name, n, year):
    command = f"setup-dht {peer_name} {n} {year}"
    LEADER_NAME = send(command)

def send(msg):
    global PEER_INFO
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    response = client.recv(RESPONSE_LENGTH).decode(FORMAT)
    print(response)
    if "SUCCESS" in response:
        parts = msg.split()
        if len(parts) == 4 and parts[0] == "Register":
            peer_name = parts[1]
            PEER_INFO[peer_name] = {
                'IP_ADDRESS': parts[2],
                'M_PORT': parts[3],
                'P_PORT': parts[4],
                'STATE': 'Free'  # Assuming all registered peers start in a "Free" state
            }
    return response


#print(SERVER)
ring_size = 5  # Example ring size
peers = {0: 'Frank', 1: 'F', 2: 'Fa', 3: 'Fr', 4: 'Fra'}  # Example peers
send("Register Frank 192.168.56.1 6060 49152")
send("Register F 192.168.56.1 6060 49153")
send("Register Fa 192.168.56.1 6060 49154")
send("Register Fr 192.168.56.1 6060 49155")
send("Register Fra 192.168.56.1 6060 49156")
setup_dht("Frank", 5, 2022)
hash_table_size = compute_hash_table_size(len(read_storm_events(FILE_NAME)))  # Calculate hash table size
distribute_storm_events(FILE_NAME, ring_size, hash_table_size)
send("Register Fran 192.168.56.1 6060 49157")
query_dht("Fran")
leave_dht("Frank")
join_dht("Fran")
send("dht-rebuilt Fran Frank")
send("Register Frank1 192.168.56.1 6060 49157")
send("deregister Frank1")
teardown_dht("Frank")
#peers = {0: 'Peer1', 1: 'Peer2', 2: 'Peer3', 3: 'Peer4', 4: 'Peer5', 5: 'Peer6'}

#send(f"Register Peer1 192.168.56.1 6060 49152")
#send(f"Register Peer2 192.168.56.1 6060 49153")
#send(f"Register Peer3 192.168.56.1 6060 49154")
#send(f"Register Peer4 192.168.56.1 6060 49155")
#send(f"Register Peer5 192.168.56.1 6060 49156")
#send(f"Register Peer6 192.168.56.1 6060 49157")
#setup_dht("Peer1", 5, 1996)

#event_ids = [5536849, 2402920, 5539287, 55770111]
#for event_id in event_ids:
 #   query_dht(event_id)

#leave_dht("Peer2")
#query_dht(5536849)  # Query by the peer that left the DHT

#join_dht("Peer6")
#query_dht(55770111)  # Query by the remaining peer outside the DHT

#teardown_dht("Peer1")

# Graceful termination
print("Gracefully terminating...")
print("Terminated.")
#send(COMMAND)
#send("dht-complete Frank")
#send("Register <Fra> <IP-address> <m-port> <p-port>")
#send("query-dht <Fra>")
#send("leave-dht Frank")
#send("Register <Fran> <IP-address> <m-port> <p-port>")
#send("join-dht <Fran>")
#send("dht-rebuilt Frank <Fra>")
#send("deregister Frank")
#send("teardown-dht Frank")
#send("teardown-complete Frank")
send(DISCONNECT_MSG)