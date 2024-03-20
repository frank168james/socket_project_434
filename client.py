import socket
import csv 
import hashlib 

HEADER = 64
M_PORT = 6060 #changed later *
P_PORT = 5050 #changed later *
FORMAT = 'utf-8'
DISCONNECT_MSG = "disconnect"
SERVER = socket.gethostbyname(socket.gethostname()) #get server ip
ADDR_M = (SERVER, M_PORT)
COMMAND = "Register Frank <IP-address> <m-port> <p-port>"
RESPONSE_LENGTH = 2048
FILE_NAME = r"C:/Users/frank/Downloads/1950-1952 (1)/1950-1952/details-1950.csv"
LOCAL_HASH_TABLE = {}

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
    neighbor_addr = (SERVER, P_PORT)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    neighbor_client.connect(neighbor_addr)

    # Send the storm event record to the right neighbor
    message = "store_record:" + str(record)  # You may need to serialize the record appropriately
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



def setup_dht(peer_name, n, year):
    command = f"setup-dht {peer_name} {n} {year}"
    send(command)

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' *(HEADER -len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(RESPONSE_LENGTH).decode(FORMAT)) #2048 can change to length the message sent from server



ring_size = 5  # Example ring size
peers = {0: 'Leader', 1: 'Peer1', 2: 'Peer2', 3: 'Peer3', 4: 'Peer4'}  # Example peers
#setup_dht("Frank", 5, 2022)
hash_table_size = compute_hash_table_size(len(read_storm_events(FILE_NAME)))  # Calculate hash table size
distribute_storm_events(FILE_NAME, ring_size, hash_table_size)
#send(COMMAND)
#send("Register <F> <IP-address> <m-port> <p-port>")
#send("Register <Fr> <IP-address> <m-port> <p-port>")
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