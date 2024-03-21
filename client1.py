import socket
import threading
import json

HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MSG = "disconnect"
SERVER = socket.gethostbyname(socket.gethostname())  # Get server IP
M_PORT = 6060  # Manager port
P_PORT = 49152  # Peer port
ADDR_M = (SERVER, M_PORT)
ADDR_P = (SERVER, P_PORT)
DISCONNECT_MSG = "disconnect"

# Global variable to store received storm event records
RECEIVED_RECORDS = []

# Function to handle incoming connections from other peers
def handle_peer_connection(conn, addr):
    print(f"[NEW CONNECTION] Peer {addr} connected.")

    connected = True
    while connected:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                print(f"Received message length: {msg_length}")
                msg = conn.recv(msg_length).decode(FORMAT)
                print(f"Received message: {msg}")
                if msg == DISCONNECT_MSG:
                    connected = False
                else:
                    # Process received message (e.g., store the received record)
                    if msg.startswith("store_record:"):
                        record_str = msg.split(":", 1)[1]
                        record = json.loads(record_str)
                        RECEIVED_RECORDS.append(record)
                        print(f"[{addr}] Received record: {record}")
                    elif msg.startswith("teardown-dht"):
                        # Perform teardown process
                        print(f"[{addr}] Received teardown command. Performing teardown...")
                        forward_teardown_command(addr)
                    else:
                        print(f"[{addr}] Unknown message: {msg}")
        except Exception as e:
            print(f"[ERROR] An error occurred: {e}")
            connected = False

    conn.close()


# Function to forward the teardown command to the next neighbor
def forward_teardown_command(addr):
    right_neighbor_port = 5051  # Example right neighbor's port
    neighbor_addr = (SERVER, right_neighbor_port)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        neighbor_client.connect(neighbor_addr)
        neighbor_client.send(DISCONNECT_MSG.encode(FORMAT))
        print(f"[FORWARD] Teardown command forwarded to the next neighbor.")
    except Exception as e:
        print(f"[ERROR] Unable to forward teardown command: {e}")
    finally:
        neighbor_client.close()

# Function to start listening for incoming connections from other peers
def start_peer():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR_P)
    server.listen()

    print(f"[LISTENING] Peer is listening on {SERVER}:{P_PORT}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_peer_connection, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

    server.close()

# Function to send a storm event record to the right neighbor
def send_store_command(record, right_neighbor_port):
    neighbor_addr = (SERVER, right_neighbor_port)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    neighbor_client.connect(neighbor_addr)
    message = json.dumps({"command": "store_record", "data": record})  # Serialize the record using JSON
    neighbor_client.send(message.encode(FORMAT))
    neighbor_client.close()


def main():
    peer_thread = threading.Thread(target=start_peer)
    peer_thread.start()
    print("[STARTED] Peer server started.")

    #start_peer()
    #Send a storm event record to the right neighbor
    record = {'event_id': 123, 'state': 'Texas', 'year': 2022}  # Example record
    right_neighbor_port = 5051  # Example right neighbor's port
    send_store_command(record, right_neighbor_port)

    send_teardown_command()

    # Keep the main thread alive
    peer_thread.join()

    print("[MAIN] Main function execution completed.")

def send_teardown_command():
    # Sending a teardown command to the neighbor
    neighbor_addr = (SERVER, P_PORT)
    neighbor_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        neighbor_client.connect(neighbor_addr)
        neighbor_client.send("teardown-dht".encode(FORMAT))
        print("[TEARDOWN] Teardown command sent to the neighbor.")
    except Exception as e:
        print(f"[ERROR] Unable to send teardown command: {e}")
    finally:
        neighbor_client.close()

if __name__ == "__main__":
    main()
