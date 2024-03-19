import socket

HEADER = 64
M_PORT = 6060 #changed later *
P_PORT = 5050 #changed later *
FORMAT = 'utf-8'
DISCONNECT_MSG = "disconnect"
SERVER = socket.gethostbyname(socket.gethostname()) #get server ip
ADDR_M = (SERVER, M_PORT)
COMMAND = "Register Frank <IP-address> <m-port> <p-port>"
RESPONSE_LENGTH = 2048

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR_M)


def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' *(HEADER -len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(RESPONSE_LENGTH).decode(FORMAT)) #2048 can change to length the message sent from server

send(COMMAND)
send("Register <F> <IP-address> <m-port> <p-port>")
send("Register <Fr> <IP-address> <m-port> <p-port>")
send("setup-dht Frank 3 2020")
send("dht-complete Frank")
send("Register <Fra> <IP-address> <m-port> <p-port>")
send("query-dht <Fra>")
send("leave-dht Frank")
send("Register <Fran> <IP-address> <m-port> <p-port>")
send("join-dht <Fran>")
send("dht-rebuilt Frank <Fra>")
send("deregister Frank")
send("teardown-dht Frank")
send("teardown-complete Frank")
send(DISCONNECT_MSG)