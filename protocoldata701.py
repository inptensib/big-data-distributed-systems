import socket

def recvAll(conn:socket.socket, length:int)-> bytes:
  data = b''
  while length > 0:
    chunk = conn.recv(length)
    if not chunk:
      return None
    data += chunk
    length -= len(chunk)
  return data

def receive_message(conn:socket.socket)->str:
    msg_length =  recvAll(conn, 4)
    if not msg_length:
        print('Data size not received')
        return None
    size = int.from_bytes(msg_length, byteorder='big')
    print('Size of message', str(size))

    message = b''
    message = recvAll(conn, size)
    if not message:
        print('Data not received')
        return None

    return message.decode()    


def send_message(conn:socket.socket,MESSAGETOSEND:str):
    conn.sendall(len(MESSAGETOSEND).to_bytes(4, byteorder='big'))
    conn.sendall(MESSAGETOSEND.encode())    