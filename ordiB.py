import socket

HOST = '0.0.0.0'  # Écoute sur toutes les interfaces
PORT = 19999

def recvAll(conn:socket.socket, length:int):
  data = b''
  while length > 0:
    chunk = conn.recv(length)
    if not chunk:
      return None
    data += chunk
    length -= len(chunk)
  return data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    size = 0
    with conn:
        print('Connecté par', addr)
        msg_length =  recvAll(conn, 4)
        if not msg_length:
            print('Data size not received')
            exit(1)
        size = int.from_bytes(msg_length, byteorder='big')
        print('Size of message', str(size))

        message = b''
        message = recvAll(conn, size)
        if not message:
            print('Data not received')
            exit(1)
        print('Données reçues:', message.decode())


