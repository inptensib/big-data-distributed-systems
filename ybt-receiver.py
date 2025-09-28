import socket
import protocoldata701 as pd

HOST = 'localhost'  # Écoute sur toutes les interfaces
PORT = 19999
MESSAGETOSEND= 'Salut'



with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    size = 0
    with conn:
        print('Connecté par', addr)
        received_message=pd.receive_message(conn)
        if received_message is None:
            print('No data received')
        pd.send_message(conn,MESSAGETOSEND)
        conn.close()
    s.close()


