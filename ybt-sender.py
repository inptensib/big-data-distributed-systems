import socket
import sys
import protocoldata701 as pd

PORT = 19999           # Port choisi
MESSAGE = 'Bonjour'



def main():
     if len(sys.argv) < 2:
        print('Usage: python3 ybt-sender.py <IP1> <IP2> ... <IPn>')
        exit(1)
     for i in range(len(sys.argv) - 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
          print('----------------------------------\n')
          print('Sending message to', sys.argv[i + 1])
          print('----------------------------------\n')
          s.connect((sys.argv[i + 1], PORT))
          pd.send_message(s,MESSAGE)
          print('Message sent:', MESSAGE)
          
        

          received_message = pd.receive_message(s)
          if received_message is None:
              print('No data received')
          print('----------------------------------\n')
          print('Sender : ',sys.argv[i + 1])
          print('----------------------------------\n')
          print('Données reçues:',received_message)
          s.close()

if __name__ == "__main__":
    sys.exit(main())
