import socket
import json
import time

def send_heartbeat(host='localhost', port=5000):
    worker_uuid = "Worker_A1"
    print(f"[SISTEMA] Iniciando Worker {worker_uuid}...")

    while True:
        try:
            # Tenta conectar ao Master
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                sock.settimeout(5.0) # Timeout para evitar travamentos
                
                while True:
                    # Payload oficial de envio
                    payload = {
                        "SERVER_UUID": worker_uuid,
                        "TASK": "HEARTBEAT"
                    }
                    
                    message = json.dumps(payload) + "\n"
                    sock.sendall(message.encode('utf-8'))
                    
                    # Aguarda resposta
                    data = sock.recv(1024).decode('utf-8')
                    if data:
                        response = json.loads(data.strip())
                        print(f"[LOG] Status: {response.get('RESPONSE')} (Master: {response.get('SERVER_UUID')})")
                    
                    # Intervalo entre heartbeats (ajustável)
                    time.sleep(10)
                    
        except (ConnectionRefusedError, socket.error):
            print("[LOG] Status: OFFLINE - Tentando Reconectar em 5s...")
            time.sleep(5)

if __name__ == "__main__":
    send_heartbeat()