import socket
import json
import threading

def handle_worker(client_socket, address):
    print(f"[CONEXÃO] Worker conectado de {address}")
    buffer = ""
    
    try:
        while True:
            # Recebe dados do socket
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break
            
            buffer += data
            # Processa mensagens delimitadas por \n
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                
                try:
                    payload = json.loads(line)
                    if payload.get("TASK") == "HEARTBEAT":
                        print(f"[REQUISIÇÃO] Heartbeat recebido de: {payload.get('SERVER_UUID')}")
                        
                        # Resposta oficial conforme o PDF
                        response = {
                            "SERVER_UUID": "Master_A", # Identificador do Master
                            "TASK": "HEARTBEAT",
                            "RESPONSE": "ALIVE"
                        }
                        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                except json.JSONDecodeError:
                    print("[ERRO] JSON inválido recebido.")

    except Exception as e:
        print(f"[ERRO] Conexão com {address} encerrada: {e}")
    finally:
        client_socket.close()

def start_master(host='localhost', port=5000):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"[SISTEMA] Master online em {host}:{port}. Aguardando Workers...")
    
    while True:
        client_sock, addr = server.accept()
        # Thread para não bloquear o Master
        thread = threading.Thread(target=handle_worker, args=(client_sock, addr))
        thread.start()

if __name__ == "__main__":
    start_master()