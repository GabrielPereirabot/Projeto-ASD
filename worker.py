import socket
import json
import time
import random

def start_worker(host='localhost', port=5000):
    worker_uuid = "Worker_A1"
    master_vinc = "Master_A"
    print(f"[SISTEMA] Iniciando Worker {worker_uuid}...")

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                sock.settimeout(10.0)
                print(f"[CONEXÃO] Conectado ao Master.")

                while True:
                    # TAREFA 01: Handshake de Apresentação[cite: 1]
                    payload = {
                        "WORKER": "ALIVE",
                        "WORKER_UUID": worker_uuid,
                        "SERVER_UUID": master_vinc
                    }
                    sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))

                    # TAREFA 02: Recebendo a Tarefa (User)[cite: 1]
                    data = sock.recv(1024).decode('utf-8')
                    if not data: break
                    response = json.loads(data.strip())

                    if response.get("TASK") == "QUERY":
                        user = response.get("USER")
                        print(f"[TRABALHO] Processando usuário: {user}...")

                        # TAREFA 03: Simulação de Processamento[cite: 1]
                        time.sleep(random.uniform(2, 4))

                        # Reportando o Status
                        report = {
                            "STATUS": "OK",
                            "TASK": "QUERY",
                            "WORKER_UUID": worker_uuid
                        }
                        sock.sendall((json.dumps(report) + "\n").encode('utf-8'))

                        # TAREFA 04: Esperando o ACK do Master[cite: 1]
                        ack_data = sock.recv(1024).decode('utf-8')
                        if ack_data and json.loads(ack_data).get("STATUS") == "ACK":
                            print(f"[SUCESSO] Tarefa '{user}' confirmada pelo Master.")

                    elif response.get("TASK") == "NO_TASK":
                        print("[FILA] Sem tarefas. Aguardando 10s...")
                        time.sleep(10)

                    time.sleep(1) # Pequena pausa entre ciclos

        except (socket.error, json.JSONDecodeError):
            print("[LOG] Status: OFFLINE - Reconectando em 5s...")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()