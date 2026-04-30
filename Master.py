import socket
import json
import threading
import queue

# Configurações do Master
HOST = 'localhost'
PORT = 5000
MASTER_UUID = "Master_A"

# --- TAREFA 02: Fila de tarefas pendentes (Sprint 2) ---
# O Master agora gerencia quem deve ser processado
task_queue = queue.Queue()
for nome in ["Nicolas", "Michel", "Guilherme", "Gabriel"]:
    task_queue.put(nome)

def handle_worker(client_socket, address):
    print(f"[CONEXÃO] Worker conectado de {address}")
    buffer = ""

    try:
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if not data: break

            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip(): continue

                try:
                    payload = json.loads(line)

                    # 1. Mantém suporte ao Heartbeat da Sprint 1
                    if payload.get("TASK") == "HEARTBEAT":
                        print(f"[HEARTBEAT] Recebido de: {payload.get('SERVER_UUID')}")
                        response = {
                            "SERVER_UUID": MASTER_UUID,
                            "TASK": "HEARTBEAT",
                            "RESPONSE": "ALIVE"
                        }

                    # 2. NOVA LOGICA SPRINT 2: Apresentação e Pedido de Tarefa
                    elif payload.get("WORKER") == "ALIVE":
                        w_uuid = payload.get("WORKER_UUID")
                        print(f"[PEDIDO] Worker {w_uuid} solicitando tarefa.")

                        if not task_queue.empty():
                            user_task = task_queue.get()
                            response = {"TASK": "QUERY", "USER": user_task}
                            print(f"[FILA] Enviando usuário '{user_task}' para {w_uuid}")
                        else:
                            response = {"TASK": "NO_TASK"}

                    # 3. NOVA LOGICA SPRINT 2: Recebimento de Status e Envio de ACK
                    elif "STATUS" in payload and payload.get("TASK") == "QUERY":
                        w_uuid = payload.get("WORKER_UUID")
                        print(f"[STATUS] Worker {w_uuid} concluiu tarefa: {payload['STATUS']}")
                        # O ACK é obrigatório para confirmar o recebimento
                        response = {"STATUS": "ACK", "WORKER_UUID": w_uuid}

                    client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                except json.JSONDecodeError:
                    print("[ERRO] JSON inválido.")
    except Exception as e:
        print(f"[ERRO] Conexão com {address} encerrada.")
    finally:
        client_socket.close()

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"--- MASTER {MASTER_UUID} ONLINE (SPRINT 2) ---")

    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_worker, args=(client_sock, addr))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    start_master()