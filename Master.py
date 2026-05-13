import socket
import json
import threading
import queue
import logging

# Carregar configuração
config = json.load(open('config.json'))

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fila de tarefas
task_queue = queue.Queue()
for nome in ["Nicolas", "Michel", "Guilherme", "Gabriel","Paloma"]:
    task_queue.put(nome)

# Estado para negociação
active_workers = 0
borrowed_workers = {}  # {worker_uuid: neighbor}

def handle_worker(client_socket, address):
    global active_workers, borrowed_workers  # Declarar global no início da função
    logging.info(f"[CONEXÃO] Worker conectado de {address}")
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

                    if payload.get("TASK") == "HEARTBEAT":
                        logging.info(f"[HEARTBEAT] Recebido de: {payload.get('SERVER_UUID')}")
                        response = {
                            "SERVER_UUID": config['master_uuid'],
                            "TASK": "HEARTBEAT",
                            "RESPONSE": "ALIVE"
                        }

                    elif payload.get("WORKER") == "ALIVE":
                        w_uuid = payload.get("WORKER_UUID")
                        logging.info(f"[PEDIDO] Worker {w_uuid} solicitando tarefa.")
                        active_workers += 1

                        if not task_queue.empty():
                            user_task = task_queue.get()
                            response = {"TASK": "QUERY", "USER": user_task}
                            logging.info(f"[FILA] Enviando usuário '{user_task}' para {w_uuid}")
                        else:
                            response = {"TASK": "NO_TASK"}

                    elif "STATUS" in payload and payload.get("TASK") == "QUERY":
                        w_uuid = payload.get("WORKER_UUID")
                        logging.info(f"[STATUS] Worker {w_uuid} concluiu tarefa: {payload['STATUS']}")
                        active_workers -= 1
                        response = {"STATUS": "ACK", "WORKER_UUID": w_uuid}

                        # Novo: Se worker emprestado e carga baixa, devolva
                        if w_uuid in borrowed_workers and task_queue.qsize() <= config['threshold'] // 2:
                            neighbor = borrowed_workers[w_uuid]
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                    host, port = neighbor.split(':')
                                    sock.connect((host, int(port)))
                                    return_msg = {"TASK": "RETURN_WORKER", "WORKER_UUID": w_uuid}
                                    sock.sendall((json.dumps(return_msg) + "\n").encode('utf-8'))
                                    del borrowed_workers[w_uuid]
                                    logging.info(f"Worker {w_uuid} devolvido a {neighbor}")
                            except Exception as e:
                                logging.error(f"Erro ao devolver worker: {e}")

                    client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                except json.JSONDecodeError:
                    logging.error("[ERRO] JSON inválido.")
    except Exception as e:
        logging.error(f"[ERRO] Conexão com {address} encerrada: {e}")
    finally:
        client_socket.close()

def check_load_and_negotiate():
    global borrowed_workers  # Declarar global no início da função
    if task_queue.qsize() > config['threshold'] and not borrowed_workers:
        for neighbor in config['neighbors']:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    host, port = neighbor.split(':')
                    sock.connect((host, int(port)))
                    request = {"TASK": "REQUEST_HELP", "MASTER_UUID": config['master_uuid'], "NEEDED_WORKERS": 1}
                    sock.sendall((json.dumps(request) + "\n").encode('utf-8'))
                    response_data = sock.recv(1024).decode('utf-8').strip()
                    response = json.loads(response_data)
                    if response.get("TASK") == "GRANT_HELP":
                        worker_uuid = response.get("WORKER_UUID")
                        borrowed_workers[worker_uuid] = neighbor
                        # Instrua o worker a mudar (assumindo que o vizinho envia via seu socket)
                        logging.info(f"Worker {worker_uuid} emprestado de {neighbor}")
                        break  # Para um por vez
            except Exception as e:
                logging.error(f"Falha ao negociar com {neighbor}: {e}")

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((config['host'], config['port']))
    server.listen(5)
    logging.info(f"--- MASTER {config['master_uuid']} ONLINE (SPRINT 3) ---")

    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_worker, args=(client_sock, addr))
        thread.daemon = True
        thread.start()
        # Verifica carga periodicamente (simplificado; em produção, use timer)
        check_load_and_negotiate()

if __name__ == "__main__":
    start_master()