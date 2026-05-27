import socket
import json
import threading
import queue
import logging
import uuid
import time

# Carregar configuração
# Exemplo de config.json: {"host": "127.0.0.1", "port": 5001, "master_uuid": "Master_A", "threshold": 4, "neighbors": ["127.0.0.1:5002"]}
config = json.load(open('config.json'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fila de tarefas (Sprint 2)
task_queue = queue.Queue()
for nome in ["Nicolas", "Michel", "Guilherme", "Gabriel", "Paloma"]:
    task_queue.put(nome)

# Estados de Controle (Sprint 3)
borrowed_workers = {}      # {worker_uuid: original_master_address}
local_workers_sockets = {} # {worker_uuid: client_socket} (Para mandar redirects)

def handle_connection(client_socket, address):
    """ Centraliza a conexão e realiza a triagem inicial de pacotes (Strict Parsing) """
    global borrowed_workers, local_workers_sockets
    buffer = ""
    try:
        while True:
            client_socket.settimeout(5.0) # Timeout de segurança
            data = client_socket.recv(2048).decode('utf-8')
            if not data:
                break
            
            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                
                try:
                    msg = json.loads(line)
                    
                    # --- TRIAGEM SPRINT 3 (Protocolo P2P - Letras Minúsculas) ---
                    if "type" in msg:
                        msg_type = msg.get("type")
                        req_id = msg.get("request_id")
                        payload = msg.get("payload", {})
                        
                        if msg_type == "request_help":
                            handle_request_help(client_socket, req_id, payload)
                            return # Encerra pois o canal P2P é por requisição
                        
                        elif msg_type == "notify_worker_returned":
                            w_id = payload.get("worker_id")
                            logging.info(f"[P2P] Vizinho notificou retorno do Worker {w_id} para a Farm.")
                            return
                        
                        elif msg_type == "register_temporary_worker":
                            w_id = payload.get("worker_id")
                            orig_master = payload.get("original_master_address")
                            borrowed_workers[w_id] = orig_master
                            local_workers_sockets[w_id] = client_socket
                            logging.info(f"[P2P] Worker Temporário {w_id} registrado. Origem: {orig_master}")
                            # Não retorna, continua em loop para processar as tarefas dele
                    
                    # --- TRIAGEM SPRINT 1 e 2 (Protocolo Worker - Caixa Alta) ---
                    elif "TASK" in msg or "WORKER" in msg:
                        # Sprint 1: Heartbeat
                        if msg.get("TASK") == "HEARTBEAT":
                            response = {"SERVER_UUID": config['master_uuid'], "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        # Sprint 2: Solicitação de Tarefa
                        elif msg.get("WORKER") == "ALIVE":
                            w_uuid = msg.get("WORKER_UUID")
                            if w_uuid not in borrowed_workers:
                                local_workers_sockets[w_uuid] = client_socket # Registra socket local
                            
                            logging.info(f"[FILA] Worker {w_uuid} solicitando tarefa. Carga atual: {task_queue.qsize()}")
                            
                            if not task_queue.empty():
                                user_task = task_queue.get()
                                response = {"TASK": "QUERY", "USER": user_task}
                                logging.info(f"[FILA] Enviando '{user_task}' para Worker {w_uuid}")
                            else:
                                response = {"TASK": "NO_TASK"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        # Sprint 2: Reporte de Status do Processamento
                        elif "STATUS" in msg and msg.get("TASK") == "QUERY":
                            w_uuid = msg.get("WORKER_UUID")
                            status_res = msg.get("STATUS")
                            logging.info(f"[STATUS] Worker {w_uuid} reportou resultado: {status_res}")
                            
                            # Envia ACK imediatamente
                            ack = {"STATUS": "ACK", "WORKER_UUID": w_uuid}
                            client_socket.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                            
                            # Efeito Histerese / Devolução (Sprint 3)
                            limiar_liberacao = config['threshold'] // 2
                            if w_uuid in borrowed_workers and task_queue.qsize() <= limiar_liberacao:
                                orig_master = borrowed_workers[w_uuid]
                                logging.info(f"[DEVOLUÇÃO] Carga normalizada ({task_queue.qsize()}). Liberando Worker {w_uuid} para {orig_master}")
                                
                                # 2.5.a) command_release para o Worker
                                release_msg = {
                                    "type": "command_release",
                                    "request_id": str(uuid.uuid4()),
                                    "payload": {"original_master_address": orig_master}
                                }
                                client_socket.sendall((json.dumps(release_msg) + "\n").encode('utf-8'))
                                
                                # 2.5.b) notify_worker_returned para o vizinho (Thread separada para não bloquear)
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
                                
                                del borrowed_workers[w_uuid]
                                if w_uuid in local_workers_sockets: del local_workers_sockets[w_uuid]
                                return 
                except json.JSONDecodeError:
                    logging.error("[STRICT PARSING] Erro ao decodificar JSON.")
    except Exception as e:
        pass
    finally:
        client_socket.close()

def handle_request_help(client_socket, req_id, payload):
    """ Processa pedido de ajuda de outro Master """
    global local_workers_sockets
    # Condição para emprestar
    if task_queue.qsize() < config['threshold'] and local_workers_sockets:
        chosen_worker_id = list(local_workers_sockets.keys())[0]
        worker_sock = local_workers_sockets[chosen_worker_id]
        
        # Resposta de Aceite
        response = {
            "type": "response_accepted",
            "request_id": req_id,
            "payload": {
                "workers_offered": 1,
                "worker_details": [{"id": chosen_worker_id, "address": f"{config['host']}:{config['port']}"}]
            }
        }
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
        
        # Determina endereço do alvo saturado dinamicamente pelas configurações de vizinhos
        saturado_address = f"{config['neighbors'][0]}" 
        
        # Envia comando de redirecionamento para o Worker escolhido
        redirect = {
            "type": "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload": {"new_master_address": saturado_address}
        }
        try:
            worker_sock.sendall((json.dumps(redirect) + "\n").encode('utf-8'))
            del local_workers_sockets[chosen_worker_id]
            logging.info(f"[EMPRÉSTIMO] Worker {chosen_worker_id} redirecionado com sucesso para {saturado_address}")
        except Exception as e:
            logging.error(f"Falha ao enviar redirecionamento para o worker: {e}")
    else:
        # Resposta de Recusa
        response = {
            "type": "response_rejected",
            "request_id": req_id,
            "payload": {"reason": "high_load"}
        }
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))

def notify_neighbor_release(neighbor_address, worker_id):
    """ Avisa o Master de origem que o worker foi devolvido """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            host, port = neighbor_address.split(':')
            sock.connect((host, int(port)))
            msg = {
                "type": "notify_worker_returned",
                "request_id": str(uuid.uuid4()),
                "payload": {"worker_id": worker_id}
            }
            sock.sendall((json.dumps(msg) + "\n").encode('utf-8'))
    except Exception as e:
        logging.error(f"Erro ao notificar devolução para vizinho {neighbor_address}: {e}")

def check_load_and_negotiate_loop():
    """ Thread contínua monitorando saturação para evitar sobrecarga (Prazo Prod) """
    while True:
        time.sleep(2)
        if task_queue.qsize() > config['threshold']:
            logging.warning(f"[ALERTA SATURAÇÃO] Fila ({task_queue.qsize()}) > Limiar ({config['threshold']})!")
            for neighbor in config['neighbors']:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.settimeout(5.0)
                        host, port = neighbor.split(':')
                        sock.connect((host, int(port)))
                        
                        req_id = str(uuid.uuid4())
                        request = {
                            "type": "request_help",
                            "request_id": req_id,
                            "payload": {
                                "master_id": config['master_uuid'],
                                "current_load": task_queue.qsize(),
                                "capacity": config['threshold'],
                                "workers_needed": 1
                            }
                        }
                        sock.sendall((json.dumps(request) + "\n").encode('utf-8'))
                        
                        res_data = sock.recv(2048).decode('utf-8').strip()
                        if res_data:
                            if "\n" in res_data: res_data, _ = res_data.split("\n", 1)
                            res = json.loads(res_data)
                            if res.get("type") == "response_accepted":
                                logging.info(f"[P2P] Ajuda aceita pelo vizinho {neighbor}.")
                                break
                except socket.timeout:
                    logging.error(f"[CT07 TIMEOUT] Vizinho {neighbor} demorou mais de 5s para responder.")
                except Exception as e:
                    logging.error(f"Falha ao negociar com {neighbor}: {e}")

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((config['host'], config['port']))
    server.listen(10)
    logging.info(f"--- MASTER {config['master_uuid']} ONLINE (PORTA {config['port']} - SPRINT 1, 2 e 3 CONSOLIDADOS) ---")
    
    # Inicia monitor de carga dinâmico
    threading.Thread(target=check_load_and_negotiate_loop, daemon=True).start()

    while True:
        client_sock, addr = server.accept()
        thread = threading.Thread(target=handle_connection, args=(client_sock, addr))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    start_master()