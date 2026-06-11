import socket
import json
import threading
import queue
import logging
import uuid
import time
import os 

# Carregar configuração
config = json.load(open('config.json'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fila de tarefas
task_queue = queue.Queue()
for nome in ["Nicolas", "Michel", "Guilherme", "Gabriel", "Paloma"]:
    task_queue.put(nome)

# Estados de Controle
borrowed_workers = {}      # {worker_uuid: original_master_address}
<<<<<<< HEAD
local_workers_sockets = {} # {worker_uuid: client_socket}

def handle_connection(client_socket, address):
    global borrowed_workers, local_workers_sockets
=======
local_workers_sockets = {} # {worker_uuid: client_socket} (Para mandar redirects)
workers_active_count = 0   # Contador de tarefas sendo processadas

def handle_connection(client_socket, address):
    """ Centraliza a conexão e realiza a triagem inicial de pacotes (Strict Parsing) """
    global borrowed_workers, local_workers_sockets, workers_active_count
>>>>>>> cac3cbf (Correções para sprint 3)
    buffer = ""
    try:
        while True:
            client_socket.settimeout(5.0)
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
                    
                    if "type" in msg:
                        msg_type = msg.get("type")
                        req_id = msg.get("request_id")
                        payload = msg.get("payload", {})
                        
                        if msg_type == "request_help":
                            handle_request_help(client_socket, req_id, payload)
<<<<<<< HEAD
                            return
=======
                            return 
>>>>>>> cac3cbf (Correções para sprint 3)
                        
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
                    
                    elif "TASK" in msg or "WORKER" in msg:
                        if msg.get("TASK") == "HEARTBEAT":
                            response = {"SERVER_UUID": config['master_uuid'], "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        elif msg.get("WORKER") == "ALIVE":
                            w_uuid = msg.get("WORKER_UUID")
                            if w_uuid not in borrowed_workers:
                                local_workers_sockets[w_uuid] = client_socket
<<<<<<< HEAD
=======
                            
                            logging.info(f"[FILA] Worker {w_uuid} solicitando tarefa. Carga atual: {task_queue.qsize()}")
>>>>>>> cac3cbf (Correções para sprint 3)
                            
                            if not task_queue.empty():
                                user_task = task_queue.get()
                                workers_active_count += 1 
                                response = {"TASK": "QUERY", "USER": user_task}
                            else:
                                response = {"TASK": "NO_TASK"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        elif "STATUS" in msg and msg.get("TASK") == "QUERY":
                            w_uuid = msg.get("WORKER_UUID")
                            status_res = msg.get("STATUS")
                            logging.info(f"[STATUS] Worker {w_uuid} reportou resultado: {status_res}")
                            
                            ack = {"STATUS": "ACK", "WORKER_UUID": w_uuid}
                            client_socket.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                            
<<<<<<< HEAD
=======
                            workers_active_count -= 1 
                            
                            if task_queue.empty() and workers_active_count == 0:
                                logging.info("Todas as tarefas concluídas. Encerrando Master.")
                                os._exit(0)
                            
                            # Correção SPRINT 3: Devolução sem fechar socket abruptamente
>>>>>>> cac3cbf (Correções para sprint 3)
                            limiar_liberacao = config['threshold'] // 2
                            if w_uuid in borrowed_workers and task_queue.qsize() <= limiar_liberacao:
                                orig_master = borrowed_workers[w_uuid]
                                logging.info(f"[DEVOLUÇÃO] Carga normalizada. Agendando liberação de {w_uuid} para {orig_master}")
                                
<<<<<<< HEAD
                                # 2.5.a) Comando para o Worker
=======
>>>>>>> cac3cbf (Correções para sprint 3)
                                release_msg = {
                                    "type": "command_release",
                                    "request_id": str(uuid.uuid4()),
                                    "payload": {"original_master_address": orig_master}
                                }
                                client_socket.sendall((json.dumps(release_msg) + "\n").encode('utf-8'))
<<<<<<< HEAD
                                
                                # Pequeno delay para garantir envio antes de fechar/deletar
                                time.sleep(0.1)
                                
                                # 2.5.b) Notificação de devolução
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
=======
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
>>>>>>> cac3cbf (Correções para sprint 3)
                                del borrowed_workers[w_uuid]
                                if w_uuid in local_workers_sockets: 
                                    del local_workers_sockets[w_uuid]
                
                except json.JSONDecodeError:
                    logging.error("[STRICT PARSING] Erro ao decodificar JSON.")
    except Exception as e:
        pass
    finally:
        client_socket.close()

# ... (funções handle_request_help, notify_neighbor_release, check_load_and_negotiate_loop e start_master permanecem inalteradas)

def handle_request_help(client_socket, req_id, payload):
    global local_workers_sockets
    if task_queue.qsize() < config['threshold'] and local_workers_sockets:
        chosen_worker_id = list(local_workers_sockets.keys())[0]
        worker_sock = local_workers_sockets[chosen_worker_id]
<<<<<<< HEAD
        
=======
>>>>>>> cac3cbf (Correções para sprint 3)
        response = {
            "type": "response_accepted",
            "request_id": req_id,
            "payload": {
                "workers_offered": 1,
                "worker_details": [{"id": chosen_worker_id, "address": f"{config['host']}:{config['port']}"}]
            }
        }
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
<<<<<<< HEAD
        
=======
>>>>>>> cac3cbf (Correções para sprint 3)
        saturado_address = f"{config['neighbors'][0]}" 
        redirect = {
            "type": "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload": {"new_master_address": saturado_address}
        }
        try:
            worker_sock.sendall((json.dumps(redirect) + "\n").encode('utf-8'))
            del local_workers_sockets[chosen_worker_id]
            logging.info(f"[EMPRÉSTIMO] Worker {chosen_worker_id} redirecionado para {saturado_address}")
        except Exception as e:
            logging.error(f"Falha ao enviar redirecionamento: {e}")
    else:
<<<<<<< HEAD
        response = {
            "type": "response_rejected",
            "request_id": req_id,
            "payload": {"reason": "high_load"}
        }
=======
        response = {"type": "response_rejected", "request_id": req_id, "payload": {"reason": "high_load"}}
>>>>>>> cac3cbf (Correções para sprint 3)
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))

def notify_neighbor_release(neighbor_address, worker_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5.0)
            host, port = neighbor_address.split(':')
            sock.connect((host, int(port)))
            msg = {
                "type": "notify_worker_returned",
                "request_id": str(uuid.uuid4()),
                "payload": {"worker_id": worker_id}
            }
            sock.sendall((json.dumps(msg) + "\n").encode('utf-8'))
            logging.info(f"[NOTIFICAÇÃO] Devolução de {worker_id} confirmada para {neighbor_address}")
    except Exception as e:
        logging.error(f"Erro ao notificar vizinho: {e}")

def check_load_and_negotiate_loop():
    while True:
        time.sleep(2)
        if task_queue.qsize() > config['threshold']:
            for neighbor in config['neighbors']:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.settimeout(5.0)
                        host, port = neighbor.split(':')
                        sock.connect((host, int(port)))
<<<<<<< HEAD
                        req_id = str(uuid.uuid4())
=======
>>>>>>> cac3cbf (Correções para sprint 3)
                        request = {
                            "type": "request_help",
                            "request_id": str(uuid.uuid4()),
                            "payload": {"master_id": config['master_uuid'], "current_load": task_queue.qsize()}
                        }
                        sock.sendall((json.dumps(request) + "\n").encode('utf-8'))
<<<<<<< HEAD
                        res_data = sock.recv(2048).decode('utf-8').strip()
                        if res_data:
                            if "\n" in res_data: res_data, _ = res_data.split("\n", 1)
                            res = json.loads(res_data)
                            if res.get("type") == "response_accepted":
                                break
=======
                        break
>>>>>>> cac3cbf (Correções para sprint 3)
                except Exception as e:
                    logging.error(f"Falha ao negociar: {e}")

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((config['host'], config['port']))
    server.listen(10)
    logging.info(f"--- MASTER {config['master_uuid']} ONLINE ---")
    threading.Thread(target=check_load_and_negotiate_loop, daemon=True).start()
    while True:
        client_sock, addr = server.accept()
        threading.Thread(target=handle_connection, args=(client_sock, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()