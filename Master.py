import socket
import json
import threading
import queue
import logging
import uuid
import time
from datetime import datetime, timezone
import ssl
import psutil

# Config
config = json.load(open('config.json'))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Estado e estruturas
start_time = time.time()
tasks_completed_counter = 0
tasks_failed_counter = 0

task_queue = queue.Queue()
for nome in range(0):
    task_queue.put((f"User_{nome}", time.time()))

borrowed_workers = {}      # worker_uuid -> original_master_address (in)
lent_workers = {}          # worker_uuid -> destination_master_address (out)
local_workers_sockets = {} # worker_uuid -> socket
running_tasks = {}         # worker_uuid -> task_name

# Locks para proteger estruturas compartilhadas
state_lock = threading.Lock()

# Utilitários de rede com delimitador \n
def send_json(sock, obj):
    try:
        sock.sendall((json.dumps(obj) + "\n").encode('utf-8'))
    except Exception as e:
        logging.debug(f"send_json failed: {e}")

def recv_json_from_buffer(sock, buffer):
    """ Recebe do socket até encontrar '\n'. Retorna (msg_dict|None, new_buffer). """
    try:
        while "\n" not in buffer:
            data = sock.recv(2048).decode('utf-8')
            if not data:
                return None, buffer
            buffer += data
        line, buffer = buffer.split("\n", 1)
        if not line.strip():
            return None, buffer
        return json.loads(line), buffer
    except json.JSONDecodeError:
        return None, buffer
    except socket.timeout:
        return None, buffer
    except Exception:
        return None, buffer

# Telemetria para supervisor (TLS)
def send_telemetry_loop():
    TCP_SOCKET_HOST = "nuted-ia.dev"
    TCP_SOCKET_PORT = 443
    TCP_SOCKET_SNI = "nuted-ia.dev"
    global tasks_completed_counter, tasks_failed_counter

    while True:
        time.sleep(10)
        try:
            uptime = int(time.time() - start_time)
            try:
                load_1m, load_5m, _ = psutil.getloadavg()
            except Exception:
                load_1m, load_5m = 0.0, 0.0
            cpu_pct = float(psutil.cpu_percent(interval=None))
            cpu_logical = psutil.cpu_count(logical=True) or 1
            cpu_physical = psutil.cpu_count(logical=False) or 1
            mem = psutil.virtual_memory()
            mem_total_mb = int(mem.total / (1024 * 1024))
            mem_avail_mb = int(mem.available / (1024 * 1024))
            mem_used_mb = int(mem.used / (1024 * 1024))
            mem_pct = float(mem.percent)
            try:
                disk = psutil.disk_usage('/')
                disk_total_gb = round(disk.total / (1024 * 1024 * 1024), 2)
                disk_free_gb = round(disk.free / (1024 * 1024 * 1024), 2)
                disk_pct = float(disk.percent)
            except Exception:
                disk_total_gb = disk_free_gb = disk_pct = 0.0

            with state_lock:
                total_reg = len(local_workers_sockets) + len(borrowed_workers)
                workers_busy = len(running_tasks)
                workers_idle = 1
                borrowed_list = []
                for w_id, orig in borrowed_workers.items():
                    borrowed_list.append({"direction": "in", "peer_uuid": orig})
                for w_id, dest in lent_workers.items():
                    borrowed_list.append({"direction": "out", "peer_uuid": dest})
                oldest_age = 0
                if not task_queue.empty():
                    try:
                        _, insert_time = task_queue.queue[0]
                        oldest_age = int(time.time() - insert_time)
                    except Exception:
                        oldest_age = 0

            telemetry_payload = {
                "server_uuid": config.get('master_uuid', "master_2_A.local"),
                "hostname": config.get('hostname', 'master_2_A.local'),
                "role": "master",
                "task": "performance_report",
                "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "message_id": str(uuid.uuid4()),
                "payload_version": "sprint4-monitor-v2",
                "performance": {
                    "system": {
                        "uptime_seconds": uptime,
                        "load_average_1m": load_1m,
                        "load_average_5m": load_5m,
                        "cpu": {
                            "usage_percent": cpu_pct,
                            "count_logical": cpu_logical,
                            "count_physical": cpu_physical
                        },
                        "memory": {
                            "total_mb": mem_total_mb,
                            "available_mb": mem_avail_mb,
                            "percent_used": mem_pct,
                            "memory_used": mem_used_mb
                        },
                        "disk": {
                            "total_gb": disk_total_gb,
                            "free_gb": disk_free_gb,
                            "percent_used": disk_pct
                        }
                    },
                    "farm_state": {
                        "workers": {
                            "total_registered": total_reg,
                            "workers_utilization": workers_busy,
                            "workers_alive": total_reg,
                            "workers_idle": workers_idle,
                            "workers_borrowed": len(lent_workers),
                            "workers_received": len(borrowed_workers),
                            "workers_failed": 0,
                            "workers_home": len(local_workers_sockets),
                            "workers_available_capacity": workers_idle,
                            "borrowed_workers": borrowed_list
                        },
                        "tasks": {
                            "tasks_pending": task_queue.qsize(),
                            "tasks_running": workers_busy,
                            "tasks_completed": tasks_completed_counter,
                            "tasks_failed": tasks_failed_counter,
                            "oldest_task_age_s": oldest_age
                        },
                        "config_thresholds": {
                            "max_task": config.get('threshold', 10),
                            "warn_cpu_percent": 85,
                            "warn_memory_percent": 85,
                            "release_task": config.get('threshold', 10) // 2
                        },
                        "neighbors": [
                            {"server_uuid": n, "status": "available", "last_heartbeat": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}
                            for n in config.get('neighbors', [])
                        ]
                    }
                }
            }

            json_data = (json.dumps(telemetry_payload) + "\n").encode('utf-8')
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_socket.settimeout(15.0)
            context = ssl.create_default_context()
            secure_socket = context.wrap_socket(raw_socket, server_hostname=TCP_SOCKET_SNI)
            secure_socket.connect((TCP_SOCKET_HOST, TCP_SOCKET_PORT))
            secure_socket.sendall(json_data)
            secure_socket.close()
            logging.info("[SUPERVISOR] Telemetria enviada com sucesso.")
        except Exception as e:
            logging.error(f"[SUPERVISOR] Erro ao enviar telemetria: {e}")

# Handler da conexão (Worker ou Master peer)
def handle_connection(client_socket, address):
    global tasks_completed_counter, tasks_failed_counter
    client_socket.settimeout(5.0)
    buffer = ""
    current_worker_attached = None

    try:
        while True:
            msg, buffer = recv_json_from_buffer(client_socket, buffer)
            if msg is None:
                # sem nova mensagem no momento (timeout) -> continuar a espera
                continue

            # Mensagens Master<->Master (type em minúsculas)
            if "type" in msg:
                msg_type = msg.get("type")
                req_id = msg.get("request_id")
                payload = msg.get("payload", {})

                if msg_type == "request_help":
                    handle_request_help(client_socket, req_id, payload)
                    # continue para manter conexão viva (podemos manter pool)
                    continue

                if msg_type == "response_accepted":
                    # se este servidor é quem pediu ajuda, aqui trataria resposta.
                    # no design atual, solicitante abriu conexão cliente e espera resposta de imediato.
                    logging.info(f"[P2P] Received response_accepted: {msg}")
                    continue

                if msg_type == "command_redirect":
                    # se receber redirect como Master (provavelmente não ocorre), log
                    logging.info("[P2P] command_redirect recebido por Master (ignorado).")
                    continue

                if msg_type == "notify_worker_returned":
                    worker_id = payload.get("worker_id")
                    with state_lock:
                        if worker_id in lent_workers:
                            del lent_workers[worker_id]
                    logging.info(f"[P2P] Notificação: worker {worker_id} retornou ao seu master.")
                    continue

                if msg_type == "register_temporary_worker":
                    w_id = payload.get("worker_id")
                    orig_master_addr = payload.get("original_master_address")
                    current_worker_attached = w_id
                    with state_lock:
                        borrowed_workers[w_id] = orig_master_addr
                        local_workers_sockets[w_id] = client_socket
                    logging.info(f"[P2P] Worker temporário registrado: {w_id} origem={orig_master_addr}")
                    continue

                if msg_type == "command_release":
                    # se master recebe command_release direcão ao worker; geralmente enviado master->worker
                    logging.info("[P2P] command_release recebido por Master (ignorado).")
                    continue

            # Mensagens Worker->Master (Sprints 1/2)
            if msg.get("TASK") == "HEARTBEAT":
                resp = {"SERVER_UUID": config['master_uuid'], "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                send_json(client_socket, resp)
                continue

            if msg.get("WORKER") == "ALIVE":
                w_uuid = msg.get("WORKER_UUID")
                current_worker_attached = w_uuid
                with state_lock:
                    if w_uuid not in local_workers_sockets:
                        local_workers_sockets[w_uuid] = client_socket
                logging.info(f"[FILA] Worker {w_uuid} se apresentou. tasks_pending={task_queue.qsize()}")

                # Devolução por limiar de liberação
                limiar_liberacao = config.get('threshold', 10) // 2
                is_borrowed = w_uuid in borrowed_workers
                if is_borrowed and task_queue.qsize() <= limiar_liberacao:
                    orig_master = borrowed_workers.get(w_uuid)
                    release_msg = {"type": "command_release", "request_id": str(uuid.uuid4()), "payload": {"original_master_address": orig_master}}
                    send_json(client_socket, release_msg)
                    # notificar o neighbor assincronamente
                    threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                    with state_lock:
                        if w_uuid in borrowed_workers: del borrowed_workers[w_uuid]
                        if w_uuid in local_workers_sockets: del local_workers_sockets[w_uuid]
                    logging.info(f"[DEVOLUÇÃO] Solicitada devolução de {w_uuid} para {orig_master}")
                    continue

                # Enviar tarefa
                if not task_queue.empty():
                    user_task, _ = task_queue.get()
                    with state_lock:
                        running_tasks[w_uuid] = user_task
                    response = {"TASK": "QUERY", "USER": user_task}
                else:
                    response = {"TASK": "NO_TASK"}
                send_json(client_socket, response)
                continue

            if msg.get("STATUS") and msg.get("TASK") == "QUERY":
                w_uuid = msg.get("WORKER_UUID")
                logging.info(f"[STATUS] Worker {w_uuid} concluiu processamento.")
                with state_lock:
                    if w_uuid in running_tasks:
                        del running_tasks[w_uuid]
                    tasks_completed_counter += 1
                ack = {"STATUS": "ACK", "WORKER_UUID": w_uuid}
                send_json(client_socket, ack)
                continue

    except Exception as e:
        logging.error(f"[CONN] Erro na conexão {address}: {e}")
        with state_lock:
            tasks_failed_counter += 1
    finally:
        if current_worker_attached:
            with state_lock:
                if current_worker_attached in running_tasks:
                    del running_tasks[current_worker_attached]
                if current_worker_attached in local_workers_sockets:
                    del local_workers_sockets[current_worker_attached]
        try:
            client_socket.close()
        except Exception:
            pass

def notify_neighbor_release(neighbor_address, worker_id):
    try:
        host, port = neighbor_address.split(':')
        with socket.create_connection((host, int(port)), timeout=5) as sock:
            msg = {"type": "notify_worker_returned", "request_id": str(uuid.uuid4()), "payload": {"worker_id": worker_id}}
            send_json(sock, msg)
            logging.info(f"[NOTIFICAÇÃO] Devolução {worker_id} notificada a {neighbor_address}")
    except Exception as e:
        logging.error(f"[NOTIFICAÇÃO] Falha ao notificar {neighbor_address}: {e}")

def handle_request_help(client_socket, req_id, payload):
    """
    Recebe request_help e decide aceitar/rejeitar.
    Resposta aceita inclui worker_details: [{ "id": id, "address": "ip:port" }, ...]
    Depois envia command_redirect para os workers selecionados.
    """
    master_id = payload.get("master_id")
    workers_needed = int(payload.get("workers_needed", 1))
    with state_lock:
        # escolher workers ociosos localmente (que têm socket e não estão em running_tasks)
        idle_candidates = [w for w in local_workers_sockets.keys() if w not in running_tasks and w not in lent_workers]
    if len(idle_candidates) >= 1:
        chosen = idle_candidates[:workers_needed]
        # montar worker_details; precisamos fornecer um address (o Worker já está conectado via socket,
        # mas para o protocolo podemos fornecer o endereço do master solicitante que o worker deverá conectar).
        # Aqui assumimos que o request payload incluiu o ip:port de destino (se não, usamos fallback).
        dest_address = payload.get("dest_master_address")
        if not dest_address:
            # fallback: assumir que client_socket peername é o master solicitante IP e porta padrão 5000
            try:
                peer_ip, _ = client_socket.getpeername()
                dest_address = f"{peer_ip}:{config.get('default_peer_port', 5000)}"
            except Exception:
                dest_address = f"{config.get('host')}:{config.get('port')}"
        # responder accepted
        response = {"type": "response_accepted", "request_id": req_id, "payload": {"workers_offered": len(chosen),
                                                                                   "worker_details": [{"id": w, "address": dest_address} for w in chosen]}}
        send_json(client_socket, response)
        # enviar command_redirect aos workers escolhidos
        for w in chosen:
            with state_lock:
                wsock = local_workers_sockets.get(w)
            if wsock:
                redirect = {"type": "command_redirect", "request_id": str(uuid.uuid4()), "payload": {"new_master_address": dest_address}}
                send_json(wsock, redirect)
                with state_lock:
                    if w in local_workers_sockets: del local_workers_sockets[w]
                    lent_workers[w] = dest_address
                logging.info(f"[P2P] Emprestado worker {w} para {dest_address}")
    else:
        response = {"type": "response_rejected", "request_id": req_id, "payload": {"reason": "no_workers_available"}}
        send_json(client_socket, response)

def check_load_and_negotiate_loop():
    while True:
        time.sleep(2)
        try:
            with state_lock:
                current_load = task_queue.qsize()
                already_borrowed = len(borrowed_workers) > 0
            if current_load > config.get('threshold', 10) and not already_borrowed:
                # quantos workers precisamos (simples)
                workers_needed = max(1, (current_load - config.get('threshold')) // 1)
                request_id = str(uuid.uuid4())
                request = {"type": "request_help", "request_id": request_id,
                           "payload": {"master_id": config.get('master_uuid'), "current_load": current_load,
                                       "capacity": config.get('threshold'), "workers_needed": workers_needed,
                                       "dest_master_address": f"{config.get('host')}:{config.get('port')}" }}
                # tentar todos os vizinhos até obter aceitação (com timeout)
                for neighbor in config.get('neighbors', []):
                    try:
                        host, port = neighbor.split(':')
                        with socket.create_connection((host, int(port)), timeout=5) as sock:
                            send_json(sock, request)
                            # esperar resposta até 5s (usamos buffer)
                            buff = ""
                            sock.settimeout(5.0)
                            start_wait = time.time()
                            while time.time() - start_wait < 5:
                                res, buff = recv_json_from_buffer(sock, buff)
                                if res is None:
                                    time.sleep(0.1)
                                    continue
                                if res.get("request_id") == request_id:
                                    if res.get("type") == "response_accepted":
                                        logging.info(f"[NEGOCIAÇÃO] Aceito por {neighbor}")
                                        # a resposta do neighbor inicia a redirect do lado dele (handled there)
                                        raise StopIteration  # sair do for
                                    else:
                                        logging.info(f"[NEGOCIAÇÃO] Recusado por {neighbor}: {res.get('payload')}")
                                        break
                            # se não obteve resposta, continua para próximo neighbor
                    except StopIteration:
                        break
                    except Exception as e:
                        logging.debug(f"[NEGOCIAÇÃO] Falha com {neighbor}: {e}")
        except Exception as e:
            logging.error(f"[NEGOCIAÇÃO] Erro na rotina de load check: {e}")

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((config['host'], config['port']))
    server.listen(50)
    logging.info(f"--- MASTER {config['master_uuid']} ONLINE em {config['host']}:{config['port']} ---")

    threading.Thread(target=check_load_and_negotiate_loop, daemon=True).start()
    threading.Thread(target=send_telemetry_loop, daemon=True).start()

    try:
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=handle_connection, args=(client_sock, addr), daemon=True).start()
    except KeyboardInterrupt:
        logging.info("Shutting down master.")
    finally:
        server.close()

if __name__ == "__main__":
    start_master()