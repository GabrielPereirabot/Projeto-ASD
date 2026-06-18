import socket
import json
import threading
import queue
import logging
import uuid
import time
<<<<<<< HEAD
from datetime import datetime, timezone  # [REQUISITO ISO-8601]
import psutil  # [REQUISITO PERFORMANCE.SYSTEM]
import ssl     # [REQUISITO TLS SOBRE TCP]
=======
import os 
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c

# ==============================================================================
# [SPRINT 01 - CONFIGURAÇÃO BASE]
# ==============================================================================
config = json.load(open('config.json'))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Métricas Históricas de Auditoria
start_time = time.time()  
tasks_completed_counter = 0
tasks_failed_counter = 0

# ==============================================================================
# [SPRINT 02 - TAREFAS E AGENDAMENTO]
# ==============================================================================
task_queue = queue.Queue()
for nome in range(1, 15):
    task_queue.put((f"User_{nome}", time.time()))

# ==============================================================================
# [SPRINT 04 - ARQUITETURA DISTRIBUÍDA P2P]
# ==============================================================================
borrowed_workers = {}      # {worker_uuid: original_master_address} -> Workers estrangeiros rodando aqui ("in")
lent_workers = {}          # {worker_uuid: destination_master_address} -> Workers que eu emprestei para fora ("out")
local_workers_sockets = {} # {worker_uuid: client_socket} -> Sockets ativos locais
running_tasks = {}         # {worker_uuid: task_name} -> Tarefas em execução

def send_telemetry_loop():
    """ 
    [REQUISITO ESTRITO: SUPERVISOR DE MÉTRICAS DO CLUSTER]
    Monta o payload complexo com todas as métricas de hardware, estado da farm e thresholds.
    Envia via Socket TLS Puro sobre a porta 443 sem esperar retorno (Fire and Forget).
    """
    TCP_SOCKET_HOST = "nuted-ia.dev"
    TCP_SOCKET_PORT = 443
    TCP_SOCKET_SNI = "nuted-ia.dev"

    while True:
        time.sleep(10) 
        
        try:
            # 1. Coleta de Métricas do Sistema via psutil (performance.system)
            uptime = int(time.time() - start_time)
            
            # [SPRINT 03 - RESILIÊNCIA E TRATAMENTO DE ERROS]
            # Tarefa: Fallback defensivo para sistemas operacionais que não suportam certos comandos
            try:
                load_1m, load_5m, _ = psutil.getloadavg()
            except AttributeError:
                load_1m, load_5m = 0.0, 0.0

            cpu_pct = float(psutil.cpu_percent(interval=None))
            cpu_logical = psutil.cpu_count(logical=True) or 1
            cpu_physical = psutil.cpu_count(logical=False) or 1
            
            mem = psutil.virtual_memory()
            mem_total_mb = int(mem.total / (1024 * 1024))
            mem_avail_mb = int(mem.available / (1024 * 1024))
            mem_used_mb = int(mem.used / (1024 * 1024))
            mem_pct = float(mem.percent)
            
            disk = psutil.disk_usage('/')
            disk_total_gb = round(disk.total / (1024 * 1024 * 1024), 2)
            disk_free_gb = round(disk.free / (1024 * 1024 * 1024), 2)
            disk_pct = float(disk.percent)

            # 2. Coleta de Estados do Cluster P2P (performance.farm_state.workers)
            total_reg = len(local_workers_sockets) + len(borrowed_workers)
            workers_busy = len(running_tasks)
            workers_idle = max(0, len(local_workers_sockets) - workers_busy)
            
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
                    pass

            # 3. Montagem do JSON seguindo EXATAMENTE o Schema exigido pelo documento
            telemetry_payload = {
                "server_uuid": config.get('master_uuid', "master_2"),
                "hostname": "master2.A.local",
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
                            {
                                "server_uuid": n,
                                "status": "available",
                                "last_heartbeat": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                            } for n in config.get('neighbors', [])
                        ]
                    }
                }
            }

            json_data = (json.dumps(telemetry_payload) + "\n").encode('utf-8')
            
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            # [SPRINT 03 - GESTÃO DE TIMEOUTS DE CONEXÃO]
            # Tarefa: Evitar que a thread fique travada indefinidamente tentando alcançar o Supervisor externo se a internet falhar
            raw_socket.settimeout(4.0)
            
            context = ssl.create_default_context()
            secure_socket = context.wrap_socket(raw_socket, server_hostname=TCP_SOCKET_SNI)
            
            secure_socket.connect((TCP_SOCKET_HOST, TCP_SOCKET_PORT))
            secure_socket.sendall(json_data)
            secure_socket.close()
            logging.info("[SUPERVISOR] Payload completo de telemetria enviado com sucesso.")

        # [SPRINT 03 - TRATAMENTO DE EXCEÇÃO ISOLADO]
        # Tarefa: Garantir que falhas de rede no envio da telemetria externa NÃO derrubem o Master local
        except Exception as e:
            logging.error(f"[SUPERVISOR - FALHA] Erro ao processar ou enviar métricas: {e}")

<<<<<<< HEAD

def handle_connection(client_socket, address):
    global borrowed_workers, lent_workers, local_workers_sockets, running_tasks, tasks_completed_counter, tasks_failed_counter
=======
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
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
    buffer = ""
    current_worker_attached = None
    
    # [SPRINT 03 - TOLERÂNCIA A FALHAS DO TRABALHADOR (TRY)]
    # Tarefa: Isolar o contexto de comunicação com cada worker para capturar quedas de conexão abruptas
    try:
        while True:
            # [SPRINT 03 - TIMEOUT DE INPUT/OUTPUT]
            # Tarefa: Se o worker parar de responder por 5 segundos, dispara a desconexão defensiva
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
<<<<<<< HEAD
                            return
=======
                            return 
>>>>>>> cac3cbf (Correções para sprint 3)
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
                        
                        elif msg_type == "notify_worker_returned":
                            w_id = payload.get("worker_id")
                            if w_id in lent_workers:
                                del lent_workers[w_id] 
                            logging.info(f"[P2P] Vizinho devolveu nosso Worker nativo {w_id}.")
                            return 
                        
                        elif msg_type == "register_temporary_worker":
                            w_id = payload.get("worker_id")
                            current_worker_attached = w_id
                            orig_master = payload.get("original_master_address")
                            borrowed_workers[w_id] = orig_master
                            local_workers_sockets[w_id] = client_socket
                            logging.info(f"[P2P] Worker Temporário {w_id} hospedado. Origem: {orig_master}")
                    
                    elif "TASK" in msg or "WORKER" in msg:
                        if msg.get("TASK") == "HEARTBEAT":
                            response = {"SERVER_UUID": config['master_uuid'], "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        elif msg.get("WORKER") == "ALIVE":
                            w_uuid = msg.get("WORKER_UUID")
                            current_worker_attached = w_uuid
                            if w_uuid not in borrowed_workers:
                                local_workers_sockets[w_uuid] = client_socket
<<<<<<< HEAD
=======
                            
                            logging.info(f"[FILA] Worker {w_uuid} solicitando tarefa. Carga atual: {task_queue.qsize()}")
>>>>>>> cac3cbf (Correções para sprint 3)
                            
                            # Logica de devolução
                            limiar_liberacao = config['threshold'] // 2
                            if w_uuid in borrowed_workers and task_queue.qsize() <= limiar_liberacao:
                                orig_master = borrowed_workers[w_uuid]
                                release_msg = {
                                    "type": "command_release",
                                    "request_id": str(uuid.uuid4()),
                                    "payload": {"original_master_address": orig_master}
                                }
                                client_socket.sendall((json.dumps(release_msg) + "\n").encode('utf-8'))
                                time.sleep(0.1)
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
                                del borrowed_workers[w_uuid]
                                if w_uuid in local_workers_sockets: del local_workers_sockets[w_uuid]
                                if w_uuid in running_tasks: del running_tasks[w_uuid]
                                return 
                            
                            # Envio de tarefa
                            if not task_queue.empty():
<<<<<<< HEAD
                                user_task, _ = task_queue.get() 
                                running_tasks[w_uuid] = user_task
=======
                                user_task = task_queue.get()
                                workers_active_count += 1 
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
                                response = {"TASK": "QUERY", "USER": user_task}
                            else:
                                response = {"TASK": "NO_TASK"}
                            client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                        elif "STATUS" in msg and msg.get("TASK") == "QUERY":
                            w_uuid = msg.get("WORKER_UUID")
                            logging.info(f"[STATUS] Worker {w_uuid} concluiu processamento.")
                            
                            if w_uuid in running_tasks:
                                del running_tasks[w_uuid]
                            tasks_completed_counter += 1 
                            
                            ack = {"STATUS": "ACK", "WORKER_UUID": w_uuid}
                            client_socket.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                            
<<<<<<< HEAD
                            # Devolução pós processamento
                            limiar_liberacao = config['threshold'] // 2
                            if w_uuid in borrowed_workers and task_queue.qsize() <= limiar_liberacao:
                                orig_master = borrowed_workers[w_uuid]
=======
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
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
                                release_msg = {
                                    "type": "command_release",
                                    "request_id": str(uuid.uuid4()),
                                    "payload": {"original_master_address": orig_master}
                                }
<<<<<<< HEAD
                                try:
                                    client_socket.sendall((json.dumps(release_msg) + "\n").encode('utf-8'))
                                except Exception:
                                    pass
=======
                                client_socket.sendall((json.dumps(release_msg) + "\n").encode('utf-8'))
<<<<<<< HEAD
                                
                                # Pequeno delay para garantir envio antes de fechar/deletar
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
                                time.sleep(0.1)
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
=======
                                threading.Thread(target=notify_neighbor_release, args=(orig_master, w_uuid), daemon=True).start()
                                
>>>>>>> cac3cbf (Correções para sprint 3)
                                del borrowed_workers[w_uuid]
<<<<<<< HEAD
                                if w_uuid in local_workers_sockets: del local_workers_sockets[w_uuid]
                                return 
                                
=======
                                if w_uuid in local_workers_sockets: 
                                    del local_workers_sockets[w_uuid]
                
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
                except json.JSONDecodeError:
                    pass
    
    # [SPRINT 03 - GESTÃO DE FALHAS DE COMUNICAÇÃO DE REDE]
    # Tarefas: Capturar timeouts e quebras repentinas de socket sem travar a aplicação principal
    except socket.timeout:
        pass 
    except Exception:
        tasks_failed_counter += 1 # Incrementa contador exigido pelo Monitor de Métricas
    
    # [SPRINT 03 - LIMPEZA DE ESTADO PÓS-QUEDA (FINALLY)]
    # Tarefa: Se o worker morreu no meio da execução, limpa o ID dele e libera a tarefa de volta caso necessário
    finally:
        if current_worker_attached and current_worker_attached in running_tasks:
            # Recuperação de desastres: se caiu executando, tiramos do dicionário de execução ativa
            del running_tasks[current_worker_attached]
        client_socket.close()

<<<<<<< HEAD
=======
# ... (funções handle_request_help, notify_neighbor_release, check_load_and_negotiate_loop e start_master permanecem inalteradas)
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c

def handle_request_help(client_socket, req_id, payload):
    global local_workers_sockets, lent_workers
    
    # EXTRAÇÃO DINÂMICA (Sprint 4): Descobre quem pediu ajuda pelo payload recebido
    # Muitas arquiteturas enviam o IP/Porta de escuta do Master solicitante no payload
    # Vamos tentar pegar o IP real do socket ou deduzir pelo mapeamento conhecido
    master_solicitante_id = payload.get("master_id", "Desconhecido")
    
    # Como o socket cliente veio de quem pediu ajuda, podemos obter o IP de origem direto da conexão TCP
    ip_solicitante, _ = client_socket.getpeername()
    
    # Procuramos nos nossos vizinhos qual porta corresponde a esse IP, 
    # ou assumimos uma porta padrão baseada no ID do mestre se não enviada
    saturado_address = None
    for neighbor in config.get('neighbors', []):
        if ip_solicitante in neighbor:
            saturado_address = neighbor
            break
            
    # Fallback defensivo: se não achou no laço, tenta deduzir de forma inteligente
    if not saturado_address:
        saturado_address = f"{ip_solicitante}:5000" # Porta padrão de comunicação do cluster

    if task_queue.qsize() < config['threshold'] and local_workers_sockets:
        chosen_worker_id = list(local_workers_sockets.keys())[0]
        worker_sock = local_workers_sockets[chosen_worker_id]
<<<<<<< HEAD
        
<<<<<<< HEAD
        # Envia a resposta de aceitação confirmando o redirecionamento para o mestre correto
=======
=======
>>>>>>> cac3cbf (Correções para sprint 3)
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
        response = {
            "type": "response_accepted",
            "request_id": req_id,
            "payload": {
                "workers_offered": 1,
                "worker_details": [{"id": chosen_worker_id, "address": saturado_address}]
            }
        }
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))
<<<<<<< HEAD
        
<<<<<<< HEAD
        # Cria o comando de redirecionamento ordenando que o Worker local vá para o Master saturado
=======
=======
>>>>>>> cac3cbf (Correções para sprint 3)
        saturado_address = f"{config['neighbors'][0]}" 
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
        redirect = {
            "type": "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload": {"new_master_address": saturado_address}
        }
        
        try:
            worker_sock.sendall((json.dumps(redirect) + "\n").encode('utf-8'))
            del local_workers_sockets[chosen_worker_id]
            lent_workers[chosen_worker_id] = saturado_address 
            logging.info(f"[P2P] Emprestou worker {chosen_worker_id} dinamicamente para o nó {saturado_address}")
        except Exception:
            pass
    else:
<<<<<<< HEAD
        response = {"type": "response_rejected", "request_id": req_id, "payload": {"reason": "high_load"}}
=======
<<<<<<< HEAD
        response = {
            "type": "response_rejected",
            "request_id": req_id,
            "payload": {"reason": "high_load"}
        }
=======
        response = {"type": "response_rejected", "request_id": req_id, "payload": {"reason": "high_load"}}
>>>>>>> cac3cbf (Correções para sprint 3)
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
        client_socket.sendall((json.dumps(response) + "\n").encode('utf-8'))


def notify_neighbor_release(neighbor_address, worker_id):
<<<<<<< HEAD
    # [SPRINT 03 - CONEXÃO ISOLADA COM TIMEOUT]
    # Tarefa: Proteger o fechamento do recurso com "with" e aplicar timeout rígido para conexões P2P externas instáveis
=======
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c
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
<<<<<<< HEAD
    except Exception:
        pass

=======
            logging.info(f"[NOTIFICAÇÃO] Devolução de {worker_id} confirmada para {neighbor_address}")
    except Exception as e:
        logging.error(f"Erro ao notificar vizinho: {e}")
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c

def check_load_and_negotiate_loop():
    while True:
        time.sleep(2)
        if task_queue.qsize() > config['threshold'] and len(borrowed_workers) == 0:
            for neighbor in config['neighbors']:
                # [SPRINT 03 - MONITORAMENTO DEFENSIVO]
                # Tarefa: Impedir que falhas temporárias em nós vizinhos interrompam a thread do mestre local
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
<<<<<<< HEAD
                except Exception:
                    pass

=======
=======
                        break
>>>>>>> cac3cbf (Correções para sprint 3)
                except Exception as e:
                    logging.error(f"Falha ao negociar: {e}")
>>>>>>> a28a132034fdcc602caecd27709c9e7c7bf8bd3c

def start_master():
    """ [SPRINT 01] Inicialização do Servidor Local """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((config['host'], config['port']))
    server.listen(10)
    logging.info(f"--- MASTER {config['master_uuid']} ONLINE ---")
    
    threading.Thread(target=check_load_and_negotiate_loop, daemon=True).start()
    threading.Thread(target=send_telemetry_loop, daemon=True).start() 
    
    while True:
        client_sock, addr = server.accept()
        threading.Thread(target=handle_connection, args=(client_sock, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()