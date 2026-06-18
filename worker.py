import socket
import json
import time
import random
import logging
import uuid

config = json.load(open('config.json'))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def recv_json_from_buffer(sock, buffer):
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

def send_json(sock, obj):
    try:
        sock.sendall((json.dumps(obj) + "\n").encode('utf-8'))
    except Exception as e:
        logging.debug(f"send_json failed: {e}")

def start_worker():
    worker_uuid = config['worker_uuid']
    orig_host, orig_port = config['host'], config['port']
    current_host, current_port = orig_host, orig_port

    logging.info(f"[SISTEMA] Iniciando Worker {worker_uuid}...")

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(15.0)
                sock.connect((current_host, current_port))
                logging.info(f"[CONEXÃO] Conectado ao Master {current_host}:{current_port}.")

                # Se estiver "emprestado", registra temporariamente
                is_borrowed = (current_host != orig_host or current_port != orig_port)
                if is_borrowed:
                    reg_msg = {"type": "register_temporary_worker", "request_id": str(uuid.uuid4()),
                               "payload": {"worker_id": worker_uuid, "original_master_address": f"{orig_host}:{orig_port}"}}
                    send_json(sock, reg_msg)
                    logging.info("[EMPRÉSTIMO] Registrado temporariamente no master remoto.")

                buffer = ""
                while True:
                    # apresentação (ALIVE)
                    payload_alive = {"WORKER": "ALIVE", "WORKER_UUID": worker_uuid}
                    if is_borrowed:
                        payload_alive["SERVER_UUID"] = config.get('original_master', config.get('master_uuid'))
                    send_json(sock, payload_alive)

                    msg, buffer = recv_json_from_buffer(sock, buffer)
                    if msg is None:
                        # sem mensagem ainda (timeout) -> reenviar heartbeat depois
                        time.sleep(1)
                        continue

                    # P2P commands (type minúscula)
                    if "type" in msg:
                        t = msg.get("type")
                        if t == "command_redirect":
                            new_addr = msg["payload"].get("new_master_address")
                            if new_addr:
                                nhost, nport = new_addr.split(':')
                                current_host = nhost
                                current_port = int(nport)
                                logging.info(f"[REDIRECIONAMENTO] Indo para {new_addr}")
                                break  # reconecta para novo master
                        elif t == "command_release":
                            logging.info("[DEVOLUÇÃO] Recebi comando de release. Voltando ao master original.")
                            current_host, current_port = orig_host, orig_port
                            is_borrowed = False
                            break

                    # Tarefas
                    elif msg.get("TASK") == "QUERY":
                        user = msg.get("USER")
                        logging.info(f"[TRABALHO] Processando {user}...")
                        time.sleep(random.uniform(1, 3))
                        report = {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": worker_uuid}
                        send_json(sock, report)
                        # aguardar ACK
                        ack, buffer = recv_json_from_buffer(sock, buffer)
                        if ack and ack.get("STATUS") == "ACK":
                            logging.info("[SUCESSO] ACK recebido.")
                    elif msg.get("TASK") == "NO_TASK":
                        time.sleep(2)
                    elif msg.get("TASK") == "HEARTBEAT":
                        # caso master envie heartbeat
                        pass

        except (socket.timeout, socket.error) as e:
            logging.error(f"[LOG] Desconectado: {e}")
            time.sleep(5)
            # ao reconectar, manter current_host/current_port (se foi redirect, worker tenta o novo)
        except Exception as e:
            logging.error(f"[ERRO] {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()