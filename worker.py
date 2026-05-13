import socket
import json
import time
import random
import logging

# Carregar configuração
config = json.load(open('config.json'))

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_worker():
    worker_uuid = config['worker_uuid']
    original_master = config['original_master']
    current_master_host, current_master_port = config['host'], config['port']

    logging.info(f"[SISTEMA] Iniciando Worker {worker_uuid}...")

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((current_master_host, current_master_port))
                sock.settimeout(10.0)
                logging.info(f"[CONEXÃO] Conectado ao Master {current_master_host}:{current_master_port}.")

                while True:
                    # TAREFA 01: Handshake de Apresentação
                    payload = {
                        "WORKER": "ALIVE",
                        "WORKER_UUID": worker_uuid,
                        "SERVER_UUID": original_master if original_master != current_master_host else None
                    }
                    # Remove SERVER_UUID se None
                    if payload["SERVER_UUID"] is None:
                        del payload["SERVER_UUID"]
                    sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))

                    # TAREFA 02: Recebendo a Tarefa
                    data = sock.recv(1024).decode('utf-8')
                    if not data: break
                    response = json.loads(data.strip())

                    if response.get("TASK") == "QUERY":
                        user = response.get("USER")
                        logging.info(f"[TRABALHO] Processando usuário: {user}...")

                        # TAREFA 03: Simulação de Processamento
                        time.sleep(random.uniform(2, 4))

                        # Reportando o Status
                        report = {
                            "STATUS": "OK",
                            "TASK": "QUERY",
                            "WORKER_UUID": worker_uuid
                        }
                        sock.sendall((json.dumps(report) + "\n").encode('utf-8'))

                        # TAREFA 04: Esperando o ACK
                        ack_data = sock.recv(1024).decode('utf-8')
                        if ack_data and json.loads(ack_data).get("STATUS") == "ACK":
                            logging.info(f"[SUCESSO] Tarefa '{user}' confirmada pelo Master.")

                    elif response.get("TASK") == "NO_TASK":
                        logging.info("[FILA] Sem tarefas. Aguardando 10s...")
                        time.sleep(10)

                    elif response.get("TASK") == "SWITCH_MASTER":  # Novo: Redirecionamento
                        new_master = response.get("NEW_MASTER")
                        if new_master:
                            current_master_host, current_master_port = new_master.split(':')
                            current_master_port = int(current_master_port)
                            logging.info(f"[REDIRECIONAMENTO] Mudando para Master {new_master}")
                            break  # Sai do loop interno para reconectar

                    time.sleep(1)  # Pequena pausa

        except (socket.error, json.JSONDecodeError) as e:
            logging.error(f"[LOG] Status: OFFLINE - Reconectando em 5s... Erro: {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()