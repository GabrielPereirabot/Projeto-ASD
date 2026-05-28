import socket
import json
import time
import random
import logging

# Carregar configuração
config = json.load(open('config.json'))

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_message_from_buffer(sock, buffer):
    """ Função auxiliar para garantir a leitura correta de mensagens delimitadas por \\n """
    while "\n" not in buffer:
        data = sock.recv(2048).decode('utf-8')
        if not data:
            return None, buffer
        buffer += data
    line, buffer = buffer.split("\n", 1)
    return json.loads(line), buffer

def start_worker():
    worker_uuid = config['worker_uuid']
    original_master_uuid = config['original_master']
    
    # Endereço original de casa (Master A)
    orig_host, orig_port = config['host'], config['port']
    
    # Endereço atual (Pode mudar caso seja emprestado)
    current_host, current_port = orig_host, orig_port

    logging.info(f"[SISTEMA] Iniciando Worker {worker_uuid}...")

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((current_host, current_port))
                sock.settimeout(15.0)
                logging.info(f"[CONEXÃO] Conectado ao Master {current_host}:{current_port}.")

                # Verifica se está "emprestado" (Tarefa 04 - Item 19/20)
                is_borrowed = (current_host != orig_host or current_port != orig_port)
                
                if is_borrowed:
                    # Tarefa 04 (Item 18): Enviar register_temporary_worker (minúsculas)
                    reg_msg = {
                        "type": "register_temporary_worker",
                        "payload": {
                            "worker_id": worker_uuid,
                            "original_master_address": f"{orig_host}:{orig_port}"
                        }
                    }
                    sock.sendall((json.dumps(reg_msg) + "\n").encode('utf-8'))
                    logging.info(f"[EMPRÉSTIMO] Registrado temporariamente no Master {current_host}:{current_port}.")

                buffer = ""

                while True:
                    # TAREFA 01: Handshake de Apresentação (MAIÚSCULAS - Sprint 02)
                    payload_alive = {
                        "WORKER": "ALIVE",
                        "WORKER_UUID": worker_uuid
                    }
                    # Tarefa 04 (Item 20): Adicionar SERVER_UUID se estiver emprestado
                    if is_borrowed:
                        payload_alive["SERVER_UUID"] = original_master_uuid
                    
                    sock.sendall((json.dumps(payload_alive) + "\n").encode('utf-8'))

                    # Aguarda a resposta (Pode ser uma TAREFA ou um COMANDO P2P)
                    msg, buffer = read_message_from_buffer(sock, buffer)
                    if not msg: 
                        break # Desconexão

                    # ---------------------------------------------------------
                    # TRIAGEM SPRINT 3 (COMANDOS P2P - LETRAS MINÚSCULAS)
                    # ---------------------------------------------------------
                    if "type" in msg:
                        # Tarefa 04 (Item 18): command_redirect
                        if msg["type"] == "command_redirect":
                            new_addr = msg["payload"].get("new_master_address")
                            current_host, current_port = new_addr.split(':')
                            current_port = int(current_port)
                            logging.info(f"[REDIRECIONAMENTO] Encerrando conexão atual graciosamente. Partindo para: {new_addr}")
                            break # Quebra o laço interno para reconectar no novo endereço
                        
                        # Tarefa 05 (Item 22): command_release
                        elif msg["type"] == "command_release":
                            logging.info("[DEVOLUÇÃO] Fila do Master aliviada. Retornando ao Master original.")
                            current_host, current_port = orig_host, orig_port
                            break # Quebra o laço interno para reconectar em casa

                    # ---------------------------------------------------------
                    # TRIAGEM SPRINT 2 (TAREFAS WORKER - LETRAS MAIÚSCULAS)
                    # ---------------------------------------------------------
                    elif msg.get("TASK") == "QUERY":
                        user = msg.get("USER")
                        logging.info(f"[TRABALHO] Processando usuário: {user}...")

                        # Simulação de Processamento
                        time.sleep(random.uniform(1, 3))

                        # Reportando o Status (MAIÚSCULAS)
                        report = {
                            "STATUS": "OK",
                            "TASK": "QUERY",
                            "WORKER_UUID": worker_uuid
                        }
                        sock.sendall((json.dumps(report) + "\n").encode('utf-8'))

                        # Tarefa 04: Esperando o ACK
                        ack_msg, buffer = read_message_from_buffer(sock, buffer)
                        if ack_msg and ack_msg.get("STATUS") == "ACK":
                            logging.info(f"[SUCESSO] Tarefa '{user}' confirmada pelo Master.")
                    
                    elif msg.get("TASK") == "NO_TASK":
                        # Pequena pausa se não houver tarefas, para não flodar a rede
                        time.sleep(2)

        except (socket.error, socket.timeout) as e:
            logging.error(f"[LOG] Desconectado. Motivo: {e}")
            # Tarefa 06 (Item 26): Resiliência. Se cair no Master B, tenta voltar para o B.
            # Se for falha permanente, você pode adicionar uma lógica para forçar a volta ao Master A.
            logging.info("[LOG] Reconectando em 5 segundos...")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()