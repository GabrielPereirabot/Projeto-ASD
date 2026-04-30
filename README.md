Projeto ASD - Sistemas DistribuídosEste repositório contém a implementação de um sistema distribuído baseado na arquitetura Master-Worker com balanceamento de carga e protocolo P2P. O objetivo principal é coordenar tarefas entre múltiplos nós utilizando comunicação via Sockets TCP e mensagens padronizadas em JSON.  📋 Pré-requisitosPython 3.xBibliotecas nativas: socket, json, threading, queue🛰️ Sprint 1: Mecanismo de HeartbeatO objetivo desta sprint foi estabelecer a infraestrutura de rede e garantir a verificação de disponibilidade (saúde) entre o Worker e o Master.  Detalhes TécnicosProtocolo: TCP  Mensageria: JSON com delimitador \n ao final de cada objeto  Concorrência: Master implementado com threads para atendimento simultâneo  Payloads OficiaisRequisição (Worker → Master):JSON{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT"
}
Resposta (Master → Worker):JSON{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
🛠️ Como Executar1. Iniciar o MasterO Master atua como servidor e deve ser iniciado primeiro para abrir a porta de escuta.  Bashpython Master.py
2. Iniciar o WorkerO Worker atua como cliente e iniciará o ciclo de Heartbeat automaticamente ao conectar.  Bashpython worker.py
Nota: Certifique-se de que o IP e a Porta definidos nos scripts sejam os mesmos para que a conexão seja estabelecida corretamente.🔄 Sprint 2: Comunicação de TarefasNesta fase, implementamos a lógica de consumo de fila e o reporte de status das tarefas processadas.  Fluxo de TrabalhoApresentação: O Worker se conecta e envia seu WORKER_UUID.  Distribuição: O Master consome uma tarefa da sua queue interna e despacha para o Worker.  Processamento: O Worker simula a execução da tarefa (QUERY).  Confirmação: Após o reporte de status pelo Worker, o Master responde com um ACK para finalizar o ciclo.  Payloads de TarefaPedido: {"WORKER": "ALIVE", "WORKER_UUID": "W-123"}  Envio: {"TASK": "QUERY", "USER": "Nome_Usuario"}  Status: {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "W-123"}  ACK: {"STATUS": "ACK", "WORKER_UUID": "W-123"}
