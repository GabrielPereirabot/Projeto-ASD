# Projeto-ASD
Documentação Técnica: Mecanismo de Heartbeat
Sprint 1: Implementação do Mecanismo de Heartbeat
1. Objetivo da Sprint
O objetivo primordial desta etapa é estabelecer a comunicação fundamental entre o Worker (Cliente) e o Master (Servidor). O foco está em garantir que o Worker consiga verificar de forma persistente se o seu Master está ativo através de trocas de mensagens JSON via protocolo TCP.
2. Infraestrutura de Comunicação
A base do sistema utiliza sockets TCP para garantir a entrega confiável das mensagens.
Configuração do Master: Atua como servidor, escutando em uma porta pré-definida por novas conexões.
Configuração do Worker: Atua como cliente, iniciando a conexão com o endereço do Master.
Delimitador de Mensagem: Para a correta identificação dos objetos no stream TCP, utiliza-se obrigatoriamente o caractere de nova linha (\n) ao final de cada objeto JSON.
Concorrência: O Master utiliza múltiplas threads (ou AsyncIO) para que o processamento de um Heartbeat não bloqueie outras operações simultâneas.

3. Protocolo Heartbeat (Payloads Oficiais)
A verificação de atividade segue o padrão de pergunta e resposta definido para o projeto.
3.1. Requisição (Worker → Master)
O Worker dispara o payload para verificar a disponibilidade do servidor.
JSON
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT"
}

3.2. Resposta (Master → Worker)
O Master interpreta a tarefa e devolve a confirmação imediata de atividade.
JSON
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}


4. Processos e Tarefas Realizadas
Para o cumprimento desta Sprint, foram executadas as seguintes subtarefas:
Desenvolvimento da Lógica de Requisição: Implementação da função no Worker para disparar o payload de verificação em intervalos regulares (ex: a cada 10 a 30 segundos).
Lógica de Resposta no Master: Programação do servidor para realizar o parsing do JSON, validar a chave "TASK" como "HEARTBEAT" e retornar o status "ALIVE".
Resiliência e Loop: Criação de um mecanismo de tratamento de exceções no Worker para identificar falhas de rede ou Master offline, disparando logs de tentativa de reconexão.

5. Definição de "Pronto" (DoD)
A Sprint 1 é considerada concluída ao atingir os seguintes critérios de sucesso:
Conectividade: O Worker abre a conexão TCP com sucesso.
Parsing de Dados: O Master recebe e processa o JSON sem erros de sintaxe.
Feedback Visual: O Worker recebe a confirmação e imprime "Status: ALIVE" no log de console.
Estabilidade: O sistema mantém o ciclo de verificações sem travar os processos principais ou vazar memória.


Como Executar a Sprint 1 (Mecanismo de Heartbeat)
Esta sprint foca na infraestrutura de comunicação base e na verificação de atividade entre o Worker e o Master via TCP.  

Pré-requisitos
Python 3.x instalado.

Bibliotecas padrão socket, json e threading.

1. Iniciar o Nó Master (Servidor)
O Master deve ser iniciado primeiro para que possa escutar as conexões recebidas. No terminal, navegue até a pasta do Master e execute:  

Bash
python Master.py
O que acontece: O Master abrirá um socket TCP e ficará aguardando por mensagens de Heartbeat no padrão JSON com delimitador \n.  

2. Iniciar o Nó Worker (Cliente)
Com o Master rodando, abra um novo terminal, navegue até a pasta do Worker e execute:

Bash
python worker.py
O que acontece: O Worker iniciará uma conexão TCP com o Master e enviará automaticamente o payload de verificação.  

3. Resultados Esperados (Logs)
Se a comunicação estiver correta, você verá as seguintes interações nos terminais:

No terminal do Worker:

Mensagem indicando o envio do Heartbeat: {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT"}.  

Recebimento da confirmação: Status: ALIVE.  

No terminal do Master:

Log de conexão recebida do Worker.

Resposta enviada: {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}.  

