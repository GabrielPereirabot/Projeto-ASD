# Projeto ASD - Sistemas Distribuídos

### Sistema P2P com Balanceamento Dinâmico de Carga

## 📋 Resumo

Implementação de um sistema distribuído **Master–Worker** que demonstra **balanceamento horizontal dinâmico** via protocolo **P2P entre Masters**.

Cada Master gerencia uma farm de Workers, monitora sua fila de tarefas e, quando saturado, negocia com Masters vizinhos para receber Workers temporariamente emprestados.

A comunicação ocorre via **sockets TCP** utilizando mensagens **JSON newline-delimited (`\n`)**.

---

# 📚 Sumário

* [Requisitos](#-requisitos-mínimos)
* [Instalação](#-instalação)
* [Git Ignore](#-ignorar-virtualenv-no-git)
* [Estrutura do Projeto](#-estrutura-de-arquivos-principais)
* [Configuração](#-explicação-do-configjson)
* [Protocolo de Comunicação](#-regras-de-protocolo)
* [Sprint 1 — Heartbeat](#-sprint-1--heartbeat)
* [Sprint 2 — Ciclo de Tarefas](#-sprint-2--apresentação--ciclo-de-tarefas)
* [Sprint 3 — P2P e Redirecionamento](#-sprint-3--negociação-mastermaster-p2p-e-redirecionamento)
* [Sprint 4 — Telemetria](#-sprint-4--telemetria-e-dashboard)
* [Evidências para Entrega](#-evidências-para-entrega)

---

# 🔧 Requisitos Mínimos

* Python 3.8+

  * Recomendado: Python 3.9 ou superior
* Biblioteca:

```bash
psutil
```

Recomenda-se utilizar ambiente virtual (`venv`).

---

# 🚀 Instalação

## 1. Entrar na pasta do projeto

```powershell
cd "C:\Users\Cliente\Desktop\Sistemas Distribuidos\Projeto-ASD"
```

## 2. Criar e ativar ambiente virtual

```powershell
python -m venv .venv

.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip

pip install psutil
```

## 3. Gerar requirements.txt (opcional)

```powershell
pip freeze > requirements.txt
```

Conteúdo mínimo recomendado:

```txt
psutil==7.2.2
```

---

# 📂 Ignorar Virtualenv no Git

Adicione ao arquivo `.gitignore`:

```gitignore
.venv/
__pycache__/
*.pyc
*.log
```

---

# 📁 Estrutura de Arquivos Principais

```text
Projeto-ASD/
│
├── Master.py
├── worker.py
├── config.json
├── requirements.txt
└── README.md
```

| Arquivo       | Descrição                                       |
| ------------- | ----------------------------------------------- |
| `Master.py`   | Nó Master (Servidor + Cliente P2P + Telemetria) |
| `worker.py`   | Nó Worker responsável pelo processamento        |
| `config.json` | Arquivo de configuração do sistema              |

---

# ⚙️ Explicação do `config.json`

Exemplo:

```json
{
  "host": "127.0.0.1",
  "port": 5000,
  "master_uuid": "michel_1",
  "original_master": "michel_1",
  "threshold": 3,
  "neighbors": ["127.0.0.1:5001"],
  "worker_uuid": "Worker_A1",
  "hostname": "master-a.local",
  "default_peer_port": 5000
}
```

## Significado dos Campos

| Campo               | Descrição                                    |
| ------------------- | -------------------------------------------- |
| `host`              | IP onde o Master escuta conexões             |
| `port`              | Porta TCP utilizada pelo Master              |
| `master_uuid`       | Identificador único do Master                |
| `original_master`   | Master de origem do Worker                   |
| `threshold`         | Limite para solicitar ajuda a outros Masters |
| `neighbors`         | Lista de Masters vizinhos                    |
| `worker_uuid`       | Identificador único do Worker                |
| `hostname`          | Nome amigável para dashboard                 |
| `default_peer_port` | Porta padrão para peers                      |

---

# 📡 Regras de Protocolo

## Delimitação

Todas as mensagens JSON terminam com:

```text
\n
```

---

## Worker ↔ Master

Campos de controle devem estar em **MAIÚSCULAS**:

```text
WORKER
TASK
STATUS
ACK
ALIVE
QUERY
NO_TASK
OK
NOK
```

---

## Master ↔ Master (P2P)

Campo:

```json
"type"
```

Valores em **minúsculas**:

```text
request_help
response_accepted
response_rejected
command_redirect
register_temporary_worker
command_release
notify_worker_returned
```

---

## Timeout

```text
5 segundos
```

para negociações P2P.

---

# ✅ Sprint 1 — Heartbeat

## Objetivo

Permitir que o Worker verifique se o Master está ativo.

### Worker → Master

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT"
}
```

### Master → Worker

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

## Teste

```powershell
python Master.py
```

```powershell
python worker.py
```

### Definition of Done

* Worker conecta ao Master
* Envia HEARTBEAT
* Recebe resposta `ALIVE`

---

# ✅ Sprint 2 — Apresentação / Ciclo de Tarefas

## Objetivo

Implementar handshake, distribuição de tarefas e confirmação de processamento.

### Apresentação do Worker

```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-123",
  "SERVER_UUID": "Master_B"
}
```

### Master → Worker

Com tarefa:

```json
{
  "TASK": "QUERY",
  "USER": "Michel"
}
```

Sem tarefa:

```json
{
  "TASK": "NO_TASK"
}
```

### Worker → Master

```json
{
  "STATUS": "OK",
  "TASK": "QUERY",
  "WORKER_UUID": "W-123"
}
```

### Master → Worker

```json
{
  "STATUS": "ACK",
  "WORKER_UUID": "W-123"
}
```

### Definition of Done

* Handshake com UUID
* QUERY e NO_TASK funcionando
* STATUS retornado corretamente
* ACK enviado pelo Master

---

# ✅ Sprint 3 — Negociação Master↔Master (P2P) e Redirecionamento

## Fluxo Completo

```text
Master A
   │
   ├── request_help
   ▼
Master B
   │
   ├── response_accepted
   │
   ├── command_redirect
   ▼
Worker B1
   │
   ├── reconnect
   ├── register_temporary_worker
   ▼
Master A
   │
   ├── processamento
   │
   ├── command_release
   └── notify_worker_returned
```

### Tipos de Mensagens

```text
request_help
response_accepted
response_rejected
command_redirect
register_temporary_worker
command_release
notify_worker_returned
```

## Teste Local

### Criar segunda instância

```powershell
cd "C:\Users\Cliente\Desktop\Sistemas Distribuidos"

Copy-Item -Recurse -Force "Projeto-ASD" "Projeto-ASD-master2"
```

### Configuração

#### Master A

```json
{
  "port": 5000,
  "threshold": 1,
  "neighbors": ["127.0.0.1:5001"],
  "master_uuid": "michel_1"
}
```

#### Master B

```json
{
  "port": 5001,
  "threshold": 10,
  "neighbors": ["127.0.0.1:5000"],
  "master_uuid": "michel_2"
}
```

### Executar

Terminal 1:

```powershell
python Master.py
```

Terminal 2:

```powershell
python Master.py
```

Terminal 3:

```powershell
python worker.py
```

### Definition of Done

Fluxo completo registrado nos logs:

```text
request_help
→ response_accepted
→ command_redirect
→ register_temporary_worker
→ command_release
→ notify_worker_returned
```

---

# ✅ Sprint 4 — Telemetria e Dashboard

## Objetivo

Enviar métricas periódicas ao Supervisor.

### Configuração

| Campo     | Valor        |
| --------- | ------------ |
| Host      | nuted-ia.dev |
| Porta     | 443          |
| TLS       | Sim          |
| Intervalo | 10 segundos  |

---

### Exemplo de Payload

```json
{
  "server_uuid": "michel_1",
  "hostname": "michel_1.farm.local",
  "role": "master",
  "task": "performance_report",
  "timestamp": "2026-06-08T12:34:56Z",
  "message_id": "<uuid>",
  "payload_version": "sprint4-monitor-v2"
}
```

---

## Testes

### Verificar conectividade

```powershell
Test-NetConnection -ComputerName nuted-ia.dev -Port 443
```

### Executar Master

```powershell
python Master.py
```

Log esperado:

```text
[SUPERVISOR] Telemetria enviada com sucesso.
```

Dashboard:

```text
https://nuted-ia.dev/supervisor/dashboard/
```

---

# 📸 Evidências para Entrega

Incluir:

* README.md
* requirements.txt
* .gitignore
* Prints dos Masters e Workers
* Logs de negociação P2P
* Captura do Dashboard
* Relatório dos testes executados

---

# 🛠️ Comandos Úteis

## Ativar ambiente virtual

```powershell
.\.venv\Scripts\Activate.ps1
```

## Executar Master

```powershell
python Master.py
```

## Executar Worker

```powershell
python worker.py
```

## Gerar requirements

```powershell
pip freeze > requirements.txt
```

---

# 📌 Boas Práticas

* Não enviar `.venv` ao repositório.
* Ajustar os IDs exigidos pelo professor antes da apresentação.
* Registrar logs com timestamps.
* Remover logs de DEBUG antes da entrega final.
* Validar comunicação P2P antes da demonstração.

---

## 👨‍💻 Autor

Projeto desenvolvido para a disciplina de **Sistemas Distribuídos (ASD)**, demonstrando:

* Arquitetura Master–Worker
* Comunicação TCP com JSON
* Balanceamento horizontal dinâmico
* Negociação P2P entre Masters
* Telemetria e monitoramento distribuído
