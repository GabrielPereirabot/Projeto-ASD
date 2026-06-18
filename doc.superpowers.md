# doc.superpowers — Projeto ASD: P2P com Balanceamento de Carga Dinâmico

## Sumário Rápido

**Objetivo:** descrever o projeto, como executar, validar e evidenciar os resultados para avaliação.

**Escopo:** resumo do sistema Master–Worker com negociação P2P para empréstimo temporário de Workers.

---

# Objetivo do Documento

Este documento reúne as informações essenciais para avaliação do projeto: visão geral da arquitetura, instruções de execução, payloads do protocolo, cenários de teste e relatório de brainstorming contendo as alternativas avaliadas e a justificativa da solução adotada.

---

# Visão Geral do Projeto

O sistema implementa uma arquitetura distribuída Master–Worker com capacidade de negociação P2P entre Masters.

Cada Master possui uma farm de Workers. Quando a fila de tarefas excede um threshold configurado, o Master pode solicitar Workers emprestados a Masters vizinhos por meio de um protocolo JSON sobre TCP (newline-delimited).

O Worker reconecta-se temporariamente ao Master solicitante e, quando a carga normaliza, é liberado de volta ao seu Master original.

---

# Objetivos do Projeto

* O1: Master capaz de gerenciar Workers e negociar com peers.
* O2: Simular carga (fila) para testar comportamento.
* O3: Detectar saturação via threshold configurável.
* O4: Protocolo de conversa consensual Master↔Master (`request_help` / `response_accepted` / `response_rejected`).
* O5: Redirecionamento dinâmico de Workers (`command_redirect`, `register_temporary_worker`, `command_release`, `notify_worker_returned`).
* O6: Interoperabilidade através de comunicação baseada exclusivamente nos payloads JSON definidos.

---

# Arquivos e Estrutura Relevante

* `Master.py` — implementação do nó Master (servidor TCP, cliente P2P e telemetria).
* `worker.py` — implementação do Worker (cliente TCP que processa tarefas e aceita redirecionamentos).
* `config.json` — configurações gerais do sistema.
* `doc.superpowers.md` — documentação do projeto.
* `README.md` — documentação complementar do repositório.

---

# Como Executar (Windows PowerShell)

## 1. Acessar a pasta do projeto

```powershell
cd 'C:\Users\E008984\Desktop\ceub atividade\Projeto-ASD'
```

## 2. Criar e ativar ambiente virtual (opcional)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install psutil
```

## 3. Executar Master e Worker

Em terminais separados:

```powershell
python Master.py
```

```powershell
python worker.py
```

### Teste P2P entre dois Masters

Para simular múltiplos Masters localmente:

1. Copie a pasta do projeto.
2. Ajuste o `config.json` de cada instância para utilizar portas e identificadores diferentes.

Exemplo:

* Master A → `127.0.0.1:5000`
* Master B → `127.0.0.1:5001`

Inicie:

* Dois Masters
* Pelo menos um Worker conectado ao Master B

---

# Payloads e Protocolo

Todas as mensagens utilizam JSON delimitado por quebra de linha (`\n`).

## HEARTBEAT (Worker → Master)

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT"
}
```

## Resposta HEARTBEAT (Master → Worker)

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

## Apresentação do Worker (ALIVE)

```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-123",
  "SERVER_UUID": "Master_B"
}
```

## Master → Worker (Tarefa)

```json
{
  "TASK": "QUERY",
  "USER": "Michel"
}
```

## Master → Worker (Sem Tarefa)

```json
{
  "TASK": "NO_TASK"
}
```

## Worker → Master (Status)

```json
{
  "STATUS": "OK",
  "TASK": "QUERY",
  "WORKER_UUID": "W-123"
}
```

## Master → Worker (ACK)

```json
{
  "STATUS": "ACK",
  "WORKER_UUID": "W-123"
}
```

## Estrutura Genérica Master ↔ Master

```json
{
  "type": "request_help",
  "request_id": "uuid",
  "payload": {}
}
```

### Principais Tipos de Mensagem

* request_help
* response_accepted
* response_rejected
* command_redirect
* register_temporary_worker
* command_release
* notify_worker_returned

---

# Fluxo de Empréstimo de Workers

### 1. Detecção de Saturação

O Master A detecta que sua fila ultrapassou o threshold configurado e envia uma mensagem `request_help` ao Master B.

### 2. Aceitação

O Master B verifica sua disponibilidade e responde com `response_accepted`, selecionando Workers ociosos.

### 3. Redirecionamento

O Master B envia `command_redirect` aos Workers escolhidos.

### 4. Registro Temporário

Os Workers encerram sua conexão atual e reconectam ao Master A, registrando-se por meio de `register_temporary_worker`.

### 5. Operação Temporária

O Master A adiciona os Workers à estrutura de empréstimos e passa a utilizá-los normalmente.

### 6. Devolução

Quando a carga retorna ao normal:

* Master A envia `command_release`
* Master A envia `notify_worker_returned` ao Master B
* O Worker reconecta ao Master original

---

# Brainstorming

## Contexto

Problema identificado:

Reduzir filas saturadas sem dependências externas, mantendo interoperabilidade entre implementações distintas.

---

## Critérios de Avaliação

* Simplicidade de implementação
* Baixa dependência de infraestrutura externa
* Baixa latência de comunicação
* Robustez frente a falhas de rede

---

## Alternativas Consideradas

### Broker Central (RabbitMQ)

#### Vantagens

* Confiabilidade elevada
* Confirmação de entrega
* Roteamento simplificado

#### Desvantagens

* Necessidade de infraestrutura adicional
* Dependência externa
* Menor aderência ao modelo descentralizado

---

### HTTP/REST entre Masters

#### Vantagens

* Facilidade de depuração
* Uso de ferramentas amplamente conhecidas

#### Desvantagens

* Maior overhead de comunicação
* Necessidade de endpoints específicos
* Latência superior

---

### Sockets TCP com JSON Delimitado por Nova Linha (Solução Escolhida)

#### Vantagens

* Baixo overhead
* Sem dependências externas
* Conexões persistentes
* Controle total do protocolo
* Compatibilidade com os requisitos do projeto

#### Desvantagens

* Necessidade de tratamento manual de framing
* Implementação própria de reconexão e tolerância a falhas

---

## Justificativa da Escolha

A utilização de sockets TCP com mensagens JSON mostrou-se a alternativa mais adequada para os objetivos do projeto.

A solução atende aos requisitos de interoperabilidade, reduz dependências externas, simplifica o processo de implantação e fornece controle completo sobre o protocolo de comunicação distribuída.

---

# Plano de Mitigação de Riscos

### Falhas de Certificados TLS

* Registro detalhado em log.
* Utilização de fallback apenas em ambiente de desenvolvimento.

### Desconexão de Workers Durante Processamento

* Conclusão da tarefa atual antes da reconexão.
* Possibilidade futura de retorno automático da tarefa para fila.

---

# Cenários de Teste

## CT01 — Heartbeat

**Objetivo:** validar comunicação básica entre Worker e Master.

**Resultado esperado:** resposta `ALIVE`.

---

## CT02 — Apresentação

**Objetivo:** validar registro inicial do Worker.

**Resultado esperado:** recebimento de `QUERY` ou `NO_TASK`.

---

## CT03 — Processamento de Tarefa

**Objetivo:** validar ciclo completo de execução.

**Resultado esperado:**

* Worker envia `STATUS: OK`
* Master responde `ACK`

---

## CT04 — Negociação P2P

**Objetivo:** validar empréstimo temporário de Workers.

**Resultado esperado:**

* envio de `request_help`
* resposta `response_accepted`
* redirecionamento dos Workers
* registro temporário no Master solicitante

---

## CT05 — Devolução

**Objetivo:** validar retorno dos Workers ao Master original.

**Resultado esperado:**

* envio de `command_release`
* envio de `notify_worker_returned`
* reconexão ao Master de origem

---

# Evidências de Execução

## Procedimento

1. Iniciar duas instâncias de Master.
2. Configurar o relacionamento entre elas no `config.json`.
3. Iniciar um ou mais Workers conectados ao Master fornecedor.
4. Gerar carga suficiente para ultrapassar o threshold configurado.
5. Coletar os logs de negociação e transferência.

### Exemplos de Logs

```text
2026-06-18 12:00:00 - INFO - [NEGOCIAÇÃO] Aceito por 10.62.217.25:5000

2026-06-18 12:00:01 - INFO - [P2P] Emprestado worker Worker2_A.local para 10.62.217.28:5000

2026-06-18 12:00:02 - INFO - [EMPRÉSTIMO] Registrado temporariamente no master remoto.
```

---

# Observações de Segurança

* Utilizar certificados TLS válidos em ambientes de produção.
* Restringir acesso às portas utilizadas pelo sistema.
* Preferir testes locais utilizando `127.0.0.1` ou redes privadas.

---

# Melhorias Futuras

* Implementação de testes automatizados de unidade e integração.
* Pool de conexões persistentes entre Masters.
* Mecanismos avançados de recuperação após falhas.
* Reconciliação baseada em estado eventual.
* Controle configurável de fallback TLS por meio de parâmetros no `config.json`.

---

# Referências

## Arquivos Principais

* `Master.py`
* `worker.py`
* `config.json`
* `README.md`
* `skills-lock.json`

## Documentação do Protocolo

Todos os payloads, fluxos de comunicação e cenários de teste encontram-se descritos neste documento.

