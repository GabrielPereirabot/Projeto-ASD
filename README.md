Aqui está o conteúdo completo do `README.md` em formato Markdown puro, pronto para copiar e colar diretamente no seu arquivo. Use um editor de texto ou GitHub para visualizar a formatação.

```
# Projeto ASD - Sistemas Distribuídos

Este repositório contém a implementação de um sistema distribuído baseado na arquitetura **Master-Worker** com **balanceamento de carga dinâmico** e protocolo **P2P (Peer-to-Peer)**. O objetivo é coordenar tarefas entre múltiplos nós utilizando comunicação via **Sockets TCP** e mensagens padronizadas em **JSON**, garantindo autonomia e interoperabilidade entre sistemas de diferentes equipes.

## 📋 Pré-requisitos

- **Python 3.x**
- Bibliotecas nativas (já incluídas no Python padrão):
  - `socket`
  - `json`
  - `threading`
  - `queue`
  - `logging` (para logs detalhados)

## 🏗️ Arquitetura Geral

- **Nó Master**: Gerencia uma fila de tarefas, distribui trabalho para Workers locais, monitora carga e negocia empréstimo de Workers com Masters vizinhos quando saturado.
- **Nó Worker**: Processa tarefas recebidas, simula execução e reporta status. Pode ser redirecionado dinamicamente para outro Master.
- **Comunicação**: TCP com JSON delimitado por `\n` ao final de cada mensagem. Protocolo consensual para negociação P2P.

## 🛰️ Sprint 1: Mecanismo de Heartbeat

Estabelece a infraestrutura de rede e verifica a disponibilidade (saúde) entre Worker e Master.

### Detalhes Técnicos
- **Protocolo**: TCP
- **Mensageria**: JSON com delimitador `\n`
- **Concorrência**: Master usa threads para atendimento simultâneo

### Payloads Oficiais
- **Requisição (Worker → Master)**:
  ```json
  {
    "SERVER_UUID": "Master_A",
    "TASK": "HEARTBEAT"
  }
  ```
- **Resposta (Master → Worker)**:
  ```json
  {
    "SERVER_UUID": "Master_A",
    "TASK": "HEARTBEAT",
    "RESPONSE": "ALIVE"
  }
  ```

## 🔄 Sprint 2: Comunicação de Tarefas e Apresentação de Workers

Implementa consumo de fila, distribuição de tarefas e relatório de status.

### Fluxo de Trabalho
1. **Apresentação**: Worker conecta e envia `WORKER_UUID` (com `SERVER_UUID` opcional se emprestado).
2. **Distribuição**: Master consome tarefa da fila e despacha.
3. **Processamento**: Worker simula execução (sleep aleatório).
4. **Confirmação**: Worker reporta status; Master responde com ACK.

### Payloads Oficiais
- **Pedido de Tarefa (Worker → Master)**:
  ```json
  {
    "WORKER": "ALIVE",
    "WORKER_UUID": "W-123",
    "SERVER_UUID": "Master_B"  // Opcional, se emprestado
  }
  ```
- **Envio de Tarefa (Master → Worker)**:
  ```json
  {
    "TASK": "QUERY",
    "USER": "Nome_Usuario"
  }
  ```
  Ou, se fila vazia:
  ```json
  {
    "TASK": "NO_TASK"
  }
  ```
- **Relatório de Status (Worker → Master)**:
  ```json
  {
    "STATUS": "OK",
    "TASK": "QUERY",
    "WORKER_UUID": "W-123"
  }
  ```
- **Confirmação (Master → Worker)**:
  ```json
  {
    "STATUS": "ACK",
    "WORKER_UUID": "W-123"
  }
  ```

## 🚀 Sprint 3: Protocolo de Negociação Master-to-Master e Redirecionamento Dinâmico de Workers

Implementa negociação consensual entre Masters para empréstimo de Workers quando um nó está saturado (fila > threshold). Workers são redirecionados dinamicamente e devolvidos quando a carga normaliza.

### Fluxo de Trabalho
1. **Monitoramento**: Master verifica carga (tamanho da fila).
2. **Negociação**: Se saturado, solicita ajuda a vizinho via P2P.
3. **Redirecionamento**: Vizinho concede Worker; Master instrui Worker a mudar.
4. **Devolução**: Quando carga baixa, Worker retorna ao Master original.

### Payloads Oficiais (P2P)
- **Pedido de Ajuda (Master → Master Vizinho)**:
  ```json
  {
    "TASK": "REQUEST_HELP",
    "MASTER_UUID": "Master_A",
    "NEEDED_WORKERS": 1
  }
  ```
- **Resposta de Concessão (Master Vizinho → Master)**:
  ```json
  {
    "TASK": "GRANT_HELP",
    "WORKER_UUID": "W-999"
  }
  ```
  Ou negação:
  ```json
  {
    "TASK": "DENY_HELP"
  }
  ```
- **Comando de Redirecionamento (Master → Worker)**:
  ```json
  {
    "TASK": "SWITCH_MASTER",
    "NEW_MASTER": "IP:porta"
  }
  ```
- **Devolução (Master → Master Vizinho)**:
  ```json
  {
    "TASK": "RETURN_WORKER",
    "WORKER_UUID": "W-999"
  }
  ```

## 🛠️ Como Executar

1. **Configure o `config.json`** (exemplo no repositório):
   ```json
   {
     "host": "localhost",
     "port": 5000,
     "master_uuid": "Master_A",
     "threshold": 3,
     "neighbors": ["localhost:5001"],
     "worker_uuid": "Worker_A1",
     "original_master": "Master_A"
   }
   ```

2. **Inicie o Master** (servidor):
   ```bash
   python master.py
   ```
   - Abre a porta de escuta e gerencia Workers.

3. **Inicie o Worker** (cliente):
   ```bash
   python worker.py
   ```
   - Conecta ao Master e inicia ciclo de tarefas/heartbeat.

**Nota**: Para testar P2P, execute múltiplos Masters em portas diferentes (ex: 5000 e 5001). Ajuste `neighbors` no `config.json` para apontar para outros Masters.

## 🧪 Testes e Cenários

- **CT01-CT05**: Verifique os cenários de teste descritos nas especificações (apresentação, tarefas, status, etc.).
- **Interoperabilidade**: Teste com sistemas de outras equipes usando o protocolo JSON.
- **Logs**: Use `logging` para debug; mensagens aparecem no console.

## 📝 Notas de Implementação

- **Strict Parsing**: Ignore campos desconhecidos no JSON para extensões futuras.
- **Case Sensitivity**: Valores como "ALIVE", "QUERY" são em maiúsculo.
- **Timeout**: Worker aguarda 5s por resposta antes de reconectar.
- **Resiliência**: Reconexão automática em caso de erro.

## 🤝 Contribuição

Sinta-se à vontade para abrir issues ou pull requests. Este projeto visa cumprir os objetivos O1-O6 do plano geral.
```
