# 🚀 Prefect Project

Projeto de exemplo utilizando **Prefect + Docker Compose** para criação, deploy e execução de flows e tasks.

---

## 🐳 Subir o ambiente

Para iniciar os containers:

```bash
make up
```

Principais serviços:

- **Prefect Server**
- **Prefect Worker**
- **PostgreSQL**
- **Setup automático dos deployments**

### Acessos

- **Dashboard:** `http://localhost:4200`
- **Swagger:** `http://localhost:4200/api/docs`

---

## 🔄 Deploy automático dos Flows

Os flows ficam dentro de:

```text
app/flows/
```

O script responsável pelo deploy automático é:

```text
deploy_flows.py
```

Ele faz o seguinte:

- *Procura automaticamente os flows dentro da pasta*
- *Identifica os flows existentes*
- *Cria os deployments no Prefect*
- *Atualiza os deployments gerenciados automaticamente*
- *Evita a necessidade de cadastrar cada flow manualmente*

Exemplo de estrutura:

```text
app/
└── flows/
    ├── users.py
    ├── sales.py
    └── reports.py
```

---

## ⚙️ Flows e Tasks

Um **Flow** representa o processo principal.

Uma **Task** representa uma etapa dentro desse processo.

```python
from prefect import flow, task


@task
def process_item(item_id: int):
    print(f"Processing item {item_id}")


@flow
def item_flow(item_id: int):
    process_item(item_id)
```

Estrutura:

```text
Flow
 |
 +-- Task
 +-- Task
 +-- Task
```

---

## 🎛️ Parametrização

Os flows podem receber parâmetros diretamente pelo Prefect Dashboard.

Exemplo:

```python
@flow
def item_flow(item_id: int):
    process_item(item_id)
```

Ao executar o deployment:

```text
item_id = 2
```

Esse valor é enviado para o flow e pode ser repassado para as tasks.

---

## 🔁 Retry

Tasks podem ser configuradas para tentar novamente automaticamente em caso de erro.

```python
@task(
    retries=3,
    retry_delay_seconds=5,
)
def process_item(item_id: int):
    print(f"Processing item {item_id}")
```

Configuração:

- **retries:** `3`
- **retry delay:** `5 seconds`
- **comportamento:** a task tenta novamente automaticamente em caso de falha

Exemplo:

```text
Attempt 1 -> Failed
   |
   +-- wait 5s
   |
Attempt 2 -> Failed
   |
   +-- wait 5s
   |
Attempt 3 -> Success
```

---

## ✅ Resumo

Até o momento, o projeto possui:

- **Ambiente completo em Docker Compose**
- **Prefect Server + Worker + PostgreSQL**
- **Deploy automático dos flows**
- **Execução através do Prefect Dashboard**
- **Flows compostos por tasks**
- **Tasks parametrizadas**
- **Retry automático em caso de falha**
- **Agendamento**
- **Paralelismo**
- **Cache**
