# Padronizador Mobile Mercado

Painel web administrativo para automatização de dados de produtos no aplicativo **Mobile Mercado**, utilizando IA (OpenAI GPT-4o) integrada ao banco de dados **Firebase Firestore**.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Stack Tecnológica](#stack-tecnológica)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configuração e Instalação](#configuração-e-instalação)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Como Executar](#como-executar)
- [Acesso ao Painel](#acesso-ao-painel)
- [Módulos / Abas do Painel](#módulos--abas-do-painel)
  - [Padronizador de Nomes](#padronizador-de-nomes)
  - [Categorizador de Produtos — Automático](#categorizador-de-produtos--automático)
  - [Categorizador de Produtos — Dirigido](#categorizador-de-produtos--dirigido)
  - [Explorador Firestore](#explorador-firestore)
- [API REST](#api-rest)
- [WebSocket (Tempo Real)](#websocket-tempo-real)
- [Estrutura do Firestore](#estrutura-do-firestore)
- [Contador Diário de Uso](#contador-diário-de-uso)
- [Autenticação](#autenticação)
- [Estabelecimentos Cadastrados](#estabelecimentos-cadastrados)

---

## Visão Geral

O **Padronizador Mobile Mercado** é uma aplicação interna que realiza três tarefas automatizadas sobre o catálogo de produtos de um estabelecimento:

| Tarefa | Descrição |
|---|---|
| **Padronizar nomes** | Melhora o nome dos produtos usando IA, expandindo abreviações, corrigindo formatação e padronizando o texto. |
| **Categorizar (automático)** | A IA determina automaticamente a categoria e a subcategoria mais adequada para cada produto com base no seu nome. |
| **Categorizar (dirigido)** | O operador define a categoria alvo; a IA escolhe apenas a subcategoria. Permite filtrar quais produtos processar. |

Todas as alterações são escritas diretamente no **Firestore** e o progresso é exibido em **tempo real** via WebSocket.

---

## Stack Tecnológica

| Componente | Tecnologia |
|---|---|
| Backend | Python 3 + Flask 3.0 |
| Tempo real | Flask-SocketIO 5.5 |
| Banco de dados | Firebase Firestore (firebase-admin 6.5) |
| IA | OpenAI API — modelo `gpt-4o` (openai 1.30) |
| Frontend | HTML5 + CSS3 + Vanilla JavaScript |
| Variáveis de ambiente | python-dotenv 1.0 |
| CORS | Flask-Cors 4.0 |

---

## Estrutura do Projeto

```
padronizador_mobile_mercado/
├── app.py                  # Aplicação principal (Flask + agentes + rotas)
├── templates/
│   ├── index.html          # Painel principal (todas as abas)
│   ├── login.html          # Tela de login
│   └── firestore_explorer.html  # Explorador simples (legado)
├── static/
│   └── logo.png            # Logo da aplicação (também usada como favicon)
├── daily_stats.json        # Histórico diário de tokens/custo (gerado automaticamente)
├── app.log                 # Log de execução (gerado automaticamente)
├── .env                    # Variáveis de ambiente (não versionado)
├── .gitignore
└── venv/                   # Ambiente virtual Python
```

> Os arquivos `firestore_for_json.py`, `firestore_simple.py`, `nome_cat.py`, `nome_cats.py` são utilitários auxiliares de desenvolvimento.

---

## Configuração e Instalação

### Pré-requisitos

- Python 3.10+
- Conta Google Cloud com Firebase Firestore habilitado
- Credenciais de serviço Firebase (arquivo JSON ou Application Default Credentials)
- Chave de API OpenAI

### Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/lucasmaarin/automacoes-Mobile-Mercado.git
cd padronizador_mobile_mercado

# 2. Crie e ative o ambiente virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/macOS

# 3. Instale as dependências
pip install flask flask-socketio flask-cors openai firebase-admin python-dotenv

# 4. Configure o .env (veja seção abaixo)
```

---

## Variáveis de Ambiente

Crie o arquivo `.env` na raiz do projeto:

```env
# Chave da API OpenAI
OPENAI_API_KEY=sk-proj-...

# Categoria padrão (usado por scripts auxiliares)
CATEGORIA=ACOUGUE

# URL do frontend mobile (opcional)
URL_PRODUTOS=http://127.0.0.1:5500/frontend/index.html

# Credenciais do painel administrativo
ADMIN_USERNAME=admin
ADMIN_PASSWORD=admin123

# Caminho para as credenciais Firebase (opcional — usa ADC se omitido)
# FIREBASE_CREDENTIALS_PATH=serviceAccountKey.json

# Chave secreta Flask (opcional — usa valor padrão em dev)
# SECRET_KEY=minha-chave-secreta
```

> **Segurança:** nunca versione o `.env`. Ele já está no `.gitignore`.

---

## Como Executar

```bash
# Ativar o ambiente virtual (se não estiver ativo)
venv\Scripts\activate

# Iniciar a aplicação
python app.py
```

A aplicação estará disponível em: **http://localhost:5000**

---

## Acesso ao Painel

Ao acessar qualquer rota, o sistema redireciona para a tela de login caso não haja sessão ativa.

| Campo | Valor padrão | Configuração |
|---|---|---|
| Usuário | `admin` | `.env` → `ADMIN_USERNAME` |
| Senha | `admin123` | `.env` → `ADMIN_PASSWORD` |

- Rota de login: `GET/POST /login`
- Rota de logout: `GET /logout`

---

## Módulos / Abas do Painel

### Padronizador de Nomes

Melhora os nomes dos produtos de um estabelecimento usando GPT-4o.

**Campos de configuração:**

| Campo | Descrição |
|---|---|
| Estabelecimento | Seletor com estabelecimentos cadastrados ou ID personalizado |
| Delay (s) | Intervalo entre chamadas à API (evita rate limit) |
| Dry Run | Simula as alterações sem gravar no Firestore |
| Categorias | Grade de seleção das categorias de produtos a processar |
| Prompt | Editor do prompt enviado à IA (salvo no Firestore em `Automacoes/padronizador_nomes`) |

**Fluxo:**
1. Carregue as categorias do estabelecimento
2. Selecione as categorias desejadas
3. (Opcional) Edite o prompt e salve
4. Clique em **Iniciar** — o progresso e os logs aparecem em tempo real

**Campo atualizado no produto:** `name`

---

### Categorizador de Produtos — Automático

A IA determina a categoria e a subcategoria mais adequadas para cada produto com base no seu nome.

**Fluxo (2 chamadas OpenAI por produto):**
1. **Chamada 1** — IA escolhe a `categoryId` entre todas as categorias disponíveis
2. **Chamada 2** — IA escolhe a `subcategoryId` entre as subcategorias da categoria escolhida

**Campos atualizados no produto:**

```json
{
  "categoriesIds": ["mercearia"],
  "subcategoriesIds": ["maionese"],
  "shelvesIds": ["mercearia_maionese"],
  "shelves": [
    {
      "id": "mercearia_maionese",
      "productCategoryId": "mercearia",
      "productSubcategoryId": "maionese",
      "categoryName": "Mercearia",
      "subcategoryName": "MAIONESE"
    }
  ]
}
```

---

### Categorizador de Produtos — Dirigido

Variante do agente automático onde **o operador define a categoria alvo** e a IA escolhe apenas a subcategoria.

**Campos de configuração:**

| Campo | Descrição |
|---|---|
| Estabelecimento | Seletor com estabelecimentos cadastrados |
| Categoria Alvo | Dropdown populado ao clicar em "Carregar Categorias" |
| Filtro por categoria | Quando ativado, processa apenas produtos que já pertencem às categorias selecionadas |
| Dry Run | Simula sem gravar |

**Casos de uso:**
- Recategorizar todos os produtos de `mercearia` com subcategorias mais precisas
- Processar um lote específico (ex: produtos atualmente em `conservas`) e movê-los para `mercearia`
- Padronizar subcategorias de uma categoria inteira sem risco de alterar a categoria principal

---

### Explorador Firestore

Ferramenta para inspecionar a estrutura do banco de dados sem sair do painel.

**Modo Rápido** — Faz uma consulta direta a qualquer caminho Firestore e exibe o JSON resultante.

**Modo Avançado** — Análise profunda com:
- Detecção de subcoleções aninhadas
- Análise de tipos de campos (map, array, timestamp, GeoPoint, DocumentReference)
- Cache de caminhos explorados
- Sugestão/autocomplete de caminhos
- Exportação do resultado como JSON

---

## API REST

### Autenticação

| Rota | Método | Descrição |
|---|---|---|
| `/login` | GET / POST | Tela de login |
| `/logout` | GET | Encerra a sessão |

### Padronizador de Nomes

| Rota | Método | Descrição |
|---|---|---|
| `/api/renamer/categories` | GET | Lista categorias do estabelecimento |
| `/api/renamer/prompt` | GET | Retorna o prompt atual (salvo ou padrão) |
| `/api/renamer/prompt` | POST | Salva um novo prompt no Firestore |
| `/api/renamer/start` | POST | Inicia a automação de nomes |
| `/api/renamer/stop` | POST | Para a automação em execução |
| `/api/renamer/status` | GET | Status e progresso atual |
| `/api/renamer/logs` | GET | Logs da execução |

**Body de `/api/renamer/start`:**
```json
{
  "estabelecimento_id": "jQQjHTCc2zW1tuZMQzGF",
  "categories": ["ACOUGUE", "BEBIDAS"],
  "delay": 1.0,
  "dry_run": false,
  "custom_prompt": "..."
}
```

### Categorizador — Automático

| Rota | Método | Descrição |
|---|---|---|
| `/api/categorizer/start` | POST | Inicia a categorização automática |
| `/api/categorizer/stop` | POST | Para a categorização |
| `/api/categorizer/status` | GET | Status e progresso |
| `/api/categorizer/logs` | GET | Logs da execução |
| `/api/categorizer/categories` | GET | Lista as categorias do estabelecimento |

**Body de `/api/categorizer/start`:**
```json
{
  "estabelecimento_id": "jQQjHTCc2zW1tuZMQzGF",
  "delay": 0.5,
  "dry_run": false
}
```

### Categorizador — Dirigido

| Rota | Método | Descrição |
|---|---|---|
| `/api/categorizer-targeted/start` | POST | Inicia o modo dirigido |
| `/api/categorizer-targeted/stop` | POST | Para a execução |
| `/api/categorizer-targeted/status` | GET | Status e progresso |
| `/api/categorizer-targeted/logs` | GET | Logs da execução |

**Body de `/api/categorizer-targeted/start`:**
```json
{
  "estabelecimento_id": "jQQjHTCc2zW1tuZMQzGF",
  "target_category_id": "mercearia",
  "filter_category_ids": ["conservas", "biscoitos"],
  "delay": 0.5,
  "dry_run": false
}
```

> `filter_category_ids` é opcional. Se omitido, processa todos os produtos.

### Explorador Firestore

| Rota | Método | Descrição |
|---|---|---|
| `/api/explorer-simple/explore` | POST | Explora um caminho no Firestore (modo rápido) |
| `/api/explorer/explore` | POST | Exploração avançada com análise de estrutura |
| `/api/explorer/suggestions` | POST | Sugestões de autocomplete para o caminho |
| `/api/explorer/cache` | GET | Lista caminhos em cache |
| `/api/explorer/cache/<path>` | GET | Retorna dados de um caminho em cache |
| `/api/explorer/export/<path>` | GET | Exporta a estrutura como JSON |
| `/api/explorer/status` | GET | Status do explorador |
| `/api/explorer/logs` | GET | Logs do explorador |

### Estatísticas

| Rota | Método | Descrição |
|---|---|---|
| `/api/stats/daily` | GET | Retorna uso de tokens e custo do dia e histórico |

**Resposta de `/api/stats/daily`:**
```json
{
  "success": true,
  "today": {
    "date": "2025-02-20",
    "tokens": 12500,
    "cost": 0.0312,
    "calls": 45
  },
  "all": {
    "2025-02-19": { "tokens": 8200, "cost": 0.0205, "calls": 30 }
  }
}
```

---

## WebSocket (Tempo Real)

A aplicação usa **Socket.IO** para transmitir atualizações de progresso e logs em tempo real.

### Eventos emitidos pelo servidor

| Evento | Payload | Descrição |
|---|---|---|
| `renamer_log_update` | `{timestamp, message, level}` | Novo log do Padronizador |
| `renamer_progress_update` | `{progress, current_product}` | Progresso do Padronizador |
| `renamer_status_update` | `{running, progress, current_product}` | Estado ao conectar |
| `categorizer_log_update` | `{timestamp, message, level}` | Novo log do Categorizador Auto |
| `categorizer_progress_update` | `{progress, current_product}` | Progresso do Categorizador Auto |
| `categorizer_targeted_log_update` | `{timestamp, message, level}` | Novo log do Categorizador Dirigido |
| `categorizer_targeted_progress_update` | `{progress, current_product}` | Progresso do Dirigido |
| `explorer_log_update` | `{timestamp, message, level}` | Novo log do Explorador |
| `daily_stats_update` | `{date, tokens, cost, calls}` | Atualização do contador diário |

### Estrutura do objeto `progress`

```json
{
  "total": 150,
  "processed": 42,
  "updated": 40,
  "errors": 2,
  "tokens_used": 18500,
  "estimated_cost": 0.0462
}
```

---

## Estrutura do Firestore

### Coleção de produtos

```
estabelecimentos/{estabelecimento_id}/Products/{product_id}
```

**Campos relevantes gerenciados pelos agentes:**

```
name                  string    Nome do produto (atualizado pelo Padronizador)
categoriesIds         array     IDs das categorias (ex: ["mercearia"])
subcategoriesIds      array     IDs das subcategorias (ex: ["maionese"])
shelvesIds            array     IDs combinados (ex: ["mercearia_maionese"])
shelves               array     Lista de objetos com detalhes da prateleira
  └ id                string    "{categoryId}_{subcategoryId}"
  └ productCategoryId string    ID da categoria
  └ productSubcategoryId string ID da subcategoria
  └ categoryName      string    Nome legível da categoria
  └ subcategoryName   string    Nome legível da subcategoria
```

### Coleção de categorias

```
estabelecimentos/{estabelecimento_id}/ProductCategories/{category_id}
```

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | string | Identificador único (ex: `mercearia`) |
| `name` | string | Nome legível (ex: `Mercearia`) |
| `isActive` | boolean | Se a categoria está ativa |
| `icon` | string | URL do ícone |

### Coleção de subcategorias

```
estabelecimentos/{estabelecimento_id}/ProductSubcategories/{subcategory_id}
```

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | string | Identificador único (ex: `maionese`) |
| `name` | string | Nome legível (ex: `MAIONESE`) |
| `categoryId` | string | ID da categoria pai |
| `isActive` | boolean | Se a subcategoria está ativa |

### Prompt salvo

```
Automacoes/padronizador_nomes
  └ prompt    string    Prompt personalizado da IA
  └ updated_at string   Data da última atualização
```

---

## Contador Diário de Uso

O contador registra automaticamente o consumo de tokens e custo estimado de **todas** as chamadas à API OpenAI realizadas pelos agentes.

- **Persistência:** arquivo `daily_stats.json` na raiz do projeto
- **Exibição:** canto superior direito do painel (`Hoje: X tokens • $Y.YYYY`)
- **Atualização:** em tempo real via WebSocket a cada chamada; também carregado ao conectar
- **Custo estimado** baseado nos preços do modelo `gpt-4o`:
  - Input: $0.0025 / 1K tokens
  - Output: $0.01 / 1K tokens

---

## Estabelecimentos Cadastrados

| Nome no painel | ID no Firestore |
|---|---|
| Estabelecimento Teste | `estabelecimento-teste` |
| Zero Grau | `jQQjHTCc2zW1tuZMQzGF` |

Para adicionar mais estabelecimentos, edite o `<select>` nos templates HTML (`templates/index.html`).
