# Monkey-patch deve ser o primeiro import para modo gevent (producao)
try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass

import os
import logging
import time
import json
from typing import List, Dict, Any
from datetime import datetime
from threading import Thread
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import firebase_admin
from firebase_admin import credentials, firestore
from openai import OpenAI
import openai as _openai_module
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template, Response, session, redirect, url_for
from flask_socketio import SocketIO, emit
from flask_cors import CORS

# Tipos Firestore especiais
try:
    from google.cloud.firestore_v1 import DocumentReference, GeoPoint
except Exception:
    try:
        from google.cloud.firestore_v1 import DocumentReference
        from google.cloud.firestore_v1._helpers import GeoPoint
    except Exception:
        DocumentReference = None
        GeoPoint = None

try:
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
except Exception:
    DatetimeWithNanoseconds = None

try:
    from google.protobuf.timestamp_pb2 import Timestamp as ProtoTimestamp
except Exception:
    ProtoTimestamp = None

# ============================================================
# Config
# ============================================================
load_dotenv()

_log_handlers = [logging.StreamHandler()]
if os.getenv('LOG_TO_FILE', '').lower() in ('1', 'true', 'yes'):
    _log_handlers.append(logging.FileHandler('app.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=_log_handlers
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-prod')
try:
    import gevent as _gevent_check  # noqa
    _async_mode = 'gevent'
except ImportError:
    _async_mode = 'threading'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode=_async_mode)
CORS(app)
openai_client = OpenAI()

def _reload_openai_client(api_key: str):
    """Recria o cliente OpenAI com uma nova chave de API."""
    global openai_client
    openai_client = OpenAI(api_key=api_key)

def _is_quota_error(exc: Exception) -> bool:
    """Retorna True se o erro indica que os créditos da API OpenAI esgotaram."""
    msg = str(exc).lower()
    return (
        isinstance(exc, _openai_module.RateLimitError)
        and ('insufficient_quota' in msg or 'quota' in msg or 'billing' in msg or 'exceeded' in msg)
    )

def emit_quota_exceeded():
    """Emite evento WebSocket avisando que os créditos da OpenAI acabaram."""
    try:
        socketio.emit('openai_quota_exceeded', {
            'message': 'Os créditos da API OpenAI acabaram. Verifique o saldo em platform.openai.com.'
        })
    except Exception:
        pass

# ============================================================
# Firebase compartilhado
# ============================================================
db = None

def init_firebase():
    global db
    if not firebase_admin._apps:
        # 1. JSON inline (preferido em producao/Render — cole o conteudo do JSON como env var)
        creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON', '').strip()
        if creds_json:
            creds_dict = json.loads(creds_json)
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
        # 2. Caminho para arquivo (desenvolvimento local)
        elif os.getenv('FIREBASE_CREDENTIALS_PATH') and os.path.exists(os.getenv('FIREBASE_CREDENTIALS_PATH')):
            cred = credentials.Certificate(os.getenv('FIREBASE_CREDENTIALS_PATH'))
            firebase_admin.initialize_app(cred)
        # 3. Application Default Credentials (Google Cloud / fallback)
        else:
            firebase_admin.initialize_app()
    db = firestore.client()
    return db

# ============================================================
# Serializadores JSON
# ============================================================
def to_json_safe(value):
    """Converte recursivamente tipos do Firestore em algo serializavel."""
    if DatetimeWithNanoseconds and isinstance(value, DatetimeWithNanoseconds):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if GeoPoint and isinstance(value, GeoPoint):
        return {"_type": "GeoPoint", "latitude": float(value.latitude), "longitude": float(value.longitude)}
    if hasattr(value, "latitude") and hasattr(value, "longitude") and \
       isinstance(getattr(value, "latitude"), (int, float)) and \
       isinstance(getattr(value, "longitude"), (int, float)):
        return {"_type": "GeoPoint", "latitude": float(value.latitude), "longitude": float(value.longitude)}
    if DocumentReference and isinstance(value, DocumentReference):
        return {"_type": "DocumentReference", "path": value.path}
    if hasattr(value, "path") and hasattr(value, "parent") and hasattr(value, "id"):
        try:
            return {"_type": "DocumentReference", "path": str(value.path)}
        except Exception:
            pass
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return {"_type": "bytes", "base16": bytes(value).hex()}
        except Exception:
            return {"_type": "bytes", "len": len(value)}
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(v) for v in list(value)]
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def firestore_default(obj):
    """Serializador para json.dumps(default=...)."""
    if 'GeoPoint' in obj.__class__.__name__ if hasattr(obj, '__class__') else False:
        try:
            lat = getattr(obj, 'latitude', None) or getattr(obj, '_latitude', None)
            lng = getattr(obj, 'longitude', None) or getattr(obj, '_longitude', None)
            return {"_type": "GeoPoint", "latitude": float(lat), "longitude": float(lng)}
        except Exception:
            return {"_type": "GeoPoint", "repr": str(obj)}
    try:
        if DocumentReference and isinstance(obj, DocumentReference):
            return {"_type": "DocumentReference", "path": obj.path}
    except Exception:
        pass
    if ProtoTimestamp is not None and isinstance(obj, ProtoTimestamp):
        try:
            return obj.ToJsonString()
        except Exception:
            pass
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray, memoryview)):
        try:
            return {"_type": "bytes", "base16": bytes(obj).hex()}
        except Exception:
            return {"_type": "bytes", "len": len(obj)}
    if isinstance(obj, (set, tuple)):
        return list(obj)
    try:
        return str(obj)
    except Exception:
        return "<unserializable>"


def safe_sample(value):
    try:
        return json.loads(json.dumps(value, default=firestore_default))
    except Exception:
        s = str(value)
        return s[:100] + ("..." if len(s) > 100 else "")


# ============================================================
# Contagem diária de uso de tokens/custo
# ============================================================
DAILY_STATS_FILE = 'daily_stats.json'
_daily_stats_data: dict = {}


def _load_daily_stats():
    global _daily_stats_data
    try:
        if os.path.exists(DAILY_STATS_FILE):
            with open(DAILY_STATS_FILE, 'r') as f:
                _daily_stats_data = json.load(f)
    except Exception:
        _daily_stats_data = {}


def _save_daily_stats():
    try:
        with open(DAILY_STATS_FILE, 'w') as f:
            json.dump(_daily_stats_data, f)
    except Exception:
        pass


def get_today_stats() -> dict:
    today = datetime.now().strftime('%Y-%m-%d')
    d = _daily_stats_data.get(today, {})
    return {
        'date': today,
        'tokens': d.get('tokens', 0),
        'cost': d.get('cost', 0.0),
        'calls': d.get('calls', 0),
    }


def record_daily_usage(tokens: int, cost: float):
    today = datetime.now().strftime('%Y-%m-%d')
    if today not in _daily_stats_data:
        _daily_stats_data[today] = {'tokens': 0, 'cost': 0.0, 'calls': 0}
    _daily_stats_data[today]['tokens'] += tokens
    _daily_stats_data[today]['cost'] += cost
    _daily_stats_data[today]['calls'] += 1
    _save_daily_stats()
    try:
        socketio.emit('daily_stats_update', get_today_stats())
    except Exception:
        pass


_load_daily_stats()


# ============================================================
# Estado global
# ============================================================
automation_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'unchanged': 0, 'errors': 0,
        'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

explorer_state = {
    'exploring': False,
    'progress': {'total_docs': 0, 'processed_docs': 0, 'collections_found': 0},
    'current_path': None,
    'logs': [],
    'structure_cache': {}
}

categorizer_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

categorizer_targeted_state = {
    'running': False,
    'progress': {
        'total': 0, 'processed': 0, 'updated': 0,
        'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
    },
    'current_product': None,
    'logs': []
}

# Histórico de desfazer — limpo no início de cada execução
# Cada entrada: dict com dados suficientes para reverter a operação
undo_store = {
    'renamer': [],           # [{product_id, estabelecimento_id, old_name, new_name}]
    'categorizer': [],       # [{product_id, estabelecimento_id, old_data, new_data}]
    'categorizer_targeted': [],
}
_undo_lock = threading.Lock()


# ============================================================
# Classe: FirestoreProductAutomator
# ============================================================
class FirestoreProductAutomator:
    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0
        self.input_token_cost  = 0.00015 / 1000   # gpt-4o-mini: $0.15/1M input
        self.output_token_cost = 0.00060 / 1000   # gpt-4o-mini: $0.60/1M output
        self._lock = threading.Lock()

        # Sufixo fixo — sempre anexado automaticamente ao prompt pelo codigo.
        # Nao e editavel pelo usuario. Define o formato de entrada/saida da IA.
        self._prompt_suffix = "\n\nNome atual: {produto_nome}\n\nNome melhorado (diferente do original):"

        self.default_prompt_template = """
Voce e um especialista em nomenclatura de produtos para um aplicativo de supermercado.

IMPORTANTE: Sua tarefa e SEMPRE MELHORAR o nome do produto. NUNCA retorne o nome original inalterado.

Analise o nome do produto atual e sugira um nome MELHOR que seja:
- Mais profissional e padronizado
- Claro e descritivo
- Adequado para sistemas de inventario
- Expandir abreviacoes (tv -> Televisao, cel -> Celular, etc.)
- Formatacao: Apenas a primeira letra de cada palavra principal maiuscula
- Respeite os nomes das marcas originais (ex: LG, Samsung, Apple)
- Adicione especificacoes quando possivel
- Organize as informacoes de forma logica
- Elimine abreviacoes tecnicas e coloque a palavra completa, exemplo COZ > cozido ou cozina dependendo do produto, Bdj > bandeja, (primeiras letras das abreviacoes trasnformadas em letra minusculas) se o nome for passar 7 tokens, apenas retire a abreviacao
- Siga esta estrutura: [Produto principal] + [Marca/variante] + [Peso/quantidade]
- Nao ultrapassar 7 tokens
- Coloque os acentos das respectivas palavras, exemplo: Purissima > Puríssima, Agua > Água
- L de Litros manter maiusculo, mas ml minusculo

Exemplos de transformacoes que voce deve fazer:
- KETCHUP HEINZ 1.033KG -> Ketchup Heinz 1.033Kg
- 260GR -> 260g
- 260 GR -> 260g
- "TRAD" -> "Tradicional"
- "S MIUDOS" -> "S/Miudos"
- "C BACON" -> "C/Bacon"
- "QJO" -> "Queijo"
- "C QJO" -> "C/Queijo"
- "CACHACA" -> "Cachaça"
- "PECA" -> "Peça"
- "CURACAU" -> "Curaçau"
- "LIM" -> "Limao"
- "LIMAO" -> "Limao"
- "HIDROT" -> "Hidrotonico"
- "BEB" -> "Bebida"
- "CERV" -> "Cerveja"
- "GHOSTFORCE" -> "Ghost Force"
- "Bdj" -> "Bandeja" (ou retirar se o nome ultrapasar 7 tokens)
- "DIET" -> "Diet"
- "MACA" -> "Maça"
- "RECH AVEL" -> "Recheio de Avela"
- "SALG" -> "Salgadinho" (Se for um salgadinho e nao um salgado de festa)
- "CX 48" -> "Caixa 48 Unidades"
- GF ou Gf = Garrafa
- GR = g
- KG = Kg
- PC = Pacote
- VD = Vidro
- CEST = Cesta
- BRC = Branco
- TP1 = Tipo 1
- TRAD = Tradicional
- SCH = Sache
- SC = Sacola
- PCT = Pacote
- CX = Caixa
- SANT = Sanitizante
- LIQUI = Liquido
- FAR = Farinha
- PUL = Pullman
- Choc = Chocolate
- Choco = Chocolate
- Calif = California
- AMDOA = Amendoa
- CARAM = Caramelo
- MOR = Morango
- BAN = Banana
- BCO = Bacon
- UND = Unidade
- PIC = Picante
- GL = Galinha
- C = com
- KINORR = knorr
- DF = Defumado
- XZ = Xadrez
- TEMP = Temperada
- CEB = Cebola
- CARAM = Caramelo
- SACHET = Sache

REGRA OBRIGATORIA: O nome que voce retornar DEVE ser diferente e melhor que o original.

Palavras ou abreviacoes para retirar:
- PT
- TP
- VDO
- VD
- UN/0001/UN
- ESTR
- PCT
- CITR
- UN/0001
- PRECIF
- ELMA CHIPS
- TP
- PT/0012/U
- PC10UN
- 12X140ML
- 1X200ML
- 6XFD
- UN/0001
- PT/0010/UN
- FA
- 12X10
- COND
- 1010KG
- 52x60g
- 66x100g
- 001/u
- FD
- ART BREAD
- PM
- 6x50gr
- 6x55gr
- PET
- LF
- LT
- UN/0001/UN
- FD/0001/UN
- PM
- GOU
- QD
- FRAPE
- RANC
- DP17X85G
- NUTS
- SUIRL
- (8x36g)
- 8x36g
- CX/0040/UN
- Unidades
- 70x130g
- 40x100g
- SQZ
- EXT
- 12X50ML
- SACH
"""

        # Tentar carregar prompt salvo do Firestore, senao usa o padrao
        saved = self.load_prompt_from_firestore()
        self.prompt_template = saved if saved else self.default_prompt_template

    def load_prompt_from_firestore(self) -> str:
        """Carrega as instrucoes do prompt salvas no Firestore (sem o sufixo fixo)."""
        try:
            doc_ref = self.db.collection('Automacoes').document('padronizador_nomes')
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                prompt = data.get('prompt', '').strip()
                if prompt:
                    # Compatibilidade: remove sufixo fixo caso esteja salvo em dados antigos
                    for old_suffix in ['\n\nNome atual: {produto_nome}\n\nNome melhorado (diferente do original):', 'Nome atual: {produto_nome}']:
                        if old_suffix in prompt:
                            prompt = prompt[:prompt.index(old_suffix)].rstrip()
                            break
                    if prompt:
                        logger.info("Instrucoes carregadas do Firestore (Automacoes/padronizador_nomes)")
                        return prompt
            return None
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar prompt do Firestore: {e}")
            return None

    def save_prompt_to_firestore(self, prompt: str) -> bool:
        """Salva o prompt na colecao Automacoes do Firestore."""
        try:
            doc_ref = self.db.collection('Automacoes').document('padronizador_nomes')
            doc_ref.set({
                'prompt': prompt,
                'updated_at': datetime.now().isoformat(),
                'tool': 'padronizador_nomes',
                'description': 'Prompt usado pela IA para padronizar nomes de produtos'
            }, merge=True)
            self.prompt_template = prompt
            logger.info("Prompt salvo no Firestore (Automacoes/padronizador_nomes)")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar prompt no Firestore: {e}")
            return False

    def log_message(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        automation_state['logs'].append(log_entry)
        if len(automation_state['logs']) > 100:
            automation_state['logs'] = automation_state['logs'][-100:]
        socketio.emit('renamer_log_update', log_entry)
        logger.info(f"{level.upper()}: {message}")

    def update_progress(self, current_product=None):
        if current_product:
            automation_state['current_product'] = current_product
        socketio.emit('renamer_progress_update', {
            'progress': automation_state['progress'],
            'current_product': automation_state['current_product']
        })

    def get_available_categories(self, estabelecimento_id: str = "estabelecimento-teste") -> List[str]:
        """Busca categorias unicas dos shelves de todos os produtos."""
        try:
            collection_ref = (
                self.db.collection('estabelecimentos')
                .document(estabelecimento_id)
                .collection('Products')
            )
            docs = collection_ref.stream()
            categories = set()
            for doc in docs:
                doc_data = doc.to_dict()
                if doc_data and 'shelves' in doc_data:
                    for shelf in doc_data.get('shelves', []):
                        cat = shelf.get('categoryName')
                        if cat:
                            categories.add(cat)
            return sorted(categories)
        except Exception as e:
            logger.error(f"Erro ao buscar categorias: {e}")
            return []

    def get_products_from_firestore(self, estabelecimento_id: str = "estabelecimento-teste",
                                     categories: List[str] = None) -> List[Dict[str, Any]]:
        try:
            products = []
            categories_set = set(categories) if categories else set()

            collection_ref = (
                self.db.collection('estabelecimentos')
                .document(estabelecimento_id)
                .collection('Products')
            )
            docs = collection_ref.stream()

            for doc in docs:
                doc_data = doc.to_dict()
                if doc_data and 'name' in doc_data and 'shelves' in doc_data:
                    shelves = doc_data.get('shelves', [])
                    for shelf in shelves:
                        shelf_category = shelf.get('categoryName')
                        if shelf_category in categories_set:
                            products.append({
                                'id': doc.id,
                                'name': doc_data['name'],
                                'original_description': doc_data['name'],
                                'shelves': shelves,
                                'matched_category': shelf_category
                            })
                            break

            categories_list = ', '.join(sorted(categories_set))
            self.log_message(f"Categorias sendo processadas: {categories_list}", "info")
            self.log_message(f"Encontrados {len(products)} produtos das categorias especificadas", "info")

            category_counts = {}
            for product in products:
                cat = product.get('matched_category', 'Unknown')
                category_counts[cat] = category_counts.get(cat, 0) + 1
            for category, count in sorted(category_counts.items()):
                self.log_message(f"  - {category}: {count} produtos", "info")

            return products
        except Exception as e:
            self.log_message(f"Erro ao buscar produtos do Firestore: {e}", "error")
            return []

    def get_improved_product_name(self, original_name: str, custom_prompt: str = None) -> str:
        try:
            clean_name = original_name.strip()
            if not clean_name:
                return original_name

            instructions = custom_prompt if custom_prompt else self.prompt_template
            prompt = instructions.rstrip() + self._prompt_suffix.format(produto_nome=clean_name)
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Voce e um especialista em nomenclatura de produtos. Sua tarefa e melhorar nomes de produtos para inventario empresarial. SEMPRE melhore o nome, nunca retorne o nome original inalterado."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.5,
                presence_penalty=0.3,
                frequency_penalty=0.3
            )

            if hasattr(response, 'usage') and response.usage:
                input_tokens = response.usage.prompt_tokens
                output_tokens = response.usage.completion_tokens
                call_cost = (input_tokens * self.input_token_cost) + (output_tokens * self.output_token_cost)
                with self._lock:
                    self.tokens_used += input_tokens + output_tokens
                    self.estimated_cost += call_cost
                record_daily_usage(input_tokens + output_tokens, call_cost)
                self.log_message(f"Tokens usados: +{input_tokens + output_tokens} | Total: {self.tokens_used} | Custo: ${self.estimated_cost:.4f}", "info")

            improved_name = response.choices[0].message.content.strip()

            if improved_name.lower() == clean_name.lower():
                self.log_message(f"OpenAI retornou o mesmo nome. Tentando melhorar manualmente: '{clean_name}'", "warning")
                improved_name = self.manual_improve_name(clean_name)

            improved_name = self.format_product_name(improved_name)
            self.log_message(f"Nome original: '{clean_name}' -> Nome melhorado: '{improved_name}'", "info")
            return improved_name
        except Exception as e:
            if _is_quota_error(e):
                self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                emit_quota_exceeded()
                raise
            self.log_message(f"Erro ao melhorar nome do produto '{original_name}': {e}", "error")
            try:
                improved_name = self.manual_improve_name(original_name)
                self.log_message(f"Melhoria manual aplicada: '{original_name}' -> '{improved_name}'", "info")
                return improved_name
            except Exception:
                return original_name

    def format_product_name(self, name: str) -> str:
        uppercase_words = ['NABRASA', 'TTO', 'JBS', 'CX']
        lowercase_words = ['de', 'da', 'do', 'para', 'por', 'em', 'na', 'no', 'e', 'ou', 'etc', 'a']
        words = name.split()
        formatted_words = []
        for i, word in enumerate(words):
            clean_word = ''.join(c for c in word if c.isalnum())
            if i == 0:
                if clean_word.upper() in uppercase_words:
                    formatted_words.append(word.upper())
                else:
                    formatted_words.append(word.capitalize())
            else:
                if clean_word.upper() in uppercase_words:
                    formatted_words.append(word.upper())
                elif clean_word.lower() in lowercase_words:
                    formatted_words.append(word.lower())
                else:
                    formatted_words.append(word.capitalize())
        return ' '.join(formatted_words)

    def manual_improve_name(self, original_name: str) -> str:
        name = original_name.strip().lower()
        replacements = {'sadia': 'Sadia'}
        words = name.split()
        improved_words = []
        for word in words:
            clean_word = ''.join(c for c in word if c.isalnum())
            if clean_word in replacements:
                improved_words.append(replacements[clean_word])
            else:
                improved_words.append(word.capitalize())
        return self.format_product_name(' '.join(improved_words))

    def update_product_in_firestore(self, product_id: str, new_description: str,
                                     estabelecimento_id: str = "estabelecimento-teste") -> bool:
        try:
            doc_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products')
                       .document(product_id))
            doc_ref.update({'name': new_description})
            self.log_message(f"Produto {product_id} atualizado com sucesso", "success")
            return True
        except Exception as e:
            self.log_message(f"Erro ao atualizar produto {product_id}: {e}", "error")
            return False

    def process_products_batch(self, products, estabelecimento_id="estabelecimento-teste",
                                delay_between_products=0, dry_run=False):
        stats = {
            'total': len(products), 'processed': 0, 'updated': 0,
            'errors': 0, 'unchanged': 0, 'tokens_used': 0, 'estimated_cost': 0.0
        }
        automation_state['progress'] = stats
        self.update_progress()
        self.log_message(f"Iniciando processamento de {stats['total']} produtos (8 paralelos)", "info")
        with _undo_lock:
            undo_store['renamer'].clear()

        def _process_one(args):
            i, product = args
            if not automation_state['running']:
                return
            original = product['name']
            pid = product['id']
            try:
                self.log_message(f"[{i}/{stats['total']}] {original}", "info")
                improved = self.get_improved_product_name(original, self._current_custom_prompt)

                with self._lock:
                    if improved.strip() == original.strip():
                        stats['unchanged'] += 1
                    elif dry_run:
                        self.log_message(f"[DRY RUN] '{original}' -> '{improved}'", "warning")
                        stats['updated'] += 1
                    else:
                        if self.update_product_in_firestore(pid, improved, estabelecimento_id):
                            self.log_message(f"Atualizado: '{original}' -> '{improved}'", "success")
                            stats['updated'] += 1
                            with _undo_lock:
                                undo_store['renamer'].append({
                                    'product_id': pid,
                                    'estabelecimento_id': estabelecimento_id,
                                    'old_name': original,
                                    'new_name': improved,
                                })
                        else:
                            stats['errors'] += 1
                    stats['processed'] += 1
                    stats['tokens_used'] = self.tokens_used
                    stats['estimated_cost'] = self.estimated_cost
                    automation_state['progress'] = stats.copy()

                self.update_progress({'id': pid, 'name': original, 'improved_name': improved,
                                      'index': i, 'total': stats['total']})
            except Exception as e:
                self.log_message(f"Erro produto {pid}: {e}", "error")
                with self._lock:
                    stats['errors'] += 1
                    stats['processed'] += 1
                    automation_state['progress'] = stats.copy()
                self.update_progress()

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(_process_one, enumerate(products, 1)))

        return stats

    def run_automation(self, estabelecimento_id="estabelecimento-teste",
                       categories=None, delay_between_products=1.0, dry_run=False,
                       custom_prompt=None):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            self._current_custom_prompt = custom_prompt
            automation_state['running'] = True
            automation_state['logs'] = []

            self.log_message(f"Iniciando automacao para: {estabelecimento_id}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            products = self.get_products_from_firestore(estabelecimento_id, categories)
            if not products:
                self.log_message("Nenhum produto encontrado", "warning")
                automation_state['running'] = False
                return False

            self.log_message("Produtos encontrados:", "info")
            for product in products:
                self.log_message(f"  - {product['id']}: {product['name']}", "info")

            stats = self.process_products_batch(products, estabelecimento_id, delay_between_products, dry_run)

            self.log_message("=== ESTATISTICAS FINAIS ===", "info")
            self.log_message(f"Total: {stats['total']}", "info")
            self.log_message(f"Processados: {stats['processed']}", "info")
            self.log_message(f"Atualizados: {stats['updated']}", "success")
            self.log_message(f"Sem mudancas: {stats['unchanged']}", "info")
            self.log_message(f"Erros: {stats['errors']}", "error" if stats['errors'] > 0 else "info")
            self.log_message(f"Tokens: {self.tokens_used:,}", "info")
            self.log_message(f"Custo estimado: ${self.estimated_cost:.4f}", "info")

            success_rate = (stats['updated'] + stats['unchanged']) / stats['total'] * 100 if stats['total'] > 0 else 0
            self.log_message(f"Taxa de sucesso: {success_rate:.1f}%", "success")

            automation_state['running'] = False
            automation_state['current_product'] = None
            self.update_progress()
            return stats['errors'] == 0
        except Exception as e:
            self.log_message(f"Erro durante a automacao: {e}", "error")
            automation_state['running'] = False
            automation_state['current_product'] = None
            self.update_progress()
            return False


# ============================================================
# Classe: FirestoreStructureExplorer (avancado)
# ============================================================
class FirestoreStructureExplorer:
    def __init__(self, db_client):
        self.db = db_client

    def log_message(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        explorer_state['logs'].append(log_entry)
        if len(explorer_state['logs']) > 100:
            explorer_state['logs'] = explorer_state['logs'][-100:]
        socketio.emit('explorer_log_update', log_entry)
        logger.info(f"{level.upper()}: {message}")

    def update_progress(self):
        socketio.emit('explorer_progress_update', {
            'progress': explorer_state['progress'],
            'current_path': explorer_state['current_path']
        })

    def parse_firestore_path(self, path: str) -> List[str]:
        path = path.strip('/')
        if not path:
            return []
        return path.split('/')

    def get_document_structure(self, doc_ref, max_depth=3, current_depth=0) -> Dict[str, Any]:
        if current_depth >= max_depth:
            return {"_max_depth_reached": True}
        try:
            doc = doc_ref.get()
            if not doc.exists:
                return {"_document_exists": False}
            doc_data = doc.to_dict()
            structure = {
                "_document_id": doc.id,
                "_document_exists": True,
                "_fields": {},
                "_subcollections": []
            }
            if doc_data:
                for field_name, field_value in doc_data.items():
                    structure["_fields"][field_name] = self.analyze_field_type(field_name, field_value)
            try:
                collections = doc_ref.collections()
                for collection in collections:
                    collection_info = {"name": collection.id, "type": "collection", "sample_documents": []}
                    sample_docs = collection.limit(3).stream()
                    for sample_doc in sample_docs:
                        doc_structure = self.get_document_structure(sample_doc.reference, max_depth, current_depth + 1)
                        collection_info["sample_documents"].append(doc_structure)
                    structure["_subcollections"].append(collection_info)
            except Exception as e:
                self.log_message(f"Erro ao listar subcoleções: {e}", "warning")
            return structure
        except Exception as e:
            self.log_message(f"Erro ao analisar documento: {e}", "error")
            return {"_error": str(e)}

    def analyze_field_type(self, field_name: str, field_value: Any) -> Dict[str, Any]:
        field_info = {"name": field_name, "type": type(field_value).__name__, "value_sample": None, "structure": None}
        try:
            if isinstance(field_value, dict):
                field_info["type"] = "map"
                field_info["structure"] = {}
                for key, value in field_value.items():
                    field_info["structure"][key] = {"type": type(value).__name__, "sample": safe_sample(value)}
            elif isinstance(field_value, list):
                field_info["type"] = "array"
                field_info["array_length"] = len(field_value)
                if field_value:
                    first_element = field_value[0]
                    field_info["element_type"] = type(first_element).__name__
                    if isinstance(first_element, dict):
                        field_info["element_structure"] = {}
                        for key in first_element.keys():
                            field_info["element_structure"][key] = type(first_element[key]).__name__
                    field_info["element_sample"] = safe_sample(first_element)
            else:
                field_info["value_sample"] = safe_sample(field_value)
        except Exception as e:
            field_info["_error"] = str(e)
        return field_info

    def get_collection_structure(self, collection_ref, max_docs=10) -> Dict[str, Any]:
        structure = {
            "collection_id": collection_ref.id, "type": "collection",
            "document_count_sample": 0, "sample_documents": [],
            "common_fields": {}, "field_statistics": {}
        }
        try:
            docs = collection_ref.limit(max_docs).stream()
            doc_count = 0
            all_fields = {}
            for doc in docs:
                doc_count += 1
                explorer_state['progress']['processed_docs'] += 1
                self.update_progress()
                doc_structure = self.get_document_structure(doc.reference, max_depth=2)
                structure["sample_documents"].append(doc_structure)
                if "_fields" in doc_structure:
                    for field_name, field_info in doc_structure["_fields"].items():
                        if field_name not in all_fields:
                            all_fields[field_name] = {"count": 0, "types": set(), "sample_values": []}
                        all_fields[field_name]["count"] += 1
                        all_fields[field_name]["types"].add(field_info.get("type", "unknown"))
                        if field_info.get("value_sample") is not None:
                            if len(all_fields[field_name]["sample_values"]) < 3:
                                all_fields[field_name]["sample_values"].append(field_info["value_sample"])
            structure["document_count_sample"] = doc_count
            for field_name, field_stats in all_fields.items():
                field_stats["types"] = list(field_stats["types"])
                structure["field_statistics"][field_name] = {
                    "frequency": field_stats["count"] / doc_count if doc_count > 0 else 0,
                    "count": field_stats["count"],
                    "types": field_stats["types"],
                    "sample_values": field_stats["sample_values"]
                }
                if doc_count > 0 and (field_stats["count"] / doc_count) >= 0.5:
                    structure["common_fields"][field_name] = structure["field_statistics"][field_name]
        except Exception as e:
            self.log_message(f"Erro ao analisar coleção: {e}", "error")
            structure["_error"] = str(e)
        return structure

    def explore_firestore_path(self, path: str, max_docs_per_collection=10) -> Dict[str, Any]:
        try:
            explorer_state['exploring'] = True
            explorer_state['current_path'] = path
            explorer_state['progress'] = {'total_docs': 0, 'processed_docs': 0, 'collections_found': 0}

            self.log_message(f"Explorando caminho: {path}", "info")
            path_components = self.parse_firestore_path(path)
            if not path_components:
                return self.explore_root_collections()

            is_collection_path = len(path_components) % 2 == 1
            if is_collection_path:
                collection_ref = self.db.collection(path_components[0])
                for i in range(1, len(path_components), 2):
                    if i + 1 < len(path_components):
                        collection_ref = collection_ref.document(path_components[i]).collection(path_components[i + 1])
                explorer_state['progress']['collections_found'] = 1
                self.update_progress()
                structure = self.get_collection_structure(collection_ref, max_docs_per_collection)
            else:
                doc_ref = self.db.collection(path_components[0])
                for i in range(1, len(path_components), 2):
                    if i < len(path_components):
                        doc_ref = doc_ref.document(path_components[i])
                        if i + 1 < len(path_components):
                            doc_ref = doc_ref.collection(path_components[i + 1])
                structure = self.get_document_structure(doc_ref)

            result = {
                "path": path,
                "path_type": "collection" if is_collection_path else "document",
                "explored_at": datetime.now().isoformat(),
                "structure": structure,
                "exploration_stats": explorer_state['progress']
            }
            explorer_state['structure_cache'][path] = result
            self.log_message(f"Exploração concluida para: {path}", "success")
            return result
        except Exception as e:
            self.log_message(f"Erro ao explorar caminho '{path}': {e}", "error")
            return {"path": path, "error": str(e), "explored_at": datetime.now().isoformat()}
        finally:
            explorer_state['exploring'] = False
            explorer_state['current_path'] = None
            self.update_progress()

    def explore_root_collections(self) -> Dict[str, Any]:
        try:
            collections = self.db.collections()
            root_structure = {"path": "/", "path_type": "root", "collections": []}
            for collection in collections:
                explorer_state['progress']['collections_found'] += 1
                self.update_progress()
                self.log_message(f"Encontrada coleção raiz: {collection.id}", "info")
                collection_info = {"name": collection.id, "type": "collection", "sample_document": None}
                try:
                    sample_doc = collection.limit(1).stream()
                    for doc in sample_doc:
                        collection_info["sample_document"] = self.get_document_structure(doc.reference, max_depth=1)
                        break
                except Exception as e:
                    self.log_message(f"Erro ao obter doc de exemplo de {collection.id}: {e}", "warning")
                root_structure["collections"].append(collection_info)
            return root_structure
        except Exception as e:
            self.log_message(f"Erro ao explorar coleções raiz: {e}", "error")
            return {"error": str(e)}

    def get_path_suggestions(self, partial_path: str) -> List[str]:
        try:
            suggestions = []
            path_components = self.parse_firestore_path(partial_path)
            if not path_components:
                collections = self.db.collections()
                for collection in collections:
                    suggestions.append(collection.id)
            else:
                is_collection_level = len(path_components) % 2 == 1
                if is_collection_level:
                    collection_ref = self.db.collection(path_components[0])
                    for i in range(1, len(path_components), 2):
                        if i + 1 < len(path_components):
                            collection_ref = collection_ref.document(path_components[i]).collection(path_components[i + 1])
                    docs = collection_ref.limit(10).stream()
                    for doc in docs:
                        suggestions.append(partial_path.rstrip('/') + '/' + doc.id)
                else:
                    doc_ref = self.db.collection(path_components[0])
                    for i in range(1, len(path_components), 2):
                        if i < len(path_components):
                            doc_ref = doc_ref.document(path_components[i])
                            if i + 1 < len(path_components):
                                doc_ref = doc_ref.collection(path_components[i + 1])
                    try:
                        collections = doc_ref.collections()
                        for collection in collections:
                            suggestions.append(partial_path.rstrip('/') + '/' + collection.id)
                    except Exception:
                        pass
            return suggestions[:10]
        except Exception as e:
            self.log_message(f"Erro ao gerar sugestoes para '{partial_path}': {e}", "error")
            return []


# ============================================================
# Classe: FirestoreSimpleExplorer (rapido)
# ============================================================
class FirestoreSimpleExplorer:
    def __init__(self, db_client):
        self.db = db_client

    def parse_path(self, path: str):
        path = path.strip('/')
        return path.split('/') if path else []

    def explore_collection(self, collection_ref, max_docs=5):
        docs_out = []
        docs = collection_ref.limit(max_docs).stream()
        for doc in docs:
            raw = doc.to_dict() or {}
            docs_out.append({"id": doc.id, "fields": to_json_safe(raw)})
        return docs_out

    def explore_document(self, doc_ref):
        doc = doc_ref.get()
        if not doc.exists:
            return {}
        raw = doc.to_dict() or {}
        return {"id": doc.id, "fields": to_json_safe(raw)}

    def explore(self, path: str, max_docs=5):
        components = self.parse_path(path)
        if not components:
            return {"error": "Caminho vazio"}
        is_collection_path = len(components) % 2 == 1
        if is_collection_path:
            collection_ref = self.db.collection(components[0])
            for i in range(1, len(components), 2):
                if i + 1 < len(components):
                    collection_ref = collection_ref.document(components[i]).collection(components[i + 1])
            return {"collection": components[-1] if components else "", "documents": self.explore_collection(collection_ref, max_docs)}
        else:
            doc_ref = self.db.collection(components[0])
            for i in range(1, len(components), 2):
                doc_ref = doc_ref.document(components[i])
                if i + 1 < len(components):
                    doc_ref = doc_ref.collection(components[i + 1])
            return {"document": components[-1], "data": self.explore_document(doc_ref)}


# ============================================================
# Classe: ProductCategorizerAgent
# ============================================================
class ProductCategorizerAgent:
    DEFAULT_CAT_SYSTEM_PROMPT = (
        "Voce e um especialista em categorizacao de produtos de supermercado. "
        "Responda APENAS com o ID solicitado, sem explicacoes, sem pontuacao extra."
    )

    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0
        self.input_token_cost  = 0.00015 / 1000   # gpt-4o-mini: $0.15/1M input
        self.output_token_cost = 0.00060 / 1000   # gpt-4o-mini: $0.60/1M output
        self._lock = threading.Lock()
        saved = self.load_cat_prompt_from_firestore()
        self.cat_system_prompt = saved if saved else self.DEFAULT_CAT_SYSTEM_PROMPT

    def load_cat_prompt_from_firestore(self) -> str:
        try:
            doc = self.db.collection('Automacoes').document('defenir_catsub').get()
            if doc.exists:
                prompt = (doc.to_dict() or {}).get('prompt', '')
                if prompt:
                    logger.info("Prompt do categorizador carregado do Firestore (Automacoes/defenir_catsub)")
                    return prompt
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar prompt do categorizador: {e}")
        return None

    def save_cat_prompt_to_firestore(self, prompt: str) -> bool:
        try:
            self.db.collection('Automacoes').document('defenir_catsub').set({
                'prompt': prompt,
                'descricao': 'Instrucoes de sistema usadas pela IA para definir categoria e subcategoria dos produtos',
                'updated_at': datetime.now().isoformat(),
            }, merge=True)
            self.cat_system_prompt = prompt
            logger.info("Prompt do categorizador salvo no Firestore (Automacoes/defenir_catsub)")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar prompt do categorizador: {e}")
            return False

    def log_message(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        categorizer_state['logs'].append(log_entry)
        if len(categorizer_state['logs']) > 200:
            categorizer_state['logs'] = categorizer_state['logs'][-200:]
        socketio.emit('categorizer_log_update', log_entry)
        logger.info(f"CATEGORIZER {level.upper()}: {message}")

    def update_progress(self, current_product=None):
        if current_product is not None:
            categorizer_state['current_product'] = current_product
        socketio.emit('categorizer_progress_update', {
            'progress': categorizer_state['progress'],
            'current_product': categorizer_state['current_product']
        })

    def load_categories(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductCategories'))
            docs = col_ref.stream()
            categories = []
            for doc in docs:
                data = doc.to_dict()
                if data:
                    categories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'isActive': data.get('isActive', True)
                    })
            self.log_message(f"Carregadas {len(categories)} categorias", "info")
            return categories
        except Exception as e:
            self.log_message(f"Erro ao carregar categorias: {e}", "error")
            return []

    def load_subcategories(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductSubcategories'))
            docs = col_ref.stream()
            subcategories = []
            for doc in docs:
                data = doc.to_dict()
                if data:
                    subcategories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'categoryId': data.get('categoryId', ''),
                        'isActive': data.get('isActive', True)
                    })
            self.log_message(f"Carregadas {len(subcategories)} subcategorias", "info")
            return subcategories
        except Exception as e:
            self.log_message(f"Erro ao carregar subcategorias: {e}", "error")
            return []

    def load_products(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            docs = col_ref.stream()
            products = []
            for doc in docs:
                data = doc.to_dict()
                if data and data.get('name'):
                    products.append({'id': doc.id, 'name': data['name']})
            self.log_message(f"Carregados {len(products)} produtos", "info")
            return products
        except Exception as e:
            self.log_message(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _call_openai(self, prompt, max_tokens=60):
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.cat_system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.1
            )
        except Exception as e:
            if _is_quota_error(e):
                self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                emit_quota_exceeded()
            raise
        if hasattr(response, 'usage') and response.usage:
            inp = response.usage.prompt_tokens
            out = response.usage.completion_tokens
            call_cost = (inp * self.input_token_cost) + (out * self.output_token_cost)
            with self._lock:
                self.tokens_used += inp + out
                self.estimated_cost += call_cost
            record_daily_usage(inp + out, call_cost)
        return response.choices[0].message.content.strip()

    def _best_match(self, returned_id, valid_items):
        """Retorna o item com id que melhor corresponde ao retornado pela IA."""
        valid_ids = {item['id'] for item in valid_items}
        if returned_id in valid_ids:
            return returned_id
        lower = returned_id.lower()
        for item in valid_items:
            if item['id'].lower() == lower:
                return item['id']
        for item in valid_items:
            if item['id'].lower() in lower or lower in item['id'].lower():
                return item['id']
        return valid_items[0]['id'] if valid_items else None

    def get_category_and_subcategory(self, product_name, categories, subcategories):
        # Step 1: determine category
        cats_text = "\n".join([f"id={c['id']} | nome={c['name']}" for c in categories])
        cat_prompt = (
            f"Produto: \"{product_name}\"\n\n"
            f"Categorias disponiveis:\n{cats_text}\n\n"
            f"Qual e o id da categoria mais adequada para este produto? "
            f"Responda APENAS com o id exato da categoria."
        )
        try:
            raw_cat = self._call_openai(cat_prompt, max_tokens=60)
            category_id = self._best_match(raw_cat, categories)
            if not category_id:
                self.log_message(f"Nao foi possivel determinar categoria para '{product_name}'", "warning")
                return None, None
        except Exception as e:
            self.log_message(f"Erro na chamada OpenAI (categoria) para '{product_name}': {e}", "error")
            return None, None

        # Step 2: determine subcategory from matching category
        relevant_subs = [s for s in subcategories if s['categoryId'] == category_id]
        if not relevant_subs:
            self.log_message(f"Nenhuma subcategoria para categoria '{category_id}', usando sem subcategoria", "warning")
            return category_id, None

        subs_text = "\n".join([f"id={s['id']} | nome={s['name']}" for s in relevant_subs])
        sub_prompt = (
            f"Produto: \"{product_name}\"\n"
            f"Categoria: {category_id}\n\n"
            f"Subcategorias disponiveis:\n{subs_text}\n\n"
            f"Qual e o id da subcategoria mais adequada para este produto? "
            f"Responda APENAS com o id exato da subcategoria."
        )
        try:
            raw_sub = self._call_openai(sub_prompt, max_tokens=100)
            subcategory_id = self._best_match(raw_sub, relevant_subs)
        except Exception as e:
            self.log_message(f"Erro na chamada OpenAI (subcategoria) para '{product_name}': {e}", "error")
            subcategory_id = relevant_subs[0]['id']

        return category_id, subcategory_id

    def update_product_categories(self, product_id, estabelecimento_id,
                                   category_id, subcategory_id,
                                   category_name, subcategory_name, dry_run=False,
                                   history_key='categorizer'):
        shelf_id = f"{category_id}_{subcategory_id}"
        shelf_entry = {
            'id': shelf_id,
            'productCategoryId': category_id,
            'productSubcategoryId': subcategory_id,
            'categoryName': category_name,
            'subcategoryName': subcategory_name
        }
        update_data = {
            'categoriesIds': [category_id],
            'subcategoriesIds': [subcategory_id],
            'shelves': [shelf_entry],
            'shelvesIds': [shelf_id]
        }
        if dry_run:
            self.log_message(
                f"[DRY RUN] {product_id}: categoria={category_id} | sub={subcategory_id}", "warning"
            )
            return True
        try:
            doc_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products')
                       .document(product_id))
            # Lê estado anterior para possibilitar desfazer
            old_data = {}
            try:
                snap = doc_ref.get()
                if snap.exists:
                    d = snap.to_dict() or {}
                    old_data = {
                        'categoriesIds': d.get('categoriesIds', []),
                        'subcategoriesIds': d.get('subcategoriesIds', []),
                        'shelves': d.get('shelves', []),
                        'shelvesIds': d.get('shelvesIds', []),
                    }
            except Exception:
                pass
            doc_ref.update(update_data)
            with _undo_lock:
                undo_store[history_key].append({
                    'product_id': product_id,
                    'estabelecimento_id': estabelecimento_id,
                    'old_data': old_data,
                    'new_data': update_data,
                })
            return True
        except Exception as e:
            self.log_message(f"Erro ao atualizar produto {product_id}: {e}", "error")
            return False

    # ---- Targeted (Dirigido) mode ----

    def log_message_targeted(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        categorizer_targeted_state['logs'].append(log_entry)
        if len(categorizer_targeted_state['logs']) > 200:
            categorizer_targeted_state['logs'] = categorizer_targeted_state['logs'][-200:]
        socketio.emit('categorizer_targeted_log_update', log_entry)
        logger.info(f"CATDIR {level.upper()}: {message}")

    def update_progress_targeted(self, current_product=None):
        if current_product is not None:
            categorizer_targeted_state['current_product'] = current_product
        socketio.emit('categorizer_targeted_progress_update', {
            'progress': categorizer_targeted_state['progress'],
            'current_product': categorizer_targeted_state['current_product']
        })

    def load_all_products_with_cats(self, estabelecimento_id):
        """Carrega todos os produtos com id, name e categoriesIds."""
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            products = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if data and data.get('name'):
                    products.append({
                        'id': doc.id,
                        'name': data['name'],
                        'categories_ids': data.get('categoriesIds', [])
                    })
            self.log_message_targeted(f"Carregados {len(products)} produtos", "info")
            return products
        except Exception as e:
            self.log_message_targeted(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _should_assign_to_category(self, product_name, category_name, target_subs):
        """Pergunta a IA se o produto pertence a categoria e, se sim, qual subcategoria."""
        subs_text = "\n".join([f"id={s['id']} | nome={s['name']}" for s in target_subs])
        prompt = (
            f"Produto: \"{product_name}\"\n"
            f"Categoria em avaliacao: {category_name}\n\n"
            f"Este produto pertence a categoria '{category_name}'?\n"
            f"Se SIM, responda: SIM|id_da_subcategoria\n"
            f"Se NAO, responda apenas: NAO\n\n"
            f"Subcategorias disponiveis:\n{subs_text}"
        )
        raw = self._call_openai(prompt, max_tokens=80).strip().upper()
        if raw.startswith('SIM'):
            parts = raw.split('|', 1)
            sub_id_raw = parts[1].strip() if len(parts) > 1 else ''
            sub_id = self._best_match(sub_id_raw, target_subs)
            return True, sub_id
        return False, None

    def run_categorization_targeted(self, estabelecimento_id, target_category_id,
                                     include_others=False, delay=0.5, dry_run=False):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_targeted_state['running'] = True
            categorizer_targeted_state['logs'] = []
            with _undo_lock:
                undo_store['categorizer_targeted'].clear()

            self.log_message_targeted(f"Iniciando modo dirigido: {estabelecimento_id}", "info")
            self.log_message_targeted(f"Categoria alvo: {target_category_id}", "info")
            if include_others:
                self.log_message_targeted("Opcao ativa: tambem avaliar produtos de outras categorias", "info")
            if dry_run:
                self.log_message_targeted("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            subcategories = self.load_subcategories(estabelecimento_id)
            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            target_cat = cat_by_id.get(target_category_id)
            if not target_cat:
                self.log_message_targeted(f"Categoria '{target_category_id}' nao encontrada", "error")
                categorizer_targeted_state['running'] = False
                return False

            target_subs = [s for s in subcategories if s['categoryId'] == target_category_id]
            if not target_subs:
                self.log_message_targeted(f"Sem subcategorias para '{target_category_id}'", "error")
                categorizer_targeted_state['running'] = False
                return False

            subs_preview = ', '.join(s['name'] for s in target_subs[:8])
            self.log_message_targeted(f"Subcategorias ({len(target_subs)}): {subs_preview}...", "info")

            all_products = self.load_all_products_with_cats(estabelecimento_id)
            if not all_products:
                self.log_message_targeted("Nenhum produto encontrado", "warning")
                categorizer_targeted_state['running'] = False
                return False

            # Fase 1: produtos que já têm a categoria alvo → só define subcategoria
            phase1 = [p for p in all_products if target_category_id in p.get('categories_ids', [])]
            # Fase 2 (opcional): demais produtos → IA decide se pertencem à categoria
            phase2 = [p for p in all_products if target_category_id not in p.get('categories_ids', [])] if include_others else []

            self.log_message_targeted(f"Fase 1 — ja em '{target_cat['name']}': {len(phase1)} produtos", "info")
            if include_others:
                self.log_message_targeted(f"Fase 2 — outras categorias a avaliar: {len(phase2)} produtos", "info")

            total = len(phase1) + len(phase2)
            if total == 0:
                self.log_message_targeted("Nenhum produto para processar", "warning")
                categorizer_targeted_state['running'] = False
                return False

            categorizer_targeted_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'skipped': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            self.update_progress_targeted()

            subs_text = "\n".join([f"id={s['id']} | nome={s['name']}" for s in target_subs])

            def _tick(ok):
                # Must be called inside self._lock
                if ok is True:
                    categorizer_targeted_state['progress']['updated'] += 1
                elif ok is False:
                    categorizer_targeted_state['progress']['errors'] += 1
                else:
                    categorizer_targeted_state['progress']['skipped'] += 1
                categorizer_targeted_state['progress']['processed'] += 1
                categorizer_targeted_state['progress']['tokens_used'] = self.tokens_used
                categorizer_targeted_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress_targeted()

            # ── Fase 1: só subcategoria ──────────────────────────────────────
            if phase1:
                self.log_message_targeted(f"=== Fase 1: definindo subcategorias ({len(phase1)} produtos, 8 paralelos) ===", "info")

            def _phase1_one(args):
                idx, product = args
                if not categorizer_targeted_state['running']:
                    return
                pid, pname = product['id'], product['name']
                self.update_progress_targeted({'id': pid, 'name': pname, 'index': idx, 'total': total})
                self.log_message_targeted(f"[{idx}/{total}] {pname}", "info")

                sub_prompt = (
                    f"Produto: \"{pname}\"\n"
                    f"Categoria: {target_category_id} ({target_cat['name']})\n\n"
                    f"Subcategorias disponiveis:\n{subs_text}\n\n"
                    f"Qual e o id da subcategoria mais adequada? Responda APENAS com o id exato."
                )
                try:
                    raw_sub = self._call_openai(sub_prompt, max_tokens=100)
                    subcategory_id = self._best_match(raw_sub, target_subs)
                except Exception as e:
                    self.log_message_targeted(f"  Erro OpenAI: {e}", "error")
                    with self._lock:
                        _tick(False)
                    return

                subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id)
                self.log_message_targeted(f"  -> {target_cat['name']} / {subcategory_name}", "success")
                ok = self.update_product_categories(pid, estabelecimento_id,
                                                    target_category_id, subcategory_id,
                                                    target_cat['name'], subcategory_name, dry_run,
                                                    history_key='categorizer_targeted')
                with self._lock:
                    _tick(ok)

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(_phase1_one, enumerate(phase1, 1)))

            # ── Fase 2: avalia se produto pertence à categoria ───────────────
            if phase2 and categorizer_targeted_state['running']:
                self.log_message_targeted(f"=== Fase 2: avaliando outros produtos ({len(phase2)}, 8 paralelos) ===", "info")

            def _phase2_one(args):
                idx, product = args
                if not categorizer_targeted_state['running']:
                    return
                pid, pname = product['id'], product['name']
                self.update_progress_targeted({'id': pid, 'name': pname, 'index': idx, 'total': total})
                self.log_message_targeted(f"[{idx}/{total}] {pname}", "info")

                try:
                    fits, subcategory_id = self._should_assign_to_category(pname, target_cat['name'], target_subs)
                except Exception as e:
                    self.log_message_targeted(f"  Erro OpenAI: {e}", "error")
                    with self._lock:
                        _tick(False)
                    return

                if not fits:
                    self.log_message_targeted(f"  -> Nao pertence a '{target_cat['name']}', ignorado", "info")
                    with self._lock:
                        _tick(None)
                    return

                subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id) if subcategory_id else ''
                self.log_message_targeted(f"  -> {target_cat['name']} / {subcategory_name}", "success")
                ok = self.update_product_categories(pid, estabelecimento_id,
                                                    target_category_id, subcategory_id,
                                                    target_cat['name'], subcategory_name, dry_run,
                                                    history_key='categorizer_targeted')
                with self._lock:
                    _tick(ok)

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(_phase2_one, enumerate(phase2, len(phase1) + 1)))

            prog = categorizer_targeted_state['progress']
            self.log_message_targeted("=== RESULTADO ===", "info")
            self.log_message_targeted(
                f"Total: {prog['total']} | Atualizados: {prog['updated']} | "
                f"Ignorados: {prog['skipped']} | Erros: {prog['errors']}", "info"
            )
            self.log_message_targeted(f"Tokens: {self.tokens_used:,} | Custo: ${self.estimated_cost:.4f}", "info")

            categorizer_targeted_state['running'] = False
            categorizer_targeted_state['current_product'] = None
            self.update_progress_targeted()
            return True
        except Exception as e:
            self.log_message_targeted(f"Erro: {e}", "error")
            categorizer_targeted_state['running'] = False
            categorizer_targeted_state['current_product'] = None
            self.update_progress_targeted()
            return False

    def run_categorization(self, estabelecimento_id, delay_between_products=0.5, dry_run=False):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_state['running'] = True
            categorizer_state['logs'] = []
            with _undo_lock:
                undo_store['categorizer'].clear()

            self.log_message(f"Iniciando categorizacao para: {estabelecimento_id}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            if not categories:
                self.log_message("Nenhuma categoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            # Exclui mercearia do modo automatico (gerenciada pelo Modo Dirigido)
            categories = [c for c in categories if c['id'].lower() != 'mercearia']
            if not categories:
                self.log_message("Nenhuma categoria disponivel apos remover mercearia", "warning")
                categorizer_state['running'] = False
                return False
            self.log_message(f"Categorias disponiveis (excluindo mercearia): {[c['id'] for c in categories]}", "info")

            subcategories = self.load_subcategories(estabelecimento_id)
            if not subcategories:
                self.log_message("Nenhuma subcategoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            products = self.load_products(estabelecimento_id)
            if not products:
                self.log_message("Nenhum produto encontrado", "warning")
                categorizer_state['running'] = False
                return False

            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            total = len(products)
            categorizer_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            self.update_progress()

            self.log_message(f"Processando {total} produtos (8 paralelos)", "info")

            def _cat_one(args):
                i, product = args
                if not categorizer_state['running']:
                    return
                pid = product['id']
                pname = product['name']
                self.log_message(f"[{i}/{total}] {pname}", "info")
                self.update_progress({'id': pid, 'name': pname, 'index': i, 'total': total})

                category_id, subcategory_id = self.get_category_and_subcategory(
                    pname, categories, subcategories
                )

                with self._lock:
                    if not category_id or not subcategory_id:
                        self.log_message(f"  Erro: nao foi possivel categorizar {pid}", "error")
                        categorizer_state['progress']['errors'] += 1
                    else:
                        category_name = cat_by_id.get(category_id, {}).get('name', category_id)
                        subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id)
                        self.log_message(f"  -> {category_name} / {subcategory_name}", "success")
                        ok = self.update_product_categories(
                            pid, estabelecimento_id,
                            category_id, subcategory_id,
                            category_name, subcategory_name, dry_run
                        )
                        if ok:
                            categorizer_state['progress']['updated'] += 1
                        else:
                            categorizer_state['progress']['errors'] += 1
                    categorizer_state['progress']['processed'] += 1
                    categorizer_state['progress']['tokens_used'] = self.tokens_used
                    categorizer_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress()

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(_cat_one, enumerate(products, 1)))

            prog = categorizer_state['progress']
            self.log_message("=== ESTATISTICAS FINAIS ===", "info")
            self.log_message(f"Total: {prog['total']}", "info")
            self.log_message(f"Processados: {prog['processed']}", "info")
            self.log_message(f"Atualizados: {prog['updated']}", "success")
            self.log_message(f"Erros: {prog['errors']}", "error" if prog['errors'] > 0 else "info")
            self.log_message(f"Tokens: {self.tokens_used:,}", "info")
            self.log_message(f"Custo estimado: ${self.estimated_cost:.4f}", "info")

            categorizer_state['running'] = False
            categorizer_state['current_product'] = None
            self.update_progress()
            return True
        except Exception as e:
            self.log_message(f"Erro durante a categorizacao: {e}", "error")
            categorizer_state['running'] = False
            categorizer_state['current_product'] = None
            self.update_progress()
            return False


# ============================================================
# Instancias globais
# ============================================================
automator = None
advanced_explorer = None
simple_explorer = None
categorizer = None


def init_all():
    global automator, advanced_explorer, simple_explorer, categorizer
    try:
        init_firebase()
        automator = FirestoreProductAutomator(db)
        advanced_explorer = FirestoreStructureExplorer(db)
        simple_explorer = FirestoreSimpleExplorer(db)
        categorizer = ProductCategorizerAgent(db)
        # Carrega chave OpenAI salva no Firestore (se existir)
        try:
            if db:
                doc = db.collection('Automacoes').document('config').get()
                if doc.exists:
                    key = (doc.to_dict() or {}).get('openai_api_key', '').strip()
                    if key:
                        _reload_openai_client(key)
                        logger.info("Chave OpenAI carregada do Firestore")
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar chave OpenAI do Firestore: {e}")
        logger.info("Todos os modulos inicializados com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao inicializar: {e}")
        return False


# ============================================================
# Autenticação de administrador
# ============================================================

def get_admin_credentials():
    """Busca credenciais de admin no Firestore (Automacoes/admin > userAdmin / passAdmin).
    Usa fallback para variáveis de ambiente se o Firestore estiver indisponível."""
    try:
        if db:
            doc = db.collection('Automacoes').document('admin').get()
            if doc.exists:
                data = doc.to_dict() or {}
                user = data.get('userAdmin', '').strip()
                passwd = data.get('passAdmin', '').strip()
                if user and passwd:
                    return user, passwd
    except Exception as e:
        logger.warning(f"Credenciais Firestore indisponíveis, usando .env: {e}")
    return os.getenv('ADMIN_USERNAME', 'admin'), os.getenv('ADMIN_PASSWORD', 'admin123')

PUBLIC_ROUTES = {'login', 'static'}

@app.before_request
def require_login():
    if request.endpoint in PUBLIC_ROUTES:
        return
    if not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        admin_user, admin_pass = get_admin_credentials()
        if username == admin_user and password == admin_pass:
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = 'Usuário ou senha incorretos.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ============================================================
# Rotas: Configurações
# ============================================================

@app.route('/api/settings', methods=['GET'])
def get_settings():
    username, _ = get_admin_credentials()
    api_key_preview = ''
    api_key_set = False
    tema = False
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                data = doc.to_dict() or {}
                key = data.get('openai_api_key', '').strip()
                if key:
                    api_key_set = True
                    visible = min(8, len(key))
                    tail = key[-4:] if len(key) > 4 else ''
                    api_key_preview = key[:visible] + '...' + tail
                tema = bool(data.get('tema', False))
    except Exception as e:
        logger.warning(f"Erro ao buscar config do Firestore: {e}")
    return jsonify({'username': username, 'api_key_set': api_key_set, 'api_key_preview': api_key_preview, 'tema': tema})


@app.route('/api/settings/credentials', methods=['POST'])
def update_credentials():
    data = request.get_json() or {}
    new_user = data.get('username', '').strip()
    new_pass = data.get('password', '').strip()
    if not new_user or not new_pass:
        return jsonify({'success': False, 'error': 'Usuário e senha são obrigatórios'}), 400
    try:
        db.collection('Automacoes').document('admin').set({
            'userAdmin': new_user,
            'passAdmin': new_pass,
            'updated_at': datetime.now().isoformat(),
        }, merge=True)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar credenciais: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/openai-key', methods=['POST'])
def update_openai_key():
    data = request.get_json() or {}
    key = data.get('api_key', '').strip()
    if not key or not key.startswith('sk-') or len(key) < 20:
        return jsonify({'success': False, 'error': 'Chave inválida. Deve começar com sk- e ter pelo menos 20 caracteres'}), 400
    try:
        db.collection('Automacoes').document('config').set({
            'openai_api_key': key,
            'updated_at': datetime.now().isoformat(),
        }, merge=True)
        _reload_openai_client(key)
        logger.info("Chave OpenAI atualizada via settings")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar chave OpenAI: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/theme', methods=['GET'])
def get_theme():
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                tema = (doc.to_dict() or {}).get('tema', False)
                return jsonify({'tema': bool(tema)})
    except Exception as e:
        logger.warning(f"Erro ao buscar tema: {e}")
    return jsonify({'tema': False})


@app.route('/api/settings/theme', methods=['POST'])
def save_theme():
    data = request.get_json() or {}
    tema = bool(data.get('tema', False))
    try:
        db.collection('Automacoes').document('config').set(
            {'tema': tema, 'updated_at': datetime.now().isoformat()},
            merge=True
        )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao salvar tema: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# Rotas: Pagina principal
# ============================================================
@app.route('/')
def index():
    return render_template("index.html")


# ============================================================
# Rotas: Renamer
# ============================================================
@app.route('/api/renamer/categories', methods=['GET'])
def get_categories():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    estabelecimento_id = request.args.get('estabelecimento_id', 'estabelecimento-teste')
    categories = automator.get_available_categories(estabelecimento_id)
    return jsonify({'success': True, 'categories': categories})


@app.route('/api/renamer/prompt', methods=['GET'])
def get_prompt():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    return jsonify({
        'success': True,
        'prompt': automator.prompt_template,
        'default_prompt': automator.default_prompt_template
    })


@app.route('/api/renamer/prompt', methods=['POST'])
def save_prompt():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    try:
        data = request.json or {}
        prompt = data.get('prompt', '').strip()
        if not prompt:
            return jsonify({'error': 'Prompt vazio'}), 400
        if automator.save_prompt_to_firestore(prompt):
            return jsonify({'success': True, 'message': 'Prompt salvo no Firestore'})
        else:
            return jsonify({'error': 'Erro ao salvar no Firestore'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/renamer/start', methods=['POST'])
def start_automation():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    if automation_state['running']:
        return jsonify({'error': 'Automacao ja esta em execucao'}), 400
    try:
        data = request.json or {}
        estabelecimento_id = data.get('estabelecimento_id', 'estabelecimento-teste')
        delay = float(data.get('delay', 1.0))
        dry_run = data.get('dry_run', False)
        categories = data.get('categories', [])
        custom_prompt = data.get('custom_prompt', '').strip() or None

        if not categories:
            return jsonify({'error': 'Selecione ao menos uma categoria'}), 400


        def run_thread():
            automator.run_automation(estabelecimento_id, categories, delay, dry_run, custom_prompt)

        thread = Thread(target=run_thread)
        thread.daemon = True
        thread.start()
        return jsonify({'success': True, 'message': 'Automacao iniciada'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/renamer/stop', methods=['POST'])
def stop_automation():
    if not automation_state['running']:
        return jsonify({'error': 'Nenhuma automacao em execucao'}), 400
    automation_state['running'] = False
    return jsonify({'success': True, 'message': 'Automacao interrompida'})


@app.route('/api/renamer/undo-info', methods=['GET'])
def renamer_undo_info():
    with _undo_lock:
        count = len(undo_store['renamer'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/renamer/undo', methods=['POST'])
def renamer_undo():
    global automator
    if not automator:
        return jsonify({'error': 'Automatizador nao inicializado'}), 500
    if automation_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    with _undo_lock:
        changes = list(undo_store['renamer'])
    if not changes:
        return jsonify({'error': 'Nenhuma alteracao para desfazer'}), 400
    reverted, errors = 0, 0
    for entry in changes:
        try:
            doc_ref = (automator.db.collection('estabelecimentos')
                       .document(entry['estabelecimento_id'])
                       .collection('Products')
                       .document(entry['product_id']))
            doc_ref.update({'name': entry['old_name']})
            reverted += 1
        except Exception as e:
            logger.error(f"Undo renamer erro {entry['product_id']}: {e}")
            errors += 1
    with _undo_lock:
        undo_store['renamer'].clear()
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/renamer/status', methods=['GET'])
def renamer_status():
    return jsonify({
        'running': automation_state['running'],
        'progress': automation_state['progress'],
        'current_product': automation_state['current_product']
    })


@app.route('/api/renamer/logs', methods=['GET'])
def renamer_logs():
    return jsonify({'logs': automation_state['logs']})


# ============================================================
# Rotas: Explorador Avancado
# ============================================================
@app.route('/api/explorer/explore', methods=['POST'])
def explorer_explore():
    global advanced_explorer
    if not advanced_explorer:
        return jsonify({'error': 'Explorador nao inicializado'}), 500
    if explorer_state['exploring']:
        return jsonify({'error': 'Exploração ja esta em andamento'}), 400
    try:
        data = request.json or {}
        path = data.get('path', '').strip()
        max_docs = min(int(data.get('max_docs', 10)), 50)

        if path in explorer_state['structure_cache']:
            cached_result = explorer_state['structure_cache'][path]
            cached_result['from_cache'] = True
            return Response(
                json.dumps({'success': True, 'data': cached_result}, default=firestore_default),
                mimetype="application/json"
            )

        result = advanced_explorer.explore_firestore_path(path, max_docs)
        return Response(
            json.dumps({'success': True, 'data': result}, default=firestore_default),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/explorer/suggestions', methods=['POST'])
def explorer_suggestions():
    global advanced_explorer
    if not advanced_explorer:
        return jsonify({'error': 'Explorador nao inicializado'}), 500
    try:
        data = request.json or {}
        partial_path = data.get('path', '').strip()
        suggestions = advanced_explorer.get_path_suggestions(partial_path)
        return Response(
            json.dumps({'success': True, 'suggestions': suggestions}, default=firestore_default),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/explorer/cache', methods=['GET'])
def explorer_cache_list():
    return Response(
        json.dumps({'success': True, 'cached_paths': list(explorer_state['structure_cache'].keys())}, default=firestore_default),
        mimetype="application/json"
    )


@app.route('/api/explorer/cache/<path:cached_path>', methods=['GET'])
def explorer_cache_get(cached_path):
    if cached_path in explorer_state['structure_cache']:
        result = explorer_state['structure_cache'][cached_path]
        result['from_cache'] = True
        return Response(
            json.dumps({'success': True, 'data': result}, default=firestore_default),
            mimetype="application/json"
        )
    return jsonify({'error': 'Caminho nao encontrado no cache'}), 404


@app.route('/api/explorer/export/<path:cached_path>', methods=['GET'])
def explorer_export(cached_path):
    if cached_path in explorer_state['structure_cache']:
        result = explorer_state['structure_cache'][cached_path]
        export_data = {
            "firestore_structure": result,
            "exported_at": datetime.now().isoformat(),
            "export_format": "firestore_structure_v1"
        }
        response = Response(
            json.dumps(export_data, default=firestore_default),
            mimetype="application/json"
        )
        response.headers['Content-Disposition'] = f'attachment; filename=firestore_structure_{cached_path.replace("/", "_")}.json'
        return response
    return jsonify({'error': 'Caminho nao encontrado no cache'}), 404


@app.route('/api/explorer/status', methods=['GET'])
def explorer_status():
    return Response(
        json.dumps({
            'exploring': explorer_state['exploring'],
            'progress': explorer_state['progress'],
            'current_path': explorer_state['current_path']
        }, default=firestore_default),
        mimetype="application/json"
    )


@app.route('/api/explorer/logs', methods=['GET'])
def explorer_logs():
    return Response(
        json.dumps({'logs': explorer_state['logs']}, default=firestore_default),
        mimetype="application/json"
    )


# ============================================================
# Rotas: Explorador Simples
# ============================================================
@app.route('/api/explorer-simple/explore', methods=['POST'])
def simple_explore():
    global simple_explorer
    if not simple_explorer:
        return jsonify({'error': 'Explorador simples nao inicializado'}), 500
    try:
        data = request.json or {}
        path = data.get('path', '').strip()
        max_docs = int(data.get('max_docs', 5))
        result = simple_explorer.explore(path, max_docs)
        result_safe = to_json_safe(result)
        return Response(json.dumps({'success': True, 'data': result_safe}, ensure_ascii=False), mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({'error': str(e)}), status=500, mimetype="application/json")


# ============================================================
# Rotas: Categorizador
# ============================================================
@app.route('/api/categorizer/prompt', methods=['GET'])
def get_cat_prompt():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    return jsonify({
        'success': True,
        'prompt': categorizer.cat_system_prompt,
        'default_prompt': ProductCategorizerAgent.DEFAULT_CAT_SYSTEM_PROMPT
    })


@app.route('/api/categorizer/prompt', methods=['POST'])
def save_cat_prompt():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    try:
        data = request.json or {}
        prompt = data.get('prompt', '').strip()
        if not prompt:
            return jsonify({'error': 'Prompt vazio'}), 400
        if categorizer.save_cat_prompt_to_firestore(prompt):
            return jsonify({'success': True, 'message': 'Prompt salvo no Firestore'})
        else:
            return jsonify({'error': 'Erro ao salvar no Firestore'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/categorizer/start', methods=['POST'])
def start_categorization():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    if categorizer_state['running']:
        return jsonify({'error': 'Categorizacao ja em execucao'}), 400
    data = request.json or {}
    estabelecimento_id = data.get('estabelecimento_id', '').strip()
    if not estabelecimento_id:
        return jsonify({'error': 'estabelecimento_id e obrigatorio'}), 400
    delay = float(data.get('delay', 0.5))
    dry_run = bool(data.get('dry_run', False))
    custom_prompt = data.get('custom_prompt', '').strip() or None

    # Sobrescreve o prompt apenas para esta execucao (restaura ao final)
    original_prompt = categorizer.cat_system_prompt
    if custom_prompt:
        categorizer.cat_system_prompt = custom_prompt

    def run():
        try:
            categorizer.run_categorization(estabelecimento_id, delay, dry_run)
        finally:
            if custom_prompt:
                categorizer.cat_system_prompt = original_prompt

    Thread(target=run, daemon=True).start()
    return jsonify({'success': True, 'message': 'Categorizacao iniciada'})


@app.route('/api/categorizer/stop', methods=['POST'])
def stop_categorization():
    if not categorizer_state['running']:
        return jsonify({'error': 'Nenhuma categorizacao em execucao'}), 400
    categorizer_state['running'] = False
    return jsonify({'success': True})


def _undo_categorizer_changes(history_key):
    """Reverte alterações de categoria/subcategoria do undo_store para a chave dada."""
    global categorizer
    if not categorizer:
        return 0, 0, 'Categorizador nao inicializado'
    with _undo_lock:
        changes = list(undo_store[history_key])
    if not changes:
        return 0, 0, 'Nenhuma alteracao para desfazer'
    reverted, errors = 0, 0
    for entry in changes:
        try:
            doc_ref = (categorizer.db.collection('estabelecimentos')
                       .document(entry['estabelecimento_id'])
                       .collection('Products')
                       .document(entry['product_id']))
            old = entry.get('old_data', {})
            doc_ref.update({
                'categoriesIds': old.get('categoriesIds', []),
                'subcategoriesIds': old.get('subcategoriesIds', []),
                'shelves': old.get('shelves', []),
                'shelvesIds': old.get('shelvesIds', []),
            })
            reverted += 1
        except Exception as e:
            logger.error(f"Undo categorizer erro {entry['product_id']}: {e}")
            errors += 1
    with _undo_lock:
        undo_store[history_key].clear()
    return reverted, errors, None


@app.route('/api/categorizer/undo-info', methods=['GET'])
def categorizer_undo_info():
    with _undo_lock:
        count = len(undo_store['categorizer'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/categorizer/undo', methods=['POST'])
def categorizer_undo():
    if categorizer_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    reverted, errors, err_msg = _undo_categorizer_changes('categorizer')
    if err_msg:
        return jsonify({'error': err_msg}), (500 if 'inicializado' in err_msg else 400)
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/categorizer-targeted/undo-info', methods=['GET'])
def categorizer_targeted_undo_info():
    with _undo_lock:
        count = len(undo_store['categorizer_targeted'])
    return jsonify({'count': count, 'available': count > 0})


@app.route('/api/categorizer-targeted/undo', methods=['POST'])
def categorizer_targeted_undo():
    if categorizer_targeted_state['running']:
        return jsonify({'error': 'Aguarde a execucao terminar antes de desfazer'}), 400
    reverted, errors, err_msg = _undo_categorizer_changes('categorizer_targeted')
    if err_msg:
        return jsonify({'error': err_msg}), (500 if 'inicializado' in err_msg else 400)
    return jsonify({'success': True, 'reverted': reverted, 'errors': errors})


@app.route('/api/categorizer/status', methods=['GET'])
def categorizer_status():
    return jsonify({
        'running': categorizer_state['running'],
        'progress': categorizer_state['progress'],
        'current_product': categorizer_state['current_product']
    })


@app.route('/api/categorizer/logs', methods=['GET'])
def categorizer_logs():
    return jsonify({'logs': categorizer_state['logs']})


@app.route('/api/categorizer/categories', methods=['GET'])
def get_categorizer_categories():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    estabelecimento_id = request.args.get('estabelecimento_id', '').strip()
    if not estabelecimento_id:
        return jsonify({'error': 'estabelecimento_id e obrigatorio'}), 400
    cats = categorizer.load_categories(estabelecimento_id)
    return jsonify({'success': True, 'categories': cats})


# ============================================================
# Rotas: Categorizador Dirigido
# ============================================================
@app.route('/api/categorizer-targeted/start', methods=['POST'])
def start_categorization_targeted():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    if categorizer_targeted_state['running']:
        return jsonify({'error': 'Ja em execucao'}), 400
    data = request.json or {}
    est_id = data.get('estabelecimento_id', '').strip()
    target_cat_id = data.get('target_category_id', '').strip()
    if not est_id or not target_cat_id:
        return jsonify({'error': 'estabelecimento_id e target_category_id sao obrigatorios'}), 400
    include_others = bool(data.get('include_others', False))
    delay = float(data.get('delay', 0.5))
    dry_run = bool(data.get('dry_run', False))

    def run():
        categorizer.run_categorization_targeted(est_id, target_cat_id, include_others, delay, dry_run)

    Thread(target=run, daemon=True).start()
    return jsonify({'success': True, 'message': 'Categorizacao dirigida iniciada'})


@app.route('/api/categorizer-targeted/stop', methods=['POST'])
def stop_categorization_targeted():
    if not categorizer_targeted_state['running']:
        return jsonify({'error': 'Nenhuma execucao em andamento'}), 400
    categorizer_targeted_state['running'] = False
    return jsonify({'success': True})


@app.route('/api/categorizer-targeted/status', methods=['GET'])
def categorizer_targeted_status():
    return jsonify({
        'running': categorizer_targeted_state['running'],
        'progress': categorizer_targeted_state['progress'],
        'current_product': categorizer_targeted_state['current_product']
    })


@app.route('/api/categorizer-targeted/logs', methods=['GET'])
def categorizer_targeted_logs_route():
    return jsonify({'logs': categorizer_targeted_state['logs']})


# ============================================================
# Rotas: Estatísticas diárias
# ============================================================
@app.route('/api/stats/daily', methods=['GET'])
def daily_stats_api():
    return jsonify({'success': True, 'today': get_today_stats(), 'all': _daily_stats_data})


# ============================================================
# WebSocket
# ============================================================
@socketio.on('connect')
def handle_connect():
    emit('renamer_status_update', {
        'running': automation_state['running'],
        'progress': automation_state['progress'],
        'current_product': automation_state['current_product']
    })
    emit('explorer_status_update', {
        'exploring': explorer_state['exploring'],
        'progress': explorer_state['progress'],
        'current_path': explorer_state['current_path']
    })
    emit('categorizer_status_update', {
        'running': categorizer_state['running'],
        'progress': categorizer_state['progress'],
        'current_product': categorizer_state['current_product']
    })
    emit('categorizer_targeted_status_update', {
        'running': categorizer_targeted_state['running'],
        'progress': categorizer_targeted_state['progress'],
        'current_product': categorizer_targeted_state['current_product']
    })
    emit('renamer_logs_update', {'logs': automation_state['logs']})
    emit('explorer_logs_update', {'logs': explorer_state['logs']})
    emit('categorizer_logs_update', {'logs': categorizer_state['logs']})
    emit('categorizer_targeted_logs_update', {'logs': categorizer_targeted_state['logs']})
    emit('daily_stats_update', get_today_stats())


@socketio.on('disconnect')
def handle_disconnect():
    pass


# ============================================================
# Inicializacao (modulo carregado por gunicorn ou diretamente)
# Roda em thread separada para que gunicorn possa fazer bind
# na porta antes do Firebase responder.
# ============================================================
_init_thread = Thread(target=init_all, daemon=True)
_init_thread.start()


# ============================================================
# Main (desenvolvimento local)
# ============================================================
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', '0') == '1'
    socketio.run(app, host='0.0.0.0', port=port, debug=debug)
