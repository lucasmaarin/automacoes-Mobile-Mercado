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
import re
import urllib.request
import urllib.error
from typing import List, Dict, Any
from datetime import datetime, timedelta
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

# Configuração e extensões extraídas para módulos separados
from config import logger, SECRET_KEY
from extensions import init_extensions, init_firebase, get_db, _reload_openai_client, _is_quota_error, emit_quota_exceeded
from utils import (to_json_safe, firestore_default, safe_sample, get_today_stats, record_daily_usage,
                   automation_state, explorer_state, categorizer_state, categorizer_targeted_state,
                   undo_store, _undo_lock)

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

# Inicializa socketio, CORS e OpenAI client (setados no módulo extensions)
init_extensions(app)
from extensions import socketio, openai_client  # disponíveis após init_extensions

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
# Estados e undo_store importados de utils.py — compartilhados com automator.py e categorizer.py


from automator import FirestoreProductAutomator


# ============================================================
# Classe: FirestoreStructureExplorer (avancado)
from explorer import FirestoreStructureExplorer, FirestoreSimpleExplorer


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


from categorizer import ProductCategorizerAgent
class ProductCategorizerAgent:
    DEFAULT_CAT_SYSTEM_PROMPT = (
        "Voce e um especialista em categorizacao de produtos de supermercado.\n"
        "Responda APENAS com o ID solicitado, sem explicacoes, sem pontuacao extra.\n"
        "\n"
        "REGRAS POR CATEGORIA (use o ID mais especifico disponivel; use o residual apenas se nenhuma outra subcategoria couber):\n"
        "\n"
        "-- Churrasco --\n"
        "pE6DrTcVmRX2LVRYmGEt (Carvao): carvao vegetal, carvao para churrasco.\n"
        "9iCNMLNxGnBulq8PjSY3 (Pao de Alho): pao de alho para churrasco.\n"
        "RaB8H9a93VNqjkFBIPmt (Kit para Churrasco): kits e acessorios para churrasco; EXCLUI tabacos e salgadinhos.\n"
        "IpeCUT0kFZoMUuJugCdJ (Mais Itens): demais itens de churrasco (tempero, grelha, etc.).\n"
        "\n"
        "-- Biscoitos --\n"
        "C3FCL5ditlkuMQnRKg3e (Agua e Sal): biscoito agua e sal.\n"
        "nu3IlKHvf0QKSpdgcc1Y (Cream Cracker): biscoito cream cracker.\n"
        "N1Eg9yvjpRjdCfpHIijM (Biscoitos Salgados): biscoitos e bolachas salgadas em geral.\n"
        "HHiFHTgXg5m0tCkLj6jx (Cookies): cookies.\n"
        "ItAcxBTHTFPS9d5KKowS (Amanteigados): biscoitos amanteigados.\n"
        "DWlp9bzlMq2Dn5CcaWB1 (Rosquinhas): rosquinhas (acucar, coco, etc.).\n"
        "PKqYem2bZaP5dA85qyJW (Biscoito de Maizena): biscoito de maizena.\n"
        "t1mFcAUUCtXyLi3IqukF (Biscoito de Nata): biscoito de nata.\n"
        "mIjFIdLZ0o1BK9W041B7 (Biscoito Doce): biscoitos doces genericos.\n"
        "AGQ23NBd67Sz4XmioIcP (Casadinho de Goiabada): casadinho e biscoito recheado de goiabada.\n"
        "0vqhZVGVFuOJw5L43V4d (Recheados): biscoitos recheados (exceto goiabada).\n"
        "6IBhORgOheeDZHWtNdmh (Wafer): wafer (chocolate, morango, baunilha).\n"
        "FZwgcpLd2YlICdXrUvfT (Biscoito de Polvilho): biscoito de polvilho.\n"
        "Hgjl65ywn4ozif05hm9R (Biscoito de Queijo): biscoito de queijo.\n"
        "ZHoNZzbqFwa8JeiPht5E (Torradas/Biscoitos): torradas empacotadas.\n"
        "9MOiAXEqxjDAZBrpu38o (Mais Biscoitos): biscoitos nao classificados acima.\n"
        "1dZRgGcyYYSKN2jZ1fkr (Variados): mistura de biscoitos variados em caixa.\n"
        "\n"
        "-- Acougue --\n"
        "BQ1OJFaCDZWVP4ycUowe: carne bovina pertencente ao Acougue.\n"
        "porco: carne suina pertencente ao Acougue.\n"
        "JjaUG8sOOfsuA8R1JThf (Cortes de Primeira): file mignon, alcatra, picanha, contrafile.\n"
        "yPwKScUgMIuUUCrAdEYl (Cortes de Segunda): coxao duro, paleta, lagarto, musculo.\n"
        "5CviHJ3oI3SfEKQrx6SM (Linha Nobre): cortes especiais e nobres.\n"
        "4VQoALxDtxtR1aI38PvJ (Aves/Acougue): frango, peru, pato inteiros ou em cortes frescos.\n"
        "cortes_de_fran (Cortes de Frango/Acougue): cortes de frango frescos do acougue.\n"
        "8rjpy4IacWLRWMJAL17V (Churrasco/Acougue): carnes preparadas para churrasco.\n"
        "ELfFMvRP2HbfCeKVixrh (Linguicas e Salsichas/Acougue): linguica fresca e salsicha do acougue.\n"
        "CMwgJ7ykB8grJ5BBZEdH (Defumados): carnes defumadas.\n"
        "Dan8MaynJfKXcycV6LZ3 (Espetinhos): espetinhos prontos.\n"
        "4rMPitDidGyGDg4FkuTb (Carnes de Sol): carne de sol, carne seca.\n"
        "0pXbfFgvCsTIyZrVAPsK (Hamburgueres/Acougue): hamburgueres artesanais.\n"
        "Yhddb08P7KeBirOaylzj (Embalados/Acougue): carnes embaladas no acougue.\n"
        "a7wUqcx8HGLcnGayQDkq (Especiais/Acougue): cortes exclusivos.\n"
        "exoticos (Exoticos): javali, cordeiro, pato, carnes exoticas.\n"
        "salgados (Salgados/Acougue): salgados do acougue.\n"
        "fUaGNueGsiBwCpBmEVfG (Mais Produtos/Acougue): residual do acougue.\n"
        "\n"
        "-- Abatedouro (categoria xNSx6EUWUnkwxXiy4jh0) --\n"
        "KBpjomnzgqWLuh6w2DWD (Peito): peito de frango embalado.\n"
        "QtHqgh24CxgX4AUgegeA (Coxa/Sobrecoxa): coxa e sobrecoxa.\n"
        "8vGdQ6ENco3O4qfn9HhJ (Asa): asa, meio da asa, coxinha da asa.\n"
        "BFHfuKpY6AGYWoDqX2HV (Files): file de frango.\n"
        "3v5DBHWf3CiZx5r9SKtK (Frango Inteiro): frango inteiro ou desossado.\n"
        "KfxJjJTowlZFTtARwcdh (Miudos): figado, coracao, moela.\n"
        "aLtsjrCXgRk8MJuK47y4 (Linguica de Frango): linguica de frango.\n"
        "xUAqzqnDcKprxgH1cOO8 (Mais Opcoes/Frango): demais itens de frango.\n"
        "\n"
        "-- Carnes Embaladas (categoria lZP2iBskINtpOVxncQtA) --\n"
        "4NqSI0qdmgRdQOzHT7GF (Carne Bovina/Carnes): carne bovina embalada (resfriada).\n"
        "LdYWFZqB5T7pTUlWJT6E (Carne Suina/Carnes): carne suina embalada.\n"
        "DEVp6SpXQp1U5KQtoOCF (Aves Congeladas): aves e cortes de frango congelados embalados.\n"
        "IHPgrxrloNfIlsv0sges (Linguica/Bacon): linguica embalada, bacon.\n"
        "wCojj4BWTuvvdhxSki9g (Salsicha): salsichas embaladas.\n"
        "aJmQ2OGi0Y6jlzerAl0L (Peixes/Frutos do Mar): peixes e frutos do mar embalados.\n"
        "Y9ANg4xm2SOxm2BFFGKa (Embalados/Carnes): carnes embaladas em geral.\n"
        "OVI7wwD4cYwHc9UiiXbM (Mais Produtos/Carnes): residual da categoria Carnes.\n"
        "\n"
        "-- Frios (categoria PcnQsRLfIHbfJju4Jt9y) --\n"
        "7w3rDlaVhlDW5cBcKTxY (Presunto/Derivado): presunto, apresuntado, fiambre.\n"
        "ukzn7WeMCizvaQF82QXZ (Mortadela/Salame): mortadela, salame, copa, pepperoni.\n"
        "N9vYlXyUeyf6Q6PKOIlr (Mais Frios): frios nao classificados acima.\n"
        "\n"
        "-- Laticinios (categoria 93OpBNqbtNLreJOvysUF) --\n"
        "57yxbAvuE7BGivr7VaAS (Leite/Laticinios): leite pasteurizado e longa vida na categoria Laticinios.\n"
        "1VcbixYgCTG5GYM5kIae (Manteiga/Margarina): manteiga e margarina.\n"
        "GqfVGeOoIbXMMV4OdU5k (Iogurte/Fermentado): iogurte, leite fermentado, bebida lactea.\n"
        "IkvMHcBZprv1zkGkhtZm (Queijo/Requeijao): queijo e requeijao.\n"
        "EsWJOMgJv5lHdX3haYfo (Achocolatados/Vitaminas): achocolatado liquido, vitamina de frutas.\n"
        "EuTwLwweukGpqKrdHtPq (A Base de Soja): leite de soja, iogurte de soja.\n"
        "qwG62CA7ZfmH9DrENyr9 (Mais Laticinios): laticinios nao classificados acima.\n"
        "\n"
        "-- Leite Longa Vida (categoria leite_longa_vida) --\n"
        "leite_integral (Integral): leite longa vida integral.\n"
        "desnatado (Desnatado): leite longa vida desnatado.\n"
        "semi_desnatado (Semi-Desnatado): leite longa vida semi-desnatado.\n"
        "zero_lactose (Zero Lactose): leite longa vida zero lactose.\n"
        "\n"
        "-- Naturais (categoria H4oUStBC1tKGbiONE0kq) --\n"
        "9D3MkgeuDpikvdYEbGGl (Granola/Aveia): granola, aveia, muesli.\n"
        "E2mNlZd1Xee5UP9UQOP7 (Linhaca): linhaca, chia e sementes similares.\n"
        "F1ZJzNIoXqmVlT1g1y54 (Mel/Derivados): mel, geleia real.\n"
        "ThSvDDUALYAr84vKR04B (Chas): cha em sache, ervas para cha.\n"
        "5u1pTw1mMQ7NS3ThNzQ3 (Mais Produtos/Cafe da Manha): residual de cafe da manha.\n"
        "\n"
        "-- Bebidas --\n"
        "0RdbVSpAFSRS6WFoPtpm (Cerveja Lata): cerveja em lata.\n"
        "DbM2IzBT4WT2AfTw0QIp (Cerveja Garrafa): cerveja em garrafa.\n"
        "3MoTA9b88ctB5C97nRBW (Refrigerantes): refrigerante em geral.\n"
        "9vKWSLbeERw7ExTuu2ZF (Energeticos): energetico (Red Bull, Monster, etc.).\n"
        "YsErdAMJliMjvS8P7PC7 (Agua/Agua de Coco): agua mineral e agua de coco na categoria Bebidas.\n"
        "ZzhgJTpqtOJn98LApa2u (Sucos/Bebidas): suco em geral na categoria Bebidas.\n"
        "Hxg4gAZ4CSnx454mL9l2 (Ice): bebida ice (smirnoff ice, etc.).\n"
        "LLZ8y0OU1AASvrZWzVdL (Vinho Seco): vinho seco.\n"
        "fSWULb0s25clpPGLNAcB (Vinhos): vinhos em geral.\n"
        "vinhos (Vinho Suave): vinho suave.\n"
        "udS91WZE3PfsBq6hAIdM (Espumantes): espumante, champagne, prosecco.\n"
        "RHS9lkuqVHVBAIZjDxVK (Cachaca): cachaca.\n"
        "C2ww9mrT9n52jiUXNtml (Vodka): vodka.\n"
        "CDLwhAVDjsm861QQAZCv (Whisky): whisky, bourbon, scotch.\n"
        "azQe5zSxbSvkCwwLUF0o (Destiladas): rum, gin, tequila, conhaque e outros destilados.\n"
        "Q7sjFk6AZWN9NGAJf2fA (Mais Bebidas): bebidas nao cobertas acima.\n"
        "\n"
        "-- Sucos e Refrescos (categoria gRZ08KY5SYQs7DHjJBDJ) --\n"
        "1xdmyCL5Uc2zpNO8mUfe (Refrigerante Lata): refrigerante em lata.\n"
        "3KUV4f5NK1U5mYmznsUy (Refrigerante Garrafa): refrigerante em garrafa.\n"
        "3eBLrMniIPDKsEgpb8sr (Suco Natural): suco natural/fresco.\n"
        "6t2afejanIRpX0HlOPSY (Suco em Garrafa): suco industrializado em garrafa.\n"
        "Qd9XnIcQd9pBVRP6iWV4 (Suco Caixa 1L): suco caixinha grande (1L).\n"
        "ZFJGH6ZM1XpWnLj5AGZV (Suco Caixinha/Lata): suco caixinha pequena ou lata.\n"
        "QmoYLCUcOVaUw5lHllYT (Suco em Po): refresco em po, suco em po.\n"
        "cdFJ2KnWwiRj0V0eItkT (Agua de Coco/Sucos): agua de coco.\n"
        "5qjH5NwzNIS2aur7k7Cn (Mais Refrescos): residual de sucos e refrescos.\n"
        "\n"
        "-- Hortifruti --\n"
        "0ZoBFIwvInk1pjMHMCdF (Frutas/Hortifruti): frutas frescas.\n"
        "legumes (Legumes): legumes e verduras.\n"
        "V7xqQj7WI90HAZZ2cPCR (Ovos): ovos de galinha, codorna.\n"
        "9OuZeU8qgrsKkWqP4WCG (Mercearia/Hortifruti): produtos hortifruti vendidos em mercearias.\n"
        "qOMtvbXjGj0UhOAJpBSd (Mais Produtos/Hortifruti): residual de hortifruti.\n"
        "\n"
        "-- FLV --\n"
        "frutas (FRUTAS/FLV): frutas frescas na categoria FLV.\n"
        "verduras (VERDURAS): verduras, legumes, folhas.\n"
        "\n"
        "-- Congelados (categoria NJBuIlFYOgXmbMRbJFsh) --\n"
        "Fc8TyAYuf2c5QXQ7Drxu (Sorvete/Acai): sorvete, gelato, acai congelado.\n"
        "JwWF0RK2LHUIQ6cL3lOW (Pizza): pizza congelada.\n"
        "LxE5hDZ63Tc8wVqc8RKs (Empanados): nuggets, frango empanado, iscas empanadas.\n"
        "XOXp60TP8q15xyFdnxUW (Lasanha): lasanha congelada.\n"
        "OqoBE2cz4IEHQSYjguOL (Salgadinhos/Congelados): coxinha, risole e salgadinhos congelados.\n"
        "Us2ejjcjGBG2kF8ECnxS (Biscoito/Pao de Queijo/Congelados): pao de queijo congelado.\n"
        "OTKghAfOKCHotE3Gi9s0 (Polpa de Frutas): polpa de fruta congelada.\n"
        "FypWNxEpFgM1Z0hRU8p8 (Mais Congelados): residual de congelados.\n"
        "\n"
        "-- Pao Embalado (categoria Q7TlMRwK1DcXybCa44PY) --\n"
        "J2udaMg64WGo2oqPrTbv (Pao de Forma): pao de forma fatiado, integral, multigraos.\n"
        "vqDt622hqpkjhu98ydrD (Hot Dog/Hamburguer): pao de hot dog e hamburguer.\n"
        "xjkzrS0aPPhoNcdYu17l (Bisnaguinhas): bisnaguinhas e paezinhos embalados.\n"
        "UG910hCJRfuIWVzCno2V (Sovado/Empacotados): paes sovados empacotados variados.\n"
        "F0KjJqgU6kKPjr8NHtTy (Mais Opcoes/Pao): demais paes embalados.\n"
        "\n"
        "-- Padaria --\n"
        "KKLiLzWhutbHWSeBvFri (Pao Frances/Recheados): pao frances, pao recheado de padaria.\n"
        "H23NaSRpvqNgGbTiJebM (Paozinho/Bisnaguinha): paozinhos e bisnaguinhas de padaria.\n"
        "FRqjgVTQJrBKFd3eqfSt (Pao Doce/Recheados): pao doce recheado de padaria.\n"
        "Mcoit0tmUtiCNtEh8ave (Paes/Padaria): paes variados de padaria.\n"
        "CTsdjUJekDG24qEEdyJ6 (Bolos/Padaria): bolos e fatias de padaria.\n"
        "B9BEsqtxrA45xgpAMbaH (Tortas): tortas salgadas e doces de padaria.\n"
        "clT4W6NnfZKgNOtG3eMc (Confeitados): doces confeitados, brigadeiros, trufas de padaria.\n"
        "hZlW6T7U0clTcpE4gof7 (Quitandas): quitandas, broa de milho, rosquinha de padaria.\n"
        "toSETvgp1QV8pq9HvNSk (Sanduiches): sanduiches prontos da padaria.\n"
        "GPKVDh11yYdK1VNKc8R1 (Chocolate/Padaria): chocolate artesanal de padaria.\n"
        "Xx2Vhb6GMUzKQeKgeO38 (Cafe/Padaria): cafe, chas e bebidas quentes da padaria.\n"
        "5yeL5rVv4NFOwGKeMmtT (Panettone): panettone.\n"
        "HoFlKyoCKf9NbiryEPCy (Mercearia/Padaria): produtos de mercearia vendidos na padaria.\n"
        "granel (Granel): produtos a granel da padaria.\n"
        "padaria_industrial (Padaria Industrial): produtos industriais de padaria.\n"
        "padaria_propria (Padaria Propria): producao propria da padaria.\n"
        "8uDM53oTLlaHcqkel8d7 (Outros/Padaria): outros itens da padaria.\n"
        "wXn0hCRIrLt7DBL7NLBy (Mais Produtos/Padaria): residual da padaria.\n"
        "\n"
        "-- Limpeza --\n"
        "M6exV5BVLqP7eoBfC3cK (Sabao em Po): sabao em po, lava-roupas em po.\n"
        "ar2fOpTDXdex3Lg7ATCa (Sabao em Barra/Liquido): sabao em barra e sabao liquido.\n"
        "YXmkCv4Tb9AYgE4rYsMe (Amaciante): amaciante de roupas.\n"
        "dJDOrXcowSIPamoMoMax (Alvejante): alvejante, agua sanitaria, cloro.\n"
        "detergente (Detergente): detergente liquido para loucas.\n"
        "QEoywjhiacithlmhnS9n (Desinfetante): desinfetante, lysoform.\n"
        "S3jUzqBSEogWZBC1V8Ra (Cera): cera para piso.\n"
        "UGM9Jd9tlMy0B6girwgX (Esponjas): esponja de limpeza.\n"
        "Bksk4oPxyMYDYlQ9FEmQ (Odorizantes): odorizante de ambiente, aromatizador.\n"
        "5TKMIrgVg5lLIW0aQX2E (Sanitarios): limpador sanitario, pedra sanitaria.\n"
        "inseticidas (Inseticidas): inseticida, repelente, mata-moscas.\n"
        "limpadores (Limpadores): limpador multiuso, limpador de cozinha.\n"
        "produtos_gerais_p_casa (Produtos Gerais Casa): produtos de limpeza gerais para casa.\n"
        "produtos_p_banheiro (Produtos para Banheiro): produtos especificos para banheiro.\n"
        "produtos_p_cozinha (Produtos para Cozinha): produtos especificos para cozinha.\n"
        "produtos_p_roupas (Produtos para Roupas): produtos para lavagem de roupas.\n"
        "0YVuyGSVBGIkQxRodAFv (Mais Produtos/Limpeza): residual da categoria Limpeza.\n"
        "\n"
        "-- Higiene Pessoal (categorias higiene e perfumaria_higiene_pessoal) --\n"
        "2aGCAlaIXyhuUFywWsQk (Shampoo): shampoo.\n"
        "bmCb8wv1HpG1BwCZ8YRk (Condicionador): condicionador de cabelo.\n"
        "DgbgvNS2t1XfbmQ2dU2e (Produtos para Cabelo): creme de pentear, finalizador, mascara capilar.\n"
        "3gWmMs6iftlYwW8Z0eSl (Desodorante): desodorante e antitranspirante.\n"
        "84XcSQ6ZUymV2DzUTuA9 (Creme Dental): creme dental e pasta de dente.\n"
        "4rlHGNJz4s0Jo68B3vyL (Escovas de Dente): escova de dente.\n"
        "ZcwgC8ccV6F5ehQ5ZfR1 (Saude Bucal): fio dental, enxaguante bucal, fixador de dentadura.\n"
        "Sti2wBBVLXD4RVTKTlfT (Barba/Depilacao): creme de barbear, lamina de barbear, creme depilatorio.\n"
        "AeEUGZmYF5qw1ven2J3H (Papel Higienico): papel higienico.\n"
        "eGslz72doMb21Qwkd1XK (Fralda Descartavel): fralda descartavel infantil.\n"
        "v95NpcFm9WiZSCSfyERR (Absorvente): absorvente feminino.\n"
        "sabonete (Sabonete): sabonete em barra e liquido.\n"
        "corpo (Corpo): hidratante corporal, creme para corpo, locao.\n"
        "3Vx5Pqg1DV8GuqmhpfQw (Linha Infantil): produtos de higiene infantil.\n"
        "FfedlcNkjCHFmEL66DW0 (Kits/Higiene): kits de higiene pessoal.\n"
        "higiene_bucal (Higiene Bucal): higiene bucal (pasta, escova, fio dental).\n"
        "produtos_femininos (Produtos Femininos): produtos femininos de higiene.\n"
        "produtos_infantis (Produtos Infantis): produtos infantis de higiene.\n"
        "produtos_masculinos (Produtos Masculinos): produtos masculinos de higiene.\n"
        "produtos_p_cabelo (Produtos para Cabelo/Perf): produtos para cabelo de perfumaria.\n"
        "produtos_p_o_corpo (Produtos para o Corpo): produtos para o corpo de perfumaria.\n"
        "CI7yNyLuVIvASZc576FC (Mais Itens/Higiene): residual de higiene pessoal.\n"
        "\n"
        "-- Graos (categoria u0Yzu9DRwcchumGYl8R4) --\n"
        "GT3sj93gQpsM05eaVuVO (Amendoim): amendoim cru, torrado e derivados.\n"
        "amYCjZDDLZE6AhuXyVKr (Canjica/Canjiquinha): canjica, canjiquinha, xerem.\n"
        "UqgxbOVGbn7ou6Ylwcyo (Milho de Pipoca/Graos): milho para pipoca da categoria Graos.\n"
        "4l4VK6pySGH5FuyRsEEC (Fava/Soja): fava, soja em grao.\n"
        "QzTvWbiPJDwekbEI7ryg (Lentilha/Grao de Bico): lentilha, grao de bico, ervilha seca.\n"
        "9yHf9IfiXk8ReERkUiJD (Mais Graos): residual de graos.\n"
        "\n"
        "-- Farinaceos --\n"
        "4KMmmZtGrCG7DCitWPhq (Farinhas de Trigo e Fermentos): farinha de trigo e fermento.\n"
        "GZAzxqi0JAb8xHvHDO8v (Farinha de Trigo): farinha de trigo especifica.\n"
        "6SBFliF5x0CC92QAzevr (Fuba): fuba, creme de milho, flocos de milho.\n"
        "farinhaMillho (Farinha de Milho): farinha de milho.\n"
        "GeUHySwfH83BV1muxDE8 (Farinhas de Milho e Mandioca): farinhas de milho e mandioca combinadas.\n"
        "BXy3GuJDraQDmXOsu7WG (Farinha de Mandioca): farinha de mandioca.\n"
        "EQaxacw2tp4ikqxkGlq3 (Polvilho/Tapioca): polvilho doce, polvilho azedo, tapioca.\n"
        "polvilhoAzedo (Polvilho Azedo): polvilho azedo.\n"
        "trigoQuibe (Trigo para Quibe): trigo para quibe.\n"
        "amendoimCanjica (Amendoim e Canjica/Far): amendoim e canjica na categoria Farinaceos.\n"
        "LeqaWDb7dUBb3Z8V8Yqj (Milho de Pipoca/Farinaceos): milho para pipoca da categoria Farinaceos.\n"
        "HLHgWIJNG8GYyrzHcBg0 (Farofas): farofa pronta e temperada.\n"
        "P5VxFglAUrffTNlwTfOs (Farofa Pronta): farofa pronta.\n"
        "PsUBkRHi6flhOYzsVZtZ (Farinha de Rosca): farinha de rosca.\n"
        "VxaF9Rq2ybd7Liq7MNVN (Amido/Mingau): amido de milho (Maisena), mingau.\n"
        "vopjiMZ0qeDHRxLY2tMX (Farinha Lactea): farinha lactea.\n"
        "H5nhvZqyvMEB7SYHlV9D (Farinhas Variadas): farinhas variadas.\n"
        "wwcNKKF8odl1RTCtDlsE (Mais Farinhas): residual de farinaceos.\n"
        "\n"
        "-- Cereais (categoria cereais, subcategorias com slug) --\n"
        "arroz (ARROZ): arroz em geral.\n"
        "feijao (FEIJAO): feijao em geral.\n"
        "acucares (ACUCARES): acucar (cristal, refinado, demerara, mascavo).\n"
        "oleos (OLEOS): oleo de soja, girassol, canola.\n"
        "graos (GRAOS/Cereais): graos dentro da categoria Cereais.\n"
        "farinaceos (FARINACEOS/Cereais): farinaceos dentro da categoria Cereais.\n"
        "sal (SAL): sal de cozinha.\n"
        "\n"
        "-- Molhos e Temperos (categoria g9NEgrhPZDKwFVKQ8ahL) --\n"
        "gltxTInDKmAQoXvUyebE (Sal/Tempero): sal temperado, sal grosso com ervas.\n"
        "av4faj0R5gLkTHax4L56 (Caldo em Cubos): caldo Knorr/Maggi em cubo, tablete, po.\n"
        "NxCHZjF7QNkfN7pFbSY9 (Saches): ketchup, maionese, molho em sache.\n"
        "KyK2QGwUkMqrUfxHcRfS (Cartela): tempero em cartela de sachês.\n"
        "XuS1HngsfAqQNepL7NwX (Parrilha): tempero tipo parrilha para churrasco.\n"
        "CatLJxtm6lkdjWSzYvpS (Pacote): tempero em pacote (alho, cebola, colorau).\n"
        "EKDxE9Ifrl7Jvluazdq2 (Molho em Frasco): molho de pimenta, shoyu, molho ingles.\n"
        "0yOrFM3UvDvBXCUEGBYy (Molho em Garrafa): ketchup, mostarda, maionese em garrafa.\n"
        "FuVblezvkEgSntEBHlom (Pote): tempero em pote (alho picado, pimenta).\n"
        "VUcqWNMLuYSjEgAT8Zmn (Creme): creme de cebola, creme de alho.\n"
        "HPNORjKYEl3QnlyCH3E1 (Especiais/Temperos): temperos especiais e gourmet.\n"
        "JApKyu2HhyxFyDDfI7qX (Mais Temperos): residual de molhos e temperos.\n"
        "\n"
        "-- Conservas --\n"
        "atomatados (ATOMATADOS): molho de tomate, extrato de tomate, tomate pelado.\n"
        "conservas_enlatados (CONSERVAS/ENLATADOS): conservas enlatadas em geral.\n"
        "batata_palha (BATATA PALHA): batata palha.\n"
        "maionese (MAIONESE): maionese.\n"
        "temperos_condimentos (TEMPEROS/CONDIMENTOS): temperos e condimentos.\n"
        "temperos_molhos (TEMPEROS/MOLHOS): temperos e molhos.\n"
        "derivados_de_carnes (DERIVADOS DE CARNES): atum, sardinha, carne seca em lata.\n"
        "\n"
        "-- Massas --\n"
        "massas_semola (MASSAS SEMOLA): macarrao semola.\n"
        "massas_c_ovos (MASSAS C/ OVOS): macarrao com ovos.\n"
        "massas_para_lasanha (MASSAS PARA LASANHA): massa para lasanha.\n"
        "macarrao_instantaneo (MACARRAO INSTANTANEO): macarrao instantaneo (Miojo).\n"
        "queijo_ralado (QUEIJO RALADO): queijo ralado e parmesao.\n"
        "\n"
        "-- Salgadinhos e Petiscos (categoria Gxl7MlJm2Xva4azNur9R) --\n"
        "7g2GSek9s9D0mpT6v6wl (Batatas): batata chips, batata frita de pacote.\n"
        "Q6yFb56pbyGdi8QSbgpp (Cebolitos/Baconzitos/Doritos): salgadinhos tipo cebolito, baconzito, doritos.\n"
        "QcE0hfRBQ4rjye8TCWxp (Cheetos/Fandangos): salgadinhos tipo cheetos, fandangos.\n"
        "8PZiSjQr4FofaSFtbnbz (Gulozitos): gulozitos e similares.\n"
        "7KgYnnqWpip4OpeQpMXI (Yokitos): yokitos e similares.\n"
        "OU7p4AuZitKBofXXmuz9 (Plinc): plinc e similares.\n"
        "MMGashoMWZcUDQHCfao4 (Pimentinhas): amendoim pimenta, amendoim japones, pimentinhas.\n"
        "AnJUikAG6VmiqNKoks09 (Pipoca Doce): pipoca doce e caramelada.\n"
        "t4rHknicXbLXyuaEW7pg (Milho Assado): milho assado, pipoca salgada de pacote.\n"
        "elkvn4iqDee9kHt1mUzD (Petiscos): petiscos em geral (castanhas, amendoim tostado).\n"
        "2LAQvqtnrMI9CaaMTKyz (Outros Chips): outros chips nao classificados acima.\n"
        "\n"
        "-- Gulodices (categoria ocbODuL4gh1SQNI9MUMn) --\n"
        "JVrlQUQrDz1z9B7v7CZz (Bala): bala em geral.\n"
        "5s5AqBMCAm1eTHzLTG6f (Chiclete): chiclete.\n"
        "DbUVfZnV8jJInyKInKIC (Pirulito): pirulito.\n"
        "431lCvsCEcL4H1aTIVrJ (Gomas): goma, jujuba.\n"
        "WXg0CLiIehrNcLzO4WEz (Pastilhas): pastilha (Halls, Dentyne).\n"
        "CyOhrPg4cYsXDekBzKrS (Mais Guloseimas): residual de guloseimas.\n"
        "\n"
        "-- Doces e Sobremesas (categoria z2Iv1xs8ve9uhaV4xgOL) --\n"
        "Cr7GdWtH1JNnMXdSCwhC (Leite Condensado): leite condensado.\n"
        "e5J05eEtPSM7r9XLLE0D (Creme de Leite): creme de leite.\n"
        "SjS3AHQTPQcVORfcAY7T (Coco/Derivados): coco ralado, leite de coco, creme de coco.\n"
        "FAyuLT11Zs0Ju7zTGPzw (Geleias): geleia de fruta.\n"
        "wDMG33utTlofsqiaKMJ8 (Doce Pastoso): doce de leite, doce de amendoim pastoso.\n"
        "Th1xGEGLzRB3bFE8Smyo (Doce Pedaco): goiabada, marmelada em pedaco.\n"
        "3dPqFvOFxeEsVUtsS8kL (Cobertura): cobertura para sorvete, calda de chocolate.\n"
        "jU77g8Z9fBh4y7kDbazP (Gelatinas): gelatina em po ou pronta.\n"
        "ZXJuh32EpDxzFOqocUEy (Gourmet/Sobremesas): sobremesas gourmet.\n"
        "OB0YqShFz1HUFmu3LZE3 (Mais Produtos/Sobremesas): residual de doces e sobremesas.\n"
        "\n"
        "-- Farmacia (categoria tBQebPhJCvz3scbEaYGu) --\n"
        "3aMmRMAtWnsa75rrqdnG (Analgesicos): dipirona, paracetamol, ibuprofeno.\n"
        "H79wk2s62mKzyWgU0pB9 (Curativos): curativo, esparadrapo, gaze, atadura.\n"
        "D5Gf5XxEUmsxZkFxq0zk (Mais Produtos/Farmacia): residual de farmacia.\n"
        "\n"
        "-- Animais (categoria TNb0sHqTOLNuvxLtLKk5) --\n"
        "ittlvWePhq2aIGPlRsZI (Alimento para Caes): racao para caes, petiscos caninos.\n"
        "4JPg09HS4LaoxKKuuS1H (Alimento para Gatos): racao para gatos, petiscos felinos.\n"
        "9rFsfInmrO8C8VwsAYF7 (Alimento para Passaros): racao e alpiste para passaros.\n"
        "JE6vmHrlR0raXPIm0Sbp (Perfumaria e Higiene Pet): shampoo pet, perfume pet.\n"
        "OF6GNcLvnDIvK6kFrUps (Utensilios Pet): comedouro, bebedouro, coleira, brinquedo pet.\n"
        "94cwJ9A7cJhH6jJngrzy (Mais Produtos/Pet): residual de pet.\n"
        "\n"
        "-- PET (categoria pet, subcategorias com slug) --\n"
        "cuidados (CUIDADOS): cuidados e higiene para animais de estimacao.\n"
        "racoes_e_aperitivos (RACOES E APERITIVOS): racoes e aperitivos para animais.\n"
        "\n"
        "-- Mercearia --\n"
        "HczoWLyYczCweXkwKm7p (Arroz/Mercearia): arroz.\n"
        "3meLa69ALn2FpA6l5wha (Feijao/Mercearia): feijao.\n"
        "MH7J73HyMe9Rn60rYHIg (Acucar/Mercearia): acucar.\n"
        "cafe (Cafe/Cappuccino): cafe em po, cappuccino, cafe soluvel.\n"
        "ECBq91eLZ3OPCCrWuS2N (Achocolatado em Po): achocolatado em po (Nescau, Ovomaltine).\n"
        "oleo (Oleo/Mercearia): oleo de cozinha.\n"
        "Cu0ygKyewwywtiqX7WsG (Azeite): azeite de oliva.\n"
        "vinagre (Vinagre): vinagre.\n"
        "adocantes (Adocantes): adocante liquido e em envelope.\n"
        "VFa8q52BYLqu9AKr4p0M (Atomatado/Mercearia): molho e extrato de tomate.\n"
        "massas (Massas/Mercearia): macarrao e massas.\n"
        "BDiywe54uqEKpJG7r6WS (Macarrao Instantaneo/Mercearia): macarrao instantaneo.\n"
        "0bZ58kb59vC4KFxDJX9h (Ketchup/Mostarda): ketchup e mostarda.\n"
        "0pNu96PvdWabZgSigPOM (Molhos Salada): molho para salada, vinagrete.\n"
        "6Gpl7D1ld94elJeIxLIM (Maionese/Mercearia): maionese.\n"
        "G1fOTdGypO0q00XaPNPF (Condimentos/Mercearia): condimentos em geral.\n"
        "3C8nAKY8C2SH4M07g7sQ (Temperos/Mercearia): temperos de mercearia.\n"
        "b88jKH4HacTpT4Vddbfh (Molhos Diversos/Mercearia): molhos variados.\n"
        "azeitona (Azeitona): azeitona.\n"
        "9FGjG8dA0Wg7CDe04hJt (Palmito e Champignon): palmito e champignon em conserva.\n"
        "uqB897hsbzJNMWJEiffN (Conservas/Mercearia): conservas em geral.\n"
        "dnYsG0uStHONv2z2bLur (Enlatados): enlatados de mercearia.\n"
        "sardinhaAtumCia (Sardinha/Atum): sardinha e atum em lata.\n"
        "XDHnsRzAxPfvYl2IQFJB (Milho Verde): milho verde em lata.\n"
        "YEaQKmNqf2nAmhun8uWX (Batata Palha/Mercearia): batata palha.\n"
        "S0G0hPEpo3S6kP4OjPUM (Chocolate/Mercearia): chocolate em barra, caixa de bombom.\n"
        "TVdzP5PNpl1tOWgyuBhl (Chocolate em Barras): chocolate em barras.\n"
        "bombons (Bombons): bombons e trufas embaladas.\n"
        "amidoDeMilho (Amido de Milho): amido de milho (Maisena).\n"
        "8NZGnQn8xnXxsKOjdLjc (Fermento/Mercearia): fermento quimico e biologico.\n"
        "VOuc2WkgBDP3kGVcULFK (Refresco em Po/Mercearia): refresco em po (Tang, Clight).\n"
        "xWPXQKAFbOQ4xdod9yG0 (Agua Mineral/Mercearia): agua mineral.\n"
        "QYMYZJoHQVJnXjqXsXmC (Filtros de Cafe): filtro de papel para cafe.\n"
        "HUijBYWWNE7xbvkxDMZd (Papel Toalha/Guardanapo): papel toalha e guardanapo (mercearia).\n"
        "DRTjw30hpraZWQgJCro5 (Sopa Pronta): sopa em caixinha, sopa instantanea.\n"
        "1F0uI4Vur8BmU9WvaeeB (Torradas/Mercearia): torradas.\n"
        "lacticinios (Laticinios/Mercearia): laticinios de mercearia.\n"
        "salgadinhos (Salgadinhos/Mercearia): salgadinhos de mercearia.\n"
        "wafer (Wafer/Mercearia): wafer de mercearia.\n"
        "EnzL0GvNbpCr8LKn2ylC (Biscoitos/Mercearia): biscoitos de mercearia.\n"
        "docesSobremesas (Doces e Sobremesas/Mercearia): doces e sobremesas de mercearia.\n"
        "nc0LHNtYTtwsfTfd6aDT (Bolos/Mercearia): bolos embalados.\n"
        "zm7BaN8gCQPuDnaO4E83 (Paes/Mercearia): paes embalados de mercearia.\n"
        "produtosNaturais (Produtos Naturais/Mercearia): produtos naturais e organicos.\n"
        "KplUqL0TuNy1LBhnHLZt (Churrasco/Mercearia): itens de churrasco de mercearia.\n"
        "9vjpwjIMSPY6gSI0IL1H (Hortifruti/Mercearia): produtos hortifruti de mercearia.\n"
        "9OuZeU8qgrsKkWqP4WCG (Hortifruti/Mercearia2): hortifruti vendido em mercearia.\n"
        "HoFlKyoCKf9NbiryEPCy (Padaria/Mercearia): produtos de padaria em mercearia.\n"
        "8yyiNGA5QAgPHmFRzBp1 (Ovo de Pascoa/Mercearia): ovo de pascoa.\n"
        "Nc32lmol8xd4XG7uTYAV (Dia a Dia): produtos do dia a dia.\n"
        "58o1isPrgis9qq17c14Z (Produtos Variados/Mercearia): variados residual de mercearia.\n"
        "S0vBLpOG7ZaocXyE5ffk (Mercearia Geral): mercearia geral.\n"
        "\n"
        "-- Frios e Laticinios (categoria frios_e_laticinios, subcategorias com slug) --\n"
        "frios (FRIOS): frios em geral.\n"
        "iogurtes_danones (IOGURTES/DANONES): iogurtes e danonetes.\n"
        "leite_pasteurizado (LEITE PASTEURIZADO): leite pasteurizado.\n"
        "manteigas (MANTEIGAS): manteigas.\n"
        "margarinas (MARGARINAS): margarinas.\n"
        "massas_e_derivados (MASSAS E DERIVADOS): massas e derivados de frios.\n"
        "requeijao (REQUEIJAO): requeijao.\n"
        "conservas_sobremesas_a_granel (CONSERVAS/SOBREMESAS A GRANEL): conservas e sobremesas a granel.\n"
        "\n"
        "-- Ilhas e Congelados (categoria ilhas_e_congelados, subcategorias com slug) --\n"
        "aperitivos (APERITIVOS): aperitivos congelados.\n"
        "cortes_bovinos (CORTES BOVINOS): cortes bovinos congelados.\n"
        "cortes_fran (CORTES FRAN): cortes de frango congelados.\n"
        "embutidos (EMBUTIDOS): embutidos (linguica, salsicha) congelados.\n"
        "frutos_do_mar (FRUTOS DO MAR): frutos do mar congelados.\n"
        "hamburguer (HAMBURGUER): hamburguer congelado.\n"
        "pratos_prontos (PRATOS PRONTOS): pratos prontos congelados (lasanha, estrogonofe).\n"
        "\n"
        "-- Matinal (categoria matinal, subcategorias com slug) --\n"
        "alimentacao_infantil (ALIMENTACAO INFANTIL): alimentacao infantil.\n"
        "bomboniere (BOMBONIERE): bomboniere, chocolates matinal.\n"
        "culinaria_doce (CULINARIA DOCE): ingredientes para culinaria doce.\n"
        "doces (DOCES/Matinal): doces em geral da secao matinal.\n"
        "preparo_p_cafe_matinal (PREPARO CAFE MATINAL): preparos para cafe da manha.\n"
        "prepparos_para_cafe_da_manha (PREPAROS CAFE DA MANHA): preparos para cafe da manha (variante).\n"
        "\n"
        "-- Utilidades --\n"
        "OftUMqjK6lP2he9MuXy1 (Calcado): sandalia, chinelo, havaianas, alpargatas, pantufas e calcados em geral.\n"
        "G5AQjgKSpVW3XE2tkmm9 (Cozinha/Utensilios): copo, taca, caneca, long drink, calice, prato, talher, garfo, faca (cutelaria), colher, assadeira, panela, tabua de corte, coador, espremedor, funil, jarra, tigela, pinca, abridor.\n"
        "5HTK02F3cSm0gsaGgzZ5 (Caixa Termica): caixa termica, bolsa termica, garrafa termica, bule termico, marmita termica, cooler, isopor.\n"
        "OWdS8XYbb9WP9GjZR7sy (Artigos de Festa): vela de aniversario, balao, bexiga, enfeite de festa, kit festa, decoracao de festa.\n"
        "D9I1Te40XnWqvLesIROy (Pilha/Bateria): pilha alcalina (AA, AAA, D, C, 9v), bateria de botao (cr2032), pilha recarregavel.\n"
        "FmVEH83EkZRoiu8TYNxj (Papelaria): caderno, papel, refil, fichario, pasta, caneta, lapis, canetinha, borracha, apontador, giz de cera, marcador, pincel atomico, fita adesiva, fita crepe, cola, grampo, clipe, envelope.\n"
        "xs5Fi1DT8rAl0SaLV4JQ (Eletrico): lampada (LED, fluorescente), extensao eletrica, fio, cabo eletrico, resistencia, interruptor, conector eletrico, conector perfurado, soquete, rabicho, chip, carregador, teia soldada.\n"
        "tomadas (Tomadas): tomada, adaptador eletrico, benjamin, filtro de linha, pino universal.\n"
        "hvf3XNctMubOv90OoOMF (Ferramentas): ferramenta manual, chave de fenda, alicate, martelo, serrote, parafuso, prego, disco de corte, disco diamantado, eletrodo, diluente de tinta, material de construcao, telha, chapa, painel, perfil metalico.\n"
        "Ghgy75J3VmAFEObHyUxX (Automotivo): oleo de moto, oleo de carro, aditivo automotivo, fluido de freio, produto para veiculo.\n"
        "D3Tezodc8OSvNS6EP6cs (Jardinagem): terra adubada, substrato, vaso de planta, regador, fertilizante, adubo, semente.\n"
        "5izG0lxD0E9eCKxvcRBW (Hidraulico): mangueira, registro, cano, conexao hidraulica, luva soldada, joelho, te hidraulico, conexao pvc.\n"
        "NzaSK77OnOij97ZECFje (Area de Servicos): rodo, vassoura, escova de limpeza, palha de aco, pano de chao, balde, luva de limpeza, prendedor de roupa, varal, corda de varal, desentupidor.\n"
        "lmcdTRQ9EHOJn9IVhXof (Mais Utilidades): tabaco, cigarro, charuto, fumo, seda de cigarro, papel de seda, piteira, isqueiro, acendedor, fosforo, palheiro, sacola plastica, saco delivery, saco freezer, embalagem, marmitex, pote, bandeja, filme plastico, bobina stretch, papel aluminio, guardanapo, canudo, vela generica (nao de aniversario), tampa descartavel, fio barbante, isca raticida, isca formiga, inseticida, repelente — residual de Utilidades."
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
            categories = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if data and data.get('isActive', True):
                    categories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                    })
            self.log_message(f"Carregadas {len(categories)} categorias ativas", "info")
            return categories
        except Exception as e:
            self.log_message(f"Erro ao carregar categorias: {e}", "error")
            return []

    def load_subcategories(self, estabelecimento_id):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductSubcategories'))
            subcategories = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if data and data.get('isActive', True):
                    subcategories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'categoryId': data.get('categoryId', ''),
                    })
            self.log_message(f"Carregadas {len(subcategories)} subcategorias ativas", "info")
            return subcategories
        except Exception as e:
            self.log_message(f"Erro ao carregar subcategorias: {e}", "error")
            return []

    def load_products(self, estabelecimento_id, only_uncategorized=False,
                      filter_category_id=None, filter_subcategory_id=None,
                      use_images=False):
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            products = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if not data or not data.get('name'):
                    continue
                cats = data.get('categoriesIds') or []
                if only_uncategorized and cats:
                    continue
                if filter_category_id and filter_category_id not in cats:
                    continue
                if filter_subcategory_id:
                    subs = data.get('subcategoriesIds') or []
                    if filter_subcategory_id not in subs:
                        continue
                image_url = None
                if use_images:
                    imgs = data.get('images') or []
                    if imgs and isinstance(imgs[0], dict):
                        image_url = imgs[0].get('fileUrl')
                products.append({'id': doc.id, 'name': data['name'], 'image_url': image_url})
            filtro = []
            if only_uncategorized:
                filtro.append('apenas sem categoria')
            if filter_category_id:
                filtro.append(f'categoria={filter_category_id}')
            if filter_subcategory_id:
                filtro.append(f'subcategoria={filter_subcategory_id}')
            desc = f" ({', '.join(filtro)})" if filtro else ''
            self.log_message(f"Carregados {len(products)} produtos{desc}", "info")

            return products
        except Exception as e:
            self.log_message(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _image_url_exists(self, url):
        """Verifica se a URL de imagem é acessível.
        Usa GET com Range: bytes=0-0 pois Firebase Storage não responde corretamente a HEAD.
        CONSERVADOR: só marca como inválida em caso de 404 definitivo.
        Para rate-limit, timeout ou outros erros, assume que a imagem existe.
        """
        MAX_ATTEMPTS = 2
        for attempt in range(MAX_ATTEMPTS):
            try:
                req = urllib.request.Request(url, headers={'Range': 'bytes=0-0'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    return resp.status in (200, 206)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return False  # definitivamente não existe
                # Rate-limit (429), 403, 500, etc. → assume que existe
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(0.5)
                    continue
                return True
            except Exception:
                # Timeout, erro de rede, etc. → assume que existe
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(0.5)
                    continue
                return True
        return True

    def _call_openai(self, prompt, max_tokens=60, content=None, system_prompt=None):
        max_retries = 5
        msg_content = content if content is not None else prompt
        sys_msg = system_prompt if system_prompt is not None else self.cat_system_prompt
        for attempt in range(max_retries):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": sys_msg},
                        {"role": "user", "content": msg_content}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.1
                )
            except _openai_module.RateLimitError as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                    emit_quota_exceeded()
                    raise
                # Throttling temporário (RPM/TPM) — aguarda e tenta novamente
                wait = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s, 4s, 8s
                self.log_message(f"Rate limit atingido, aguardando {wait:.1f}s (tentativa {attempt + 1}/{max_retries})...", "warning")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    raise
                continue
            except Exception as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados. O agente foi interrompido.", "error")
                    emit_quota_exceeded()
                raise
            break
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
        if not returned_id or not valid_items:
            return None
        valid_ids = {item['id'] for item in valid_items}
        # 1. Match exato
        if returned_id in valid_ids:
            return returned_id
        lower = returned_id.lower()
        # 2. Match exato case-insensitive
        for item in valid_items:
            if item['id'].lower() == lower:
                return item['id']
        # 3. Match parcial (IDs hex longos com prefixo extra, ex: "5G5AQjg..." → "G5AQjg...")
        for item in valid_items:
            item_lower = item['id'].lower()
            if len(item_lower) >= 8 and (item_lower in lower or lower in item_lower):
                return item['id']
        # 4. Sem match — retorna None (não assume primeira subcategoria)
        return None

    def get_category_and_subcategory(self, product_name, categories, subcategories):
        """Avalia subcategoria primeiro; a categoria é derivada da subcategoria escolhida."""
        cat_by_id = {c['id']: c for c in categories}
        subs_text = "\n".join(
            f"{s['id']}|{s['name']}|{cat_by_id.get(s['categoryId'], {}).get('name', s['categoryId'])}"
            for s in subcategories
        )
        prompt = (
            f"Produto: \"{product_name}\"\n\n"
            f"SUBCATEGORIAS (id|nome|categoria):\n{subs_text}\n\n"
            f"Responda APENAS com o id da subcategoria mais adequada para o produto.\n"
            f"Sem explicações."
        )
        try:
            raw = self._call_openai(prompt, max_tokens=60)
            sub_id = self._best_match(raw.strip(), subcategories)
            if not sub_id:
                self.log_message(f"Nao foi possivel determinar subcategoria para '{product_name}'", "warning")
                return None, None
            sub = next(s for s in subcategories if s['id'] == sub_id)
            return sub['categoryId'], sub_id
        except Exception as e:
            self.log_message(f"Erro na chamada OpenAI para '{product_name}': {e}", "error")
            return None, None

    # Sufixo de formato adicionado ao cat_system_prompt nas chamadas em batch.
    _BATCH_FORMAT_SUFFIX = (
        "\n\n---\n"
        "Para esta tarefa de batch, responda SOMENTE com um objeto JSON:\n"
        "{\"1\": \"subcategoria_id\", \"2\": \"subcategoria_id\", ...}\n"
        "A chave é o número do produto (começando em 1). Sem markdown, sem explicações."
    )

    def get_categories_batch(self, product_names, categories, subcategories, image_urls=None):
        """Avalia subcategoria primeiro em batch; a categoria é derivada da subcategoria. Retorna lista de (cat_id, sub_id)."""
        cat_by_id = {c['id']: c for c in categories}
        sub_by_id = {s['id']: s for s in subcategories}
        subs_text = "\n".join(
            f"{s['id']}|{s['name']}|{cat_by_id.get(s['categoryId'], {}).get('name', s['categoryId'])}"
            for s in subcategories
        )
        header = (
            f"SUBCATEGORIAS (id|nome|categoria):\n{subs_text}\n\n"
            f"Para cada produto abaixo, responda SOMENTE com JSON:\n"
            f"{{\"1\": \"subcategoria_id\", \"2\": \"subcategoria_id\", ...}}\n"
            f"A chave é o número do produto. Sem markdown, sem explicações.\n\n"
            f"Produtos:"
        )
        has_images = bool(image_urls and any(image_urls))
        if has_images:
            content = [{"type": "text", "text": header}]
            for i, (name, img_url) in enumerate(zip(product_names, image_urls)):
                content.append({"type": "text", "text": f"\n{i+1}. {name}"})
                if img_url:
                    content.append({"type": "image_url", "image_url": {"url": img_url, "detail": "low"}})
        else:
            numbered = "\n".join(f"{i+1}. {name}" for i, name in enumerate(product_names))
            content = None
            prompt = f"{header}\n{numbered}"
        results = [(None, None)] * len(product_names)
        numbered = "\n".join(f"{i+1}. {name}" for i, name in enumerate(product_names))
        text_only_prompt = f"{header}\n{numbered}"
        try:
            if has_images:
                raw = self._call_openai('', max_tokens=30 * len(product_names), content=content,
                                        system_prompt=self.cat_system_prompt + self._BATCH_FORMAT_SUFFIX)
            else:
                raw = self._call_openai(text_only_prompt, max_tokens=30 * len(product_names),
                                        system_prompt=self.cat_system_prompt + self._BATCH_FORMAT_SUFFIX)
        except Exception as e:
            err_str = str(e).lower()
            if has_images and ('image' in err_str or 'downloading' in err_str or '400' in err_str):
                self.log_message(f"Erro ao processar imagens do batch, reprocessando sem imagens...", "warning")
                try:
                    raw = self._call_openai(text_only_prompt, max_tokens=30 * len(product_names),
                                            system_prompt=self.cat_system_prompt + self._BATCH_FORMAT_SUFFIX)
                except Exception as e2:
                    self.log_message(f"Erro OpenAI no batch (sem imagens): {e2}", "error")
                    return results
            else:
                self.log_message(f"Erro OpenAI no batch: {e}", "error")
                return results

        def _resolve_sub(sub_id_raw):
            sub_id = self._best_match(sub_id_raw.split('|')[0].strip(), subcategories)
            if not sub_id:
                return None, None
            sub = sub_by_id.get(sub_id)
            if not sub:
                return None, None
            return sub['categoryId'], sub_id

        def _parse_batch_raw(raw_text):
            parsed = [(None, None)] * len(product_names)
            text = raw_text.strip()

            # Remove markdown code fences if present
            text = re.sub(r'```[a-z]*\n?', '', text).strip()

            # 1) Tenta JSON: {"1": "sub_id", "2": "sub_id", ...}
            json_match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
            if json_match:
                try:
                    mapping = json.loads(json_match.group())
                    for key, val in mapping.items():
                        try:
                            n = int(key) - 1
                            if n < 0 or n >= len(product_names):
                                continue
                            cat_id, sub_id = _resolve_sub(str(val))
                            if sub_id:
                                parsed[n] = (cat_id, sub_id)
                        except (ValueError, TypeError):
                            continue
                    return parsed
                except (json.JSONDecodeError, ValueError):
                    pass

            # 2) Fallback: formato numerado "N. sub_id"
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            has_numbered = any(
                line[:line.index('.')].strip().isdigit()
                for line in lines if '.' in line
            )
            if has_numbered:
                for line in lines:
                    try:
                        dot_idx = line.index('.')
                        n = int(line[:dot_idx].strip()) - 1
                        if n < 0 or n >= len(product_names):
                            continue
                        cat_id, sub_id = _resolve_sub(line[dot_idx + 1:].strip())
                        if sub_id:
                            parsed[n] = (cat_id, sub_id)
                    except (ValueError, IndexError):
                        continue
            else:
                # 3) Último recurso: sequencial (sujeito a desalinhamento)
                for i, line in enumerate(lines):
                    if i >= len(product_names):
                        break
                    cat_id, sub_id = _resolve_sub(line)
                    if sub_id:
                        parsed[i] = (cat_id, sub_id)
            return parsed

        results = _parse_batch_raw(raw)

        # Se todos os produtos falharam, loga o response e tenta novamente com prompt reforçado
        if all(r == (None, None) for r in results):
            self.log_message(
                f"Batch retornou todos nulos. Response bruto (primeiros 300 chars): {raw[:300]!r}",
                "warning"
            )
            retry_prompt = (
                f"ATENÇÃO: Responda SOMENTE com JSON puro, sem markdown:\n"
                f"{{\"1\": \"subcategoria_id\", \"2\": \"subcategoria_id\", ...}}\n\n"
                f"{text_only_prompt}"
            )
            try:
                raw2 = self._call_openai(retry_prompt, max_tokens=30 * len(product_names),
                                         system_prompt=self.cat_system_prompt + self._BATCH_FORMAT_SUFFIX)
                results = _parse_batch_raw(raw2)
                if all(r == (None, None) for r in results):
                    self.log_message(
                        f"Retry tambem falhou. Response bruto: {raw2[:300]!r}",
                        "warning"
                    )
            except Exception as e:
                self.log_message(f"Erro no retry do batch: {e}", "error")

        return results

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
        """Carrega todos os produtos com id, name, categoriesIds e subcategoriesIds."""
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
                        'categories_ids': data.get('categoriesIds', []),
                        'subcategories_ids': data.get('subcategoriesIds', []),
                    })
            self.log_message_targeted(f"Carregados {len(products)} produtos", "info")
            return products
        except Exception as e:
            self.log_message_targeted(f"Erro ao carregar produtos: {e}", "error")
            return []

    def _should_assign_to_category(self, product_name, category_name, target_subs):
        """Avalia pela subcategoria se o produto pertence à categoria."""
        subs_text = "\n".join(f"{s['id']}|{s['name']}" for s in target_subs)
        prompt = (
            f"Produto: \"{product_name}\"\n\n"
            f"Subcategorias de '{category_name}':\n{subs_text}\n\n"
            f"Se o produto pertence a '{category_name}', responda com o id da subcategoria mais adequada.\n"
            f"Se NAO pertence, responda apenas: NENHUMA"
        )
        raw = self._call_openai(prompt, max_tokens=60).strip()
        if raw.upper() == 'NENHUMA':
            return False, None
        sub_id = self._best_match(raw, target_subs)
        return (True, sub_id) if sub_id else (False, None)

    def _should_assign_batch(self, product_names, category_name, target_subs):
        """Avalia em batch pela subcategoria se cada produto pertence à categoria. Retorna lista de (fits, sub_id)."""
        subs_text = "\n".join(f"{s['id']}|{s['name']}" for s in target_subs)
        numbered = "\n".join(f"{i+1}. {name}" for i, name in enumerate(product_names))
        prompt = (
            f"Subcategorias de '{category_name}':\n{subs_text}\n\n"
            f"Para cada produto, responda com o id da subcategoria mais adequada de '{category_name}'.\n"
            f"Se o produto NAO pertence a '{category_name}', responda: N. NENHUMA\n"
            f"Formato exato, uma linha por produto:\n"
            f"N. subcategoria_id\n\n"
            f"Produtos:\n{numbered}"
        )
        results = [(False, None)] * len(product_names)
        try:
            raw = self._call_openai(prompt, max_tokens=30 * len(product_names))
        except Exception as e:
            self.log_message_targeted(f"Erro OpenAI no batch fase 2: {e}", "error")
            return results
        for line in raw.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                dot_idx = line.index('.')
                n = int(line[:dot_idx].strip()) - 1
                if n < 0 or n >= len(product_names):
                    continue
                rest = line[dot_idx + 1:].strip()
                if rest.upper() == 'NENHUMA':
                    results[n] = (False, None)
                else:
                    sub_id = self._best_match(rest, target_subs)
                    results[n] = (True, sub_id) if sub_id else (False, None)
            except (ValueError, IndexError):
                continue
        return results

    def run_categorization_targeted(self, estabelecimento_id, target_category_id,
                                     include_others=False, delay=0.5, dry_run=False,
                                     filter_subcategory_id=None):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_targeted_state['running'] = True
            categorizer_targeted_state['logs'] = []
            categorizer_targeted_state['current_product'] = None
            categorizer_targeted_state['progress'] = {
                'total': 0, 'processed': 0, 'updated': 0,
                'skipped': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            with _undo_lock:
                undo_store['categorizer_targeted'].clear()
            try:
                socketio.emit('categorizer_targeted_status_update', {
                    'running': True,
                    'progress': categorizer_targeted_state['progress'],
                    'current_product': None,
                })
                socketio.emit('categorizer_targeted_logs_update', {'logs': []})
            except Exception:
                pass

            self.log_message_targeted(f"Iniciando modo dirigido: {estabelecimento_id}", "info")
            self.log_message_targeted(f"Categoria alvo: {target_category_id}", "info")
            if filter_subcategory_id:
                self.log_message_targeted(f"Filtro de subcategoria: {filter_subcategory_id}", "info")
            if include_others:
                self.log_message_targeted("Opcao ativa: tambem avaliar produtos de outras categorias", "info")
            if dry_run:
                self.log_message_targeted("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            subcategories = self.load_subcategories(estabelecimento_id)
            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            # Fallback: categoria/subcategoria "Outros" (IDs fixos)
            outros_cat_id = 'outros'
            outros_sub_id = 'outrosOutros'

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
            if filter_subcategory_id:
                phase1 = [p for p in phase1 if filter_subcategory_id in p.get('subcategories_ids', [])]
            # Fase 2 (opcional): demais produtos → IA decide se pertencem à categoria
            phase2 = [p for p in all_products if target_category_id not in p.get('categories_ids', [])] if include_others else []
            if filter_subcategory_id and phase2:
                phase2 = [p for p in phase2 if filter_subcategory_id in p.get('subcategories_ids', [])]

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

            BATCH_SIZE = 20

            # ── Fase 1: avalia produto contra TODAS as categorias (batch) ────
            if phase1:
                batches_p1 = [phase1[s:s + BATCH_SIZE] for s in range(0, len(phase1), BATCH_SIZE)]
                self.log_message_targeted(
                    f"=== Fase 1: categorizando ({len(phase1)} produtos em lotes de {BATCH_SIZE}, 2 paralelos) ===", "info"
                )

            def _phase1_batch(args):
                batch_start, batch = args
                if not categorizer_targeted_state['running']:
                    return
                names = [p['name'] for p in batch]
                self.update_progress_targeted({'id': batch[0]['id'], 'name': names[0], 'index': batch_start + 1, 'total': total})
                try:
                    cat_results = self.get_categories_batch(names, categories, subcategories)
                except Exception as e:
                    self.log_message_targeted(f"  Erro OpenAI no batch fase 1: {e}", "error")
                    with self._lock:
                        for _ in batch:
                            _tick(False)
                    return
                for j, (product, (category_id, subcategory_id)) in enumerate(zip(batch, cat_results)):
                    if not categorizer_targeted_state['running']:
                        return
                    pid, pname = product['id'], product['name']
                    i = batch_start + j + 1
                    self.log_message_targeted(f"[{i}/{total}] {pname}", "info")
                    if not category_id or not subcategory_id:
                        if outros_cat_id:
                            reason = "categoria nao identificada" if not category_id else "subcategoria nao encontrada para a categoria"
                            self.log_message_targeted(f"  -> Outros / Outros ({reason})", "warning")
                            category_id, subcategory_id = outros_cat_id, outros_sub_id
                            cat_name, sub_name = 'Outros', 'Outros'
                        else:
                            self.log_message_targeted(f"  Erro: nao foi possivel categorizar {pid}", "error")
                            with self._lock:
                                _tick(False)
                            continue
                    else:
                        cat_name = cat_by_id.get(category_id, {}).get('name', category_id)
                        sub_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id)
                        if category_id != target_category_id:
                            self.log_message_targeted(f"  -> Reatribuido: {cat_name} / {sub_name}", "info")
                        else:
                            self.log_message_targeted(f"  -> {cat_name} / {sub_name}", "success")
                    ok = self.update_product_categories(pid, estabelecimento_id,
                                                        category_id, subcategory_id,
                                                        cat_name, sub_name, dry_run,
                                                        history_key='categorizer_targeted')
                    with self._lock:
                        _tick(ok)

            if phase1:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    list(executor.map(_phase1_batch, ((i * BATCH_SIZE, b) for i, b in enumerate(batches_p1))))

            # ── Fase 2: avalia se produto pertence à categoria (batch) ────────
            if phase2 and categorizer_targeted_state['running']:
                batches_p2 = [phase2[s:s + BATCH_SIZE] for s in range(0, len(phase2), BATCH_SIZE)]
                self.log_message_targeted(
                    f"=== Fase 2: avaliando outros produtos ({len(phase2)} em lotes de {BATCH_SIZE}, 2 paralelos) ===", "info"
                )

            def _phase2_batch(args):
                batch_start, batch = args
                if not categorizer_targeted_state['running']:
                    return
                p2_offset = len(phase1)
                names = [p['name'] for p in batch]
                self.update_progress_targeted({'id': batch[0]['id'], 'name': names[0], 'index': p2_offset + batch_start + 1, 'total': total})
                fit_results = self._should_assign_batch(names, target_cat['name'], target_subs)
                for j, (product, (fits, subcategory_id)) in enumerate(zip(batch, fit_results)):
                    if not categorizer_targeted_state['running']:
                        return
                    pid, pname = product['id'], product['name']
                    i = p2_offset + batch_start + j + 1
                    self.log_message_targeted(f"[{i}/{total}] {pname}", "info")
                    if not fits:
                        self.log_message_targeted(f"  -> Nao pertence a '{target_cat['name']}', ignorado", "info")
                        with self._lock:
                            _tick(None)
                    else:
                        subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id) if subcategory_id else ''
                        self.log_message_targeted(f"  -> {target_cat['name']} / {subcategory_name}", "success")
                        ok = self.update_product_categories(pid, estabelecimento_id,
                                                            target_category_id, subcategory_id,
                                                            target_cat['name'], subcategory_name, dry_run,
                                                            history_key='categorizer_targeted')
                        with self._lock:
                            _tick(ok)

            if phase2 and categorizer_targeted_state['running']:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    list(executor.map(_phase2_batch, ((i * BATCH_SIZE, b) for i, b in enumerate(batches_p2))))

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

    def run_categorization(self, estabelecimento_id, delay_between_products=0.5, dry_run=False,
                           only_uncategorized=False, filter_category_id=None,
                           include_mercearia=False, filter_subcategory_id=None,
                           use_images=False):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            categorizer_state['running'] = True
            categorizer_state['logs'] = []
            categorizer_state['current_product'] = None
            categorizer_state['progress'] = {
                'total': 0, 'processed': 0, 'updated': 0,
                'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            with _undo_lock:
                undo_store['categorizer'].clear()
            try:
                socketio.emit('categorizer_status_update', {
                    'running': True,
                    'progress': categorizer_state['progress'],
                    'current_product': None,
                })
                socketio.emit('categorizer_logs_update', {'logs': []})
            except Exception:
                pass

            self.log_message(f"Iniciando categorizacao para: {estabelecimento_id}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualizacao sera feita", "warning")

            categories = self.load_categories(estabelecimento_id)
            if not categories:
                self.log_message("Nenhuma categoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            if not include_mercearia:
                categories = [c for c in categories if c['id'].lower() != 'mercearia']
            if not categories:
                self.log_message("Nenhuma categoria disponivel", "warning")
                categorizer_state['running'] = False
                return False
            self.log_message(f"Categorias: {[c['id'] for c in categories]}", "info")
            if filter_subcategory_id:
                self.log_message(f"Filtro de subcategoria: {filter_subcategory_id}", "info")

            subcategories = self.load_subcategories(estabelecimento_id)
            if not subcategories:
                self.log_message("Nenhuma subcategoria encontrada", "warning")
                categorizer_state['running'] = False
                return False

            if use_images:
                self.log_message("Analise de imagens ativada", "info")
            products = self.load_products(estabelecimento_id, only_uncategorized,
                                          filter_category_id, filter_subcategory_id, use_images)
            if not products:
                self.log_message("Nenhum produto encontrado com os filtros aplicados", "warning")
                categorizer_state['running'] = False
                return False

            cat_by_id = {c['id']: c for c in categories}
            sub_by_id = {s['id']: s for s in subcategories}

            # Fallback: categoria/subcategoria "Outros" (IDs fixos)
            outros_cat_id = 'outros'
            outros_sub_id = 'outrosOutros'

            total = len(products)
            categorizer_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            self.update_progress()

            BATCH_SIZE = 20
            batches = [products[s:s + BATCH_SIZE] for s in range(0, total, BATCH_SIZE)]
            self.log_message(
                f"Processando {total} produtos em {len(batches)} lotes de {BATCH_SIZE} (2 paralelos)", "info"
            )

            def _tick_product(ok):
                if ok:
                    categorizer_state['progress']['updated'] += 1
                else:
                    categorizer_state['progress']['errors'] += 1
                categorizer_state['progress']['processed'] += 1
                categorizer_state['progress']['tokens_used'] = self.tokens_used
                categorizer_state['progress']['estimated_cost'] = self.estimated_cost

            def _cat_batch(args):
                batch_start, batch = args
                if not categorizer_state['running']:
                    return
                names = [p['name'] for p in batch]
                self.update_progress({'id': batch[0]['id'], 'name': names[0], 'index': batch_start + 1, 'total': total})

                if not categorizer_state['running']:
                    return

                image_urls = [p.get('image_url') for p in batch] if use_images else None
                cat_results = self.get_categories_batch([p['name'] for p in batch], categories, subcategories, image_urls)
                for idx, (product, (category_id, subcategory_id)) in enumerate(zip(batch, cat_results)):
                    if not categorizer_state['running']:
                        return
                    pid, pname = product['id'], product['name']
                    i = batch_start + idx + 1
                    self.log_message(f"[{i}/{total}] {pname}", "info")
                    with self._lock:
                        if not category_id or not subcategory_id:
                            if outros_cat_id:
                                reason = "categoria nao identificada" if not category_id else "subcategoria nao encontrada para a categoria"
                                self.log_message(f"  -> Outros / Outros ({reason})", "warning")
                                ok = self.update_product_categories(
                                    pid, estabelecimento_id,
                                    outros_cat_id, outros_sub_id,
                                    'Outros', 'Outros', dry_run
                                )
                                _tick_product(ok)
                            else:
                                self.log_message(f"  Erro: nao foi possivel categorizar {pid}", "error")
                                categorizer_state['progress']['errors'] += 1
                                categorizer_state['progress']['processed'] += 1
                        else:
                            category_name = cat_by_id.get(category_id, {}).get('name', category_id)
                            subcategory_name = sub_by_id.get(subcategory_id, {}).get('name', subcategory_id)
                            self.log_message(f"  -> {category_name} / {subcategory_name}", "success")
                            ok = self.update_product_categories(
                                pid, estabelecimento_id,
                                category_id, subcategory_id,
                                category_name, subcategory_name, dry_run
                            )
                            _tick_product(ok)
                        categorizer_state['progress']['tokens_used'] = self.tokens_used
                        categorizer_state['progress']['estimated_cost'] = self.estimated_cost
                    self.update_progress()

            with ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(_cat_batch, ((i * BATCH_SIZE, b) for i, b in enumerate(batches))))

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
        # Carrega credenciais de admin no cache
        try:
            _load_admin_creds()
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar credenciais do Firestore: {e}")
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

# Cache em memória das credenciais de admin — evita chamada ao Firestore em cada login
_admin_creds_cache: dict = {'user': None, 'passwd': None}


def _load_admin_creds():
    """Carrega credenciais do Firestore para o cache em memória.
    Fallback para variáveis de ambiente ADMIN_USERNAME / ADMIN_PASSWORD."""
    global _admin_creds_cache
    try:
        if db:
            doc = db.collection('Automacoes').document('admin').get()
            if doc.exists:
                data = doc.to_dict() or {}
                user = data.get('userAdmin', '').strip()
                passwd = data.get('passAdmin', '').strip()
                if user and passwd:
                    _admin_creds_cache = {'user': user, 'passwd': passwd}
                    logger.info("Credenciais de admin carregadas do Firestore")
                    return
    except Exception as e:
        logger.warning(f"Erro ao carregar credenciais do Firestore: {e}")
    # Fallback: variáveis de ambiente (desenvolvimento local)
    env_user = os.getenv('ADMIN_USERNAME', '').strip()
    env_pass = os.getenv('ADMIN_PASSWORD', '').strip()
    if env_user and env_pass:
        _admin_creds_cache = {'user': env_user, 'passwd': env_pass}
        logger.info("Credenciais de admin carregadas do .env (fallback)")
    else:
        _admin_creds_cache = {'user': None, 'passwd': None}


def get_admin_credentials():
    """Retorna credenciais de admin do cache em memória (sem chamada ao Firestore)."""
    return _admin_creds_cache.get('user'), _admin_creds_cache.get('passwd')

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
        if admin_user is None:
            error = 'Serviço indisponível. Tente novamente em instantes.'
        elif username == admin_user and password == admin_pass:
            remember = request.form.get('remember') == 'on'
            session.permanent = remember
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
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
        _admin_creds_cache['user'] = new_user
        _admin_creds_cache['passwd'] = new_pass
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
# Estabelecimentos
# ============================================================
_DEFAULT_ESTABELECIMENTOS = [
    {'id': 'estabelecimento-teste', 'name': 'Estabelecimento Teste', 'default': True},
    {'id': 'jQQjHTCc2zW1tuZMQzGF', 'name': 'Zero Grau', 'default': True},
]


def _get_all_establishment_ids():
    """Retorna todos os IDs de estabelecimentos (padrão + extras salvos no Firestore)."""
    result = [e['id'] for e in _DEFAULT_ESTABELECIMENTOS]
    seen = set(result)
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                for e in (doc.to_dict() or {}).get('estabelecimentos', []):
                    eid = e.get('id', '').strip()
                    if eid and eid not in seen:
                        result.append(eid)
                        seen.add(eid)
    except Exception:
        pass
    return result


def _explore_wildcard(path, max_docs, explorer):
    """Busca um caminho com * em todos os estabelecimentos e mescla sem repetição.
    Suporta qualquer subcoleção: ex. 'estabelecimentos/*/ProductSubcategories'.
    """
    estab_ids = _get_all_establishment_ids()

    def fetch_one(eid):
        real_path = path.replace('*', eid, 1)
        try:
            # Em modo wildcard busca sem limite para não perder documentos
            return eid, explorer.explore_firestore_path(real_path, 9999)
        except Exception as e:
            return eid, {'error': str(e)}

    merged_docs = {}   # doc_id -> primeiro doc encontrado (sem repetição)
    estabs_ok = []
    estabs_err = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        for eid, result in ex.map(fetch_one, estab_ids):
            if isinstance(result, dict) and 'error' in result:
                estabs_err.append(eid)
                continue
            estabs_ok.append(eid)
            for doc in result.get('documents', []):
                doc_id = doc.get('id')
                if doc_id and doc_id not in merged_docs:
                    merged_docs[doc_id] = doc

    docs = list(merged_docs.values())
    return {
        'type': 'collection_all',
        'path': path,
        'name': path.split('/')[-1],
        'establishments_queried': len(estabs_ok),
        'establishments_failed': estabs_err,
        'total_unique': len(docs),
        'count': len(docs),
        'documents': docs,
    }


@app.route('/api/settings/estabelecimentos', methods=['GET'])
def get_estabelecimentos():
    result = [dict(e) for e in _DEFAULT_ESTABELECIMENTOS]
    default_ids = {e['id'] for e in _DEFAULT_ESTABELECIMENTOS}
    try:
        if db:
            doc = db.collection('Automacoes').document('config').get()
            if doc.exists:
                extras = (doc.to_dict() or {}).get('estabelecimentos', [])
                for e in extras:
                    if e.get('id') and e['id'] not in default_ids:
                        result.append({'id': e['id'], 'name': e.get('name', e['id']), 'default': False})
    except Exception as e:
        logger.warning(f'Erro ao carregar estabelecimentos: {e}')
    return jsonify({'success': True, 'estabelecimentos': result})


@app.route('/api/settings/estabelecimentos', methods=['POST'])
def add_estabelecimento():
    data = request.get_json() or {}
    est_id = data.get('id', '').strip()
    name = data.get('name', '').strip()
    if not est_id or not name:
        return jsonify({'success': False, 'error': 'ID e nome sao obrigatorios'}), 400
    protected = {e['id'] for e in _DEFAULT_ESTABELECIMENTOS}
    if est_id in protected:
        return jsonify({'success': False, 'error': 'Esse ID ja e um estabelecimento padrao'}), 400
    try:
        doc_ref = db.collection('Automacoes').document('config')
        doc = doc_ref.get()
        extras = (doc.to_dict() or {}).get('estabelecimentos', []) if doc.exists else []
        if any(e.get('id') == est_id for e in extras):
            return jsonify({'success': False, 'error': 'Estabelecimento ja cadastrado'}), 400
        extras.append({'id': est_id, 'name': name})
        doc_ref.set({'estabelecimentos': extras, 'updated_at': datetime.now().isoformat()}, merge=True)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f'Erro ao adicionar estabelecimento: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/estabelecimentos/<est_id>', methods=['DELETE'])
def delete_estabelecimento(est_id):
    protected = {e['id'] for e in _DEFAULT_ESTABELECIMENTOS}
    if est_id in protected:
        return jsonify({'success': False, 'error': 'Nao e possivel remover estabelecimentos padrao'}), 400
    try:
        doc_ref = db.collection('Automacoes').document('config')
        doc = doc_ref.get()
        extras = (doc.to_dict() or {}).get('estabelecimentos', []) if doc.exists else []
        extras = [e for e in extras if e.get('id') != est_id]
        doc_ref.set({'estabelecimentos': extras, 'updated_at': datetime.now().isoformat()}, merge=True)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f'Erro ao remover estabelecimento: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# Rotas: Pagina principal
# ============================================================
@app.route('/')
def index():
    return render_template("index.html")


@app.route('/relatorio')
def relatorio():
    return render_template("relatorio.html")


@app.route('/api/report/categorizer', methods=['GET'])
def categorizer_report():
    est_id = request.args.get('estabelecimento_id', 'estabelecimento-teste')
    if not db:
        return jsonify({'error': 'Firebase nao inicializado'}), 500
    try:
        col_ref = (db.collection('estabelecimentos')
                   .document(est_id)
                   .collection('Products'))
        products = []
        for doc in col_ref.stream():
            data = doc.to_dict()
            if not data or not data.get('name'):
                continue
            cats = data.get('categoriesIds') or []
            shelves = data.get('shelves') or []
            cat_name = shelves[0].get('categoryName', '') if shelves else ''
            sub_name = shelves[0].get('subcategoryName', '') if shelves else ''
            products.append({
                'id': doc.id,
                'name': data.get('name', ''),
                'has_category': bool(cats),
                'category_name': cat_name,
                'subcategory_name': sub_name,
            })
        error_logs = [
            log for log in categorizer_state.get('logs', [])
            if log.get('level') == 'error'
        ]
        products.sort(key=lambda p: (p['has_category'], p['name'].lower()))
        return jsonify({
            'success': True,
            'produtos': products,
            'total': len(products),
            'with_category': sum(1 for p in products if p['has_category']),
            'without_category': sum(1 for p in products if not p['has_category']),
            'error_logs': error_logs,
            'estabelecimento_id': est_id,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/report/renamer', methods=['GET'])
def renamer_report():
    """Relatório de produtos renomeados (últimas mudanças do undo_store)"""
    est_id = request.args.get('estabelecimento_id', 'estabelecimento-teste')
    if not db:
        return jsonify({'success': False, 'error': 'Firebase nao inicializado'}), 500
    try:
        # Buscar todos os produtos
        col_ref = (db.collection('estabelecimentos')
                   .document(est_id)
                   .collection('Products'))
        all_products = []
        products_by_id = {}
        for doc in col_ref.stream():
            data = doc.to_dict()
            if not data or not data.get('name'):
                continue
            all_products.append({
                'id': doc.id,
                'name': data.get('name', ''),
            })
            products_by_id[doc.id] = data.get('name', '')
        
        # Extrair histórico de mudanças do undo_store (últimas renomeações)
        with _undo_lock:
            changes = [c for c in undo_store['renamer'] if c.get('estabelecimento_id') == est_id]
        
        # Construir lista de produtos renomeados com before/after
        renamed_products = []
        for change in changes:
            product_id = change.get('product_id', '')
            old_name = change.get('old_name', '')
            new_name = change.get('new_name', '')
            renamed_products.append({
                'id': product_id,
                'old_name': old_name,
                'new_name': new_name,
                'renamed': bool(old_name and new_name and old_name != new_name),
                'timestamp': change.get('timestamp', ''),
            })
        
        # Erros armazenados no automation_state
        errors_list = automation_state.get('renamer_errors', [])
        
        # Estatísticas
        total_products = len(all_products)
        renamed_count = len(renamed_products)
        
        return jsonify({
            'success': True,
            'produtos': renamed_products,
            'total_products': total_products,
            'renamed_count': renamed_count,
            'errors': errors_list,
            'estabelecimento_id': est_id,
        })
    except Exception as e:
        logger.error(f'Erro em /api/report/renamer: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500



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
        filter_subcategory_id = data.get('filter_subcategory_id', '').strip() or None
        use_images = bool(data.get('use_images', False))

        if not categories:
            return jsonify({'error': 'Selecione ao menos uma categoria'}), 400


        def run_thread():
            automator.run_automation(estabelecimento_id, categories, delay, dry_run, custom_prompt, filter_subcategory_id, use_images)

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
    try:
        socketio.emit('renamer_status_update', {
            'running': False,
            'progress': automation_state['progress'],
            'current_product': automation_state['current_product'],
        })
    except Exception:
        pass
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
        max_docs = min(int(data.get('max_docs', 10)), 200)

        if path in explorer_state['structure_cache']:
            cached_result = explorer_state['structure_cache'][path]
            cached_result['from_cache'] = True
            return Response(
                json.dumps({'success': True, 'data': cached_result}, default=firestore_default),
                mimetype="application/json"
            )

        if '*' in path:
            result = _explore_wildcard(path, max_docs, advanced_explorer)
        else:
            result = advanced_explorer.explore_firestore_path(path, max_docs)

        explorer_state['structure_cache'][path] = result
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
        max_docs = min(int(data.get('max_docs', 5)), 200)

        if '*' in path:
            # Adapta FirestoreSimpleExplorer para o wildcard:
            # wraps simple_explorer.explore() num objeto compatível com _explore_wildcard
            class _SimpleAsAdvanced:
                def __init__(self, s): self._s = s
                def explore_firestore_path(self, p, m):
                    r = self._s.explore(p, m)
                    # Normaliza para o formato {documents: [...]}
                    docs = r.get('documents') or ([r['data']] if 'data' in r else [])
                    return {'documents': docs}
            result = _explore_wildcard(path, max_docs, _SimpleAsAdvanced(simple_explorer))
        else:
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


@app.route('/api/categorizer/prompt/reset', methods=['POST'])
def reset_cat_prompt():
    """Restaura o prompt do Firestore para o DEFAULT_CAT_SYSTEM_PROMPT do codigo."""
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    try:
        default = ProductCategorizerAgent.DEFAULT_CAT_SYSTEM_PROMPT
        if categorizer.save_cat_prompt_to_firestore(default):
            return jsonify({'success': True, 'message': 'Prompt restaurado para o padrao do codigo'})
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
    only_uncategorized = bool(data.get('only_uncategorized', False))
    filter_category_id = data.get('filter_category_id', '').strip() or None
    filter_subcategory_id = data.get('filter_subcategory_id', '').strip() or None
    include_mercearia = bool(data.get('include_mercearia', False))
    use_images = bool(data.get('use_images', False))
    custom_prompt = data.get('custom_prompt', '').strip() or None

    # Sobrescreve o prompt apenas para esta execucao (restaura ao final)
    original_prompt = categorizer.cat_system_prompt
    if custom_prompt:
        categorizer.cat_system_prompt = custom_prompt

    def run():
        try:
            categorizer.run_categorization(
                estabelecimento_id, delay, dry_run,
                only_uncategorized=only_uncategorized,
                filter_category_id=filter_category_id,
                include_mercearia=include_mercearia,
                filter_subcategory_id=filter_subcategory_id,
                use_images=use_images,
            )
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


@app.route('/api/subcategories', methods=['GET'])
def get_subcategories():
    global categorizer
    if not categorizer:
        return jsonify({'error': 'Categorizador nao inicializado'}), 500
    estabelecimento_id = request.args.get('estabelecimento_id', '').strip()
    if not estabelecimento_id:
        return jsonify({'error': 'estabelecimento_id e obrigatorio'}), 400
    category_id = request.args.get('category_id', '').strip() or None
    subs = categorizer.load_subcategories(estabelecimento_id)
    if category_id:
        subs = [s for s in subs if s.get('categoryId') == category_id]
    return jsonify({'success': True, 'subcategories': subs})


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
    filter_subcategory_id = data.get('filter_subcategory_id', '').strip() or None

    def run():
        categorizer.run_categorization_targeted(est_id, target_cat_id, include_others, delay, dry_run, filter_subcategory_id)

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
    # Se nao ha nada rodando, zera os estados para a pagina iniciar limpa
    if not automation_state['running']:
        automation_state['progress'] = {
            'total': 0, 'processed': 0, 'updated': 0,
            'unchanged': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
        }
        automation_state['current_product'] = None
        automation_state['logs'] = []
    if not categorizer_state['running']:
        categorizer_state['progress'] = {
            'total': 0, 'processed': 0, 'updated': 0,
            'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
        }
        categorizer_state['current_product'] = None
        categorizer_state['logs'] = []
    if not categorizer_targeted_state['running']:
        categorizer_targeted_state['progress'] = {
            'total': 0, 'processed': 0, 'updated': 0,
            'skipped': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
        }
        categorizer_targeted_state['current_product'] = None
        categorizer_targeted_state['logs'] = []

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
_init_thread.join(timeout=5)  # Permite no maximo 5 segundos para inicializar
if _init_thread.is_alive():
    logger.warning("Inicializacao ainda em andamento (timeout). Servidor funcionara sem Firestore completo.")


# ============================================================
# Main (desenvolvimento local)
# ============================================================
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', '0') == '1'
    socketio.run(app, host='0.0.0.0', port=port, debug=debug, allow_unsafe_werkzeug=True)