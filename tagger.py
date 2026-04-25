import threading
import time
import re
import json
import base64
import urllib.request
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import openai as _openai_module
import extensions as _ext
from utils import record_daily_usage, tagger_state
from config import logger


def _parse_rate_limit_wait(error: Exception, default: float = 5.0) -> float:
    m = re.search(r'try again in (\d+\.?\d*)s', str(error), re.IGNORECASE)
    if m:
        return float(m.group(1)) + 0.5
    return default


# Tipos válidos de embalagem que a IA pode retornar
_PACKAGING_TYPES = {
    'garrafa', 'lata', 'caixinha', 'caixa', 'sache', 'sachê', 'pote',
    'copo', 'bandeja', 'vidro', 'tetra pak', 'tetrapak', 'envelope',
    'bisnaga', 'barrica', 'frasco', 'longa vida', 'longavida', 'embalagem',
    'saco', 'saquinho'
}

# Normaliza variações de embalagem para um tag canônico
_PACKAGING_NORMALIZE = {
    'tetrapak': 'caixinha',
    'tetra pak': 'caixinha',
    'longavida': 'caixinha',
    'longa vida': 'caixinha',
    'sachê': 'sache',
}


def _quantity_tags_from_name(name: str) -> list:
    """Extrai tags de quantidade/medida do nome. Ex: 50g, 1L, 500ml, 1kg."""
    # Normaliza gr → g e lt → L
    normalized = re.sub(r'(\d)\s*gr\b', r'\1g', name, flags=re.IGNORECASE)
    normalized = re.sub(r'(\d)\s*lt\b', r'\1L', normalized, flags=re.IGNORECASE)

    pattern = r'\d+(?:[.,]\d+)?\s*(?:g|kg|ml|l|oz|un|unid)\b'
    tags = []
    seen = set()
    for m in re.finditer(pattern, normalized, flags=re.IGNORECASE):
        token = m.group(0).strip().replace(' ', '').replace(',', '.')
        # Mantém L maiúsculo para litros
        if token.lower().endswith('l') and not token.lower().endswith('ml'):
            token = token[:-1] + 'L'
        tag = '#' + token.lower() if not token.endswith('L') else '#' + token
        if tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return tags


def _filter_packaging_tags(tags: list, product_name: str) -> list:
    """Remove #caixa e #caixinha de produtos que não são leite."""
    name_lower = product_name.lower().strip()
    leite_allowed = (
        name_lower.startswith('leite') or
        'creme de leite' in name_lower or
        'leite condensado' in name_lower
    )
    if leite_allowed:
        return tags
    return [t for t in tags if t not in ('#caixa', '#caixinha')]


class ProductTagger:
    # Com imagens: 5 produtos por chamada, 3 workers
    IMAGE_BATCH_SIZE = 5
    MAX_WORKERS = 3

    INPUT_COST  = 0.00015 / 1000   # gpt-4o-mini
    OUTPUT_COST = 0.0006  / 1000

    # Tags de características válidas — a IA só pode usar estas
    _CHARACTERISTICS_TAGS = [
        # Composição / Origem
        'derivado_do_leite', 'origem_animal', 'origem_vegetal', 'integral',
        'desnatado', 'organico', 'natural', 'ultraprocessado',
        # Tipos de produto
        'massa', 'creme_de_leite',
        # Estilo de vida / Dieta
        'vegano', 'vegetariano', 'fitness', 'saudavel', 'low_carb',
        'sem_gluten', 'sem_lactose', 'sem_acucar', 'diet', 'light',
        # Restrições / Alergênicos
        'contem_lactose', 'contem_gluten', 'contem_acucar', 'contem_alcool',
        'contem_cafeina', 'contem_amendoim', 'contem_soja',
        # Tipo Funcional
        'alcoolico', 'nao_alcoolico', 'sem_alcool', '0_alcool',
        'energetico', 'hidratante', 'estimulante', 'calmante',
        # Uso / Contexto
        'limpeza', 'higiene_pessoal', 'consumo_imediato', 'preparo_culinario',
        'infantil', 'pet',
        # Risco / Logística
        'inflamavel', 'perecivel', 'congelado', 'refrigerado', 'fragil',
    ]

    # Tags extras disponíveis apenas no modo combinado (características + imagens)
    _CHARACTERISTICS_IMAGES_EXTRA_TAGS = [
        'racao_cachorro', 'cachorro', 'racao_gato', 'gato',
    ]

    # Mapeamento para exibição: tags que não seguem o padrão simples de substituir _ por espaço
    _CHAR_TAG_FORMAT = {
        'racao_cachorro': 'ração para cachorro',
        'racao_gato': 'ração para gato',
        'creme_de_leite': 'creme de leite',
        '0_alcool': '0 alcool',
    }

    _CHARACTERISTICS_PROMPT = (
        "Voce recebera uma lista de nomes de produtos de supermercado.\n"
        "Para cada produto, retorne APENAS as tags de caracteristicas que voce tem CERTEZA ABSOLUTA que se aplicam, "
        "baseando-se exclusivamente no nome do produto.\n"
        "Se tiver qualquer duvida sobre uma tag, NAO a inclua.\n"
        "Use SOMENTE tags da lista fornecida — nao invente outras.\n"
        "Responda SOMENTE com JSON: {\"1\": [\"tag1\", \"tag2\"], \"2\": [], ...}\n"
        "Se nenhuma tag se aplica com certeza, retorne lista vazia para aquele produto.\n"
        "Sem markdown, sem explicacoes.\n\n"
        "REGRAS ESPECIAIS OBRIGATORIAS:\n"
        "- Chocolates, achocolatados, cacau: use derivado_do_leite — NUNCA origem_animal\n"
        "- Espaguete, macarrao, massa, penne, fusilli, talharim e similares: use massa — NUNCA origem_animal\n"
        "- Creme de leite: inclua creme_de_leite alem das demais tags\n"
        "- Bebidas sem alcool (agua, suco, refrigerante, cha, isotonico, etc): use SEMPRE as 3 tags juntas: nao_alcoolico, sem_alcool, 0_alcool\n\n"
        "TAGS VALIDAS:\n"
        "Composicao/Origem: derivado_do_leite, origem_animal, origem_vegetal, integral, desnatado, organico, natural, ultraprocessado\n"
        "Tipos: massa, creme_de_leite\n"
        "Dieta: vegano, vegetariano, fitness, saudavel, low_carb, sem_gluten, sem_lactose, sem_acucar, diet, light\n"
        "Alergenos: contem_lactose, contem_gluten, contem_acucar, contem_alcool, contem_cafeina, contem_amendoim, contem_soja\n"
        "Funcional: alcoolico, nao_alcoolico, sem_alcool, 0_alcool, energetico, hidratante, estimulante, calmante\n"
        "Uso: limpeza, higiene_pessoal, consumo_imediato, preparo_culinario, infantil, pet\n"
        "Logistica: inflamavel, perecivel, congelado, refrigerado, fragil\n"
    )

    _CHARACTERISTICS_IMAGES_PROMPT = (
        "Voce recebera o nome e a foto de cada produto de supermercado.\n"
        "Retorne APENAS as tags de caracteristicas que voce tem CERTEZA ABSOLUTA que se aplicam.\n"
        "Use o nome como base principal. Use a foto como apoio, especialmente para identificar racoes pet.\n"
        "Se tiver qualquer duvida sobre uma tag, NAO a inclua.\n"
        "Use SOMENTE tags da lista fornecida — nao invente outras.\n"
        "Responda SOMENTE com JSON: {\"1\": [\"tag1\", \"tag2\"], \"2\": [], ...}\n"
        "Se nenhuma tag se aplica com certeza, retorne lista vazia.\n"
        "Sem markdown, sem explicacoes.\n\n"
        "REGRAS ESPECIAIS OBRIGATORIAS:\n"
        "- Chocolates, achocolatados, cacau: use derivado_do_leite — NUNCA origem_animal\n"
        "- Espaguete, macarrao, massa, penne, fusilli, talharim e similares: use massa — NUNCA origem_animal\n"
        "- Creme de leite: inclua creme_de_leite alem das demais tags\n"
        "- Bebidas sem alcool (agua, suco, refrigerante, cha, isotonico, etc): use SEMPRE as 3 tags juntas: nao_alcoolico, sem_alcool, 0_alcool\n"
        "- Racao para cachorro (pela foto da embalagem): use racao_cachorro e cachorro\n"
        "- Racao para gato (pela foto da embalagem): use racao_gato e gato\n\n"
        "TAGS VALIDAS:\n"
        "Composicao/Origem: derivado_do_leite, origem_animal, origem_vegetal, integral, desnatado, organico, natural, ultraprocessado\n"
        "Tipos: massa, creme_de_leite\n"
        "Dieta: vegano, vegetariano, fitness, saudavel, low_carb, sem_gluten, sem_lactose, sem_acucar, diet, light\n"
        "Alergenos: contem_lactose, contem_gluten, contem_acucar, contem_alcool, contem_cafeina, contem_amendoim, contem_soja\n"
        "Funcional: alcoolico, nao_alcoolico, sem_alcool, 0_alcool, energetico, hidratante, estimulante, calmante\n"
        "Uso: limpeza, higiene_pessoal, consumo_imediato, preparo_culinario, infantil, pet\n"
        "Logistica: inflamavel, perecivel, congelado, refrigerado, fragil\n"
        "Pet food: racao_cachorro, cachorro, racao_gato, gato\n"
    )

    _BRANDS_PROMPT = (
        "Voce recebera o nome e a foto de cada produto de supermercado.\n"
        "Identifique a MARCA do produto — use a foto como fonte principal e o nome como apoio.\n"
        "Retorne a tag de marca em minusculas, sem acentos, sem espacos. Ex: #nissin, #nestle, #barilla.\n"
        "Se nao for possivel identificar a marca com certeza, retorne null.\n"
        "Responda com JSON: {\"1\": \"#marca\", \"2\": null, ...}\n"
        "Sem markdown, sem explicacoes."
    )

    _BRANDS_NAME_FALLBACK_PROMPT = (
        "Voce recebera nomes de produtos de supermercado.\n"
        "Identifique a MARCA do produto a partir do nome — geralmente e o primeiro token reconhecivel como marca comercial.\n"
        "Retorne a tag de marca em minusculas, sem acentos, sem espacos. Ex: #nissin, #nestle, #barilla.\n"
        "Se nao for possivel identificar a marca com certeza pelo nome, retorne null.\n"
        "Nao confunda descricao do produto com marca (ex: 'Agua Mineral' nao tem marca identificavel).\n"
        "Responda com JSON: {\"1\": \"#marca\", \"2\": null, ...}\n"
        "Sem markdown, sem explicacoes."
    )

    _PACKAGING_PROMPT = (
        "Voce recebera o nome e a foto de cada produto de supermercado.\n"
        "Sua UNICA tarefa: identificar o TIPO DE EMBALAGEM do produto pela foto.\n"
        "Nao gere nenhuma outra tag — apenas a embalagem.\n\n"
        "Tipos validos de embalagem:\n"
        "#garrafa — qualquer garrafa (plastico ou vidro): agua, refrigerante, oleo, molho, etc.\n"
        "#lata — lata de metal\n"
        "#pote — pote de plastico ou vidro\n"
        "#saco — saco plastico grande\n"
        "#saquinho — saquinho pequeno\n"
        "#frasco — frasco (higiene, medicamento, condimento)\n"
        "#bisnaga — bisnaga (maionese, creme dental, etc.)\n"
        "#bandeja — bandeja (carnes, frios, hortifruti)\n"
        "#vidro — vidro (conservas, geleia, molho)\n"
        "#copo — copo plastico ou de vidro\n"
        "#sache — sache individual\n"
        "#envelope — envelope (tempero, refresco em po)\n"
        "#caixinha — APENAS leite longa vida e leite em caixinha\n"
        "#caixa — APENAS leite longa vida\n\n"
        "EXCECOES — NAO defina tag de embalagem para: ovos de pascoa, biscoitos, bolachas, cookies.\n"
        "[OUTROS PRODUTOS COM CAIXA/CAIXINHA — adicione aqui futuramente]\n\n"
        "Se nao for possivel identificar a embalagem com certeza pela foto, retorne null.\n"
        "Se a foto estiver ilegivel, retorne null.\n\n"
        "Formato: JSON onde a chave e o numero do produto e o valor e a tag de embalagem (string) ou null.\n"
        "Nenhum texto adicional. Nenhuma explicacao.\n"
        'Exemplo: {"1": "#garrafa", "2": "#lata", "3": null, "4": "#pote"}'
    )

    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0.0
        self._lock = threading.Lock()
        self._user_additions = self._load_user_additions()

    def _load_user_additions(self) -> str:
        try:
            doc = self.db.collection('Automacoes').document('taggeador').get()
            if doc.exists:
                additions = (doc.to_dict() or {}).get('user_additions', '')
                if additions:
                    logger.info("Instrucoes adicionais do tagger carregadas do Firestore")
                    return additions
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar instrucoes adicionais do tagger: {e}")
        return ''

    def save_user_additions_to_firestore(self, additions: str) -> bool:
        try:
            self.db.collection('Automacoes').document('taggeador').set({
                'user_additions': additions,
                'updated_at': datetime.now().isoformat(),
            }, merge=True)
            self._user_additions = additions
            logger.info("Instrucoes adicionais do tagger salvas no Firestore")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar instrucoes adicionais do tagger: {e}")
            return False

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def log_message(self, message: str, level: str = 'info'):
        entry = {
            'timestamp': datetime.now().strftime("%H:%M:%S"),
            'message': message,
            'level': level
        }
        with self._lock:
            tagger_state['logs'].append(entry)
            if len(tagger_state['logs']) > 500:
                tagger_state['logs'] = tagger_state['logs'][-500:]
        logger.info(f"[TAGGER] {message}")
        try:
            _ext.socketio.emit('tagger_log_update', entry)
        except Exception as e:
            logger.error(f"[TAGGER] emit falhou: {e}")

    def _emit_progress(self):
        try:
            _ext.socketio.emit('tagger_progress_update', {
                'progress': tagger_state['progress'],
                'current_product': tagger_state['current_product']
            })
        except Exception:
            pass

    def _update_progress(self, updated=0, skipped=0, errors=0):
        with self._lock:
            tagger_state['progress']['updated'] += updated
            tagger_state['progress']['skipped'] += skipped
            tagger_state['progress']['errors'] += errors
            tagger_state['progress']['processed'] += updated + skipped + errors
            tagger_state['progress']['tokens_used'] = self.tokens_used
            tagger_state['progress']['estimated_cost'] = round(self.estimated_cost, 6)
        self._emit_progress()

    # ------------------------------------------------------------------
    # OpenAI helpers
    # ------------------------------------------------------------------

    def _record_usage(self, usage):
        with self._lock:
            self.tokens_used += usage.total_tokens
            cost = usage.prompt_tokens * self.INPUT_COST + usage.completion_tokens * self.OUTPUT_COST
            self.estimated_cost += cost
            record_daily_usage(usage.total_tokens, cost)

    def _clean_json(self, raw: str) -> str:
        raw = raw.strip()
        if raw.startswith('```'):
            raw = re.sub(r'^```[^\n]*\n?', '', raw)
            raw = re.sub(r'\n?```$', '', raw).strip()
        return raw

    def _fetch_image_base64(self, url: str) -> tuple:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = resp.read()
            ext = url.split('?')[0].split('.')[-1].lower()
            media_type = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                          'png': 'image/png', 'webp': 'image/webp',
                          'gif': 'image/gif'}.get(ext, 'image/jpeg')
            return base64.b64encode(data).decode('utf-8'), media_type
        except Exception:
            return None, None

    def _image_data_url(self, url: str):
        b64, media_type = self._fetch_image_base64(url)
        if b64:
            return f"data:{media_type};base64,{b64}"
        return None

    # ------------------------------------------------------------------
    # API — tags completas por imagem + nome
    # ------------------------------------------------------------------

    def get_packaging_batch(self, products: list, max_retries: int = 8) -> dict:
        """Retorna {index: '#tag'} com a tag de embalagem identificada pela foto."""
        base = self._PACKAGING_PROMPT
        if self._user_additions:
            base = base + '\n\nInstruções adicionais:\n' + self._user_additions
        user_content = [{"type": "text", "text": base + "\n\nProdutos:\n"}]

        for i, p in enumerate(products):
            user_content.append({"type": "text", "text": f"Produto {i + 1}: {p['name']}"})
            if p.get('image_url'):
                data_url = self._image_data_url(p['image_url'])
                if data_url:
                    user_content.append({
                        "type": "image_url",
                        "image_url": {"url": data_url, "detail": "low"}
                    })

        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": user_content}],
                    max_tokens=15 * len(products),
                    temperature=0
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    try:
                        idx = int(k) - 1
                    except (ValueError, TypeError):
                        continue
                    if v and isinstance(v, str) and v.strip().startswith('#'):
                        result[idx] = [v.strip().lower()]
                return result
            except _openai_module.RateLimitError as e:
                if _ext._is_quota_error(e):
                    _ext.emit_quota_exceeded()
                    raise
                wait = _parse_rate_limit_wait(e)
                self.log_message(f"Rate limit, aguardando {wait:.1f}s...", "warning")
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return {}

    def _format_char_tag(self, tag_id: str) -> str:
        """Converte tag interna (snake_case) para exibição com espaços e acentos."""
        if tag_id in self._CHAR_TAG_FORMAT:
            return '#' + self._CHAR_TAG_FORMAT[tag_id]
        return '#' + tag_id.replace('_', ' ')

    def get_characteristics_batch(self, products: list, max_retries: int = 8) -> dict:
        """Retorna {index: ['tag1', 'tag2']} com características identificadas pelo nome."""
        lines = "\n".join(f"{i+1}. {p['name']}" for i, p in enumerate(products))
        prompt = self._CHARACTERISTICS_PROMPT + f"\nProdutos:\n{lines}"
        valid = set(self._CHARACTERISTICS_TAGS)
        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=100 * len(products),
                    temperature=0
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    try:
                        idx = int(k) - 1
                    except (ValueError, TypeError):
                        continue
                    if 0 <= idx < len(products) and isinstance(v, list):
                        result[idx] = [t for t in v if t in valid]
                return result
            except _openai_module.RateLimitError as e:
                if _ext._is_quota_error(e):
                    _ext.emit_quota_exceeded()
                    raise
                wait = _parse_rate_limit_wait(e)
                self.log_message(f"Rate limit, aguardando {wait:.1f}s...", "warning")
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return {}

    def get_characteristics_with_images_batch(self, products: list, max_retries: int = 8) -> dict:
        """Características + detecção pet via imagem. Retorna {index: ['tag1', ...]}."""
        valid = set(self._CHARACTERISTICS_TAGS + self._CHARACTERISTICS_IMAGES_EXTRA_TAGS)
        user_content = [{"type": "text", "text": self._CHARACTERISTICS_IMAGES_PROMPT + "\n\nProdutos:\n"}]
        for i, p in enumerate(products):
            user_content.append({"type": "text", "text": f"Produto {i + 1}: {p['name']}"})
            if p.get('image_url'):
                data_url = self._image_data_url(p['image_url'])
                if data_url:
                    user_content.append({
                        "type": "image_url",
                        "image_url": {"url": data_url, "detail": "low"}
                    })
        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": user_content}],
                    max_tokens=120 * len(products),
                    temperature=0
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    try:
                        idx = int(k) - 1
                    except (ValueError, TypeError):
                        continue
                    if 0 <= idx < len(products) and isinstance(v, list):
                        result[idx] = [t for t in v if t in valid]
                return result
            except _openai_module.RateLimitError as e:
                if _ext._is_quota_error(e):
                    _ext.emit_quota_exceeded()
                    raise
                wait = _parse_rate_limit_wait(e)
                self.log_message(f"Rate limit, aguardando {wait:.1f}s...", "warning")
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return {}

    def get_brands_batch(self, products: list, max_retries: int = 8) -> dict:
        """Retorna {index: '#marca'} identificando a marca pela imagem."""
        user_content = [{"type": "text", "text": self._BRANDS_PROMPT + "\n\nProdutos:\n"}]
        for i, p in enumerate(products):
            user_content.append({"type": "text", "text": f"Produto {i + 1}: {p['name']}"})
            if p.get('image_url'):
                data_url = self._image_data_url(p['image_url'])
                if data_url:
                    user_content.append({
                        "type": "image_url",
                        "image_url": {"url": data_url, "detail": "low"}
                    })
        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": user_content}],
                    max_tokens=20 * len(products),
                    temperature=0
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    try:
                        idx = int(k) - 1
                    except (ValueError, TypeError):
                        continue
                    if v and isinstance(v, str) and v.startswith('#'):
                        result[idx] = v.lower().strip()
                return result
            except _openai_module.RateLimitError as e:
                if _ext._is_quota_error(e):
                    _ext.emit_quota_exceeded()
                    raise
                wait = _parse_rate_limit_wait(e)
                self.log_message(f"Rate limit, aguardando {wait:.1f}s...", "warning")
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return {}

    def get_brands_from_names_batch(self, products: list, max_retries: int = 8) -> dict:
        """Fallback: identifica marcas apenas pelo nome, sem imagens."""
        lines = "\n".join(f"{i+1}. {p['name']}" for i, p in enumerate(products))
        prompt = self._BRANDS_NAME_FALLBACK_PROMPT + f"\n\nProdutos:\n{lines}"
        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=20 * len(products),
                    temperature=0
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    try:
                        idx = int(k) - 1
                    except (ValueError, TypeError):
                        continue
                    if v and isinstance(v, str) and v.startswith('#'):
                        result[idx] = v.lower().strip()
                return result
            except _openai_module.RateLimitError as e:
                if _ext._is_quota_error(e):
                    _ext.emit_quota_exceeded()
                    raise
                wait = _parse_rate_limit_wait(e)
                self.log_message(f"Rate limit, aguardando {wait:.1f}s...", "warning")
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return {}

    # ------------------------------------------------------------------
    # Firestore helpers
    # ------------------------------------------------------------------

    def _get_products(self, estabelecimento_id: str, categories: list, use_images: bool,
                      filter_subcategory_id: str = None) -> list:
        ref = (
            self.db.collection('estabelecimentos')
            .document(estabelecimento_id)
            .collection('Products')
        )
        products = []
        for doc in ref.stream():
            data = doc.to_dict()
            if not data.get('name'):
                continue
            if data.get('isTrashed', False):
                continue
            if categories:
                cats = data.get('categoriesIds') or []
                if not any(c in categories for c in cats):
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
            products.append({
                'id': doc.id,
                'name': data['name'],
                'image_url': image_url,
                'existing_tags': data.get('tags') or []
            })
        return products

    def _save_tags_batch(self, estabelecimento_id: str, updates: list, dry_run: bool):
        if dry_run or not updates:
            return
        db_batch = self.db.batch()
        for pid, tags in updates:
            ref = (self.db.collection('estabelecimentos')
                   .document(estabelecimento_id)
                   .collection('Products')
                   .document(pid))
            db_batch.update(ref, {'tags': tags})
        db_batch.commit()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run_tagging(self, estabelecimento_id: str, categories: list,
                    delay: float = 0, dry_run: bool = False,
                    use_images: bool = False, overwrite: bool = False,
                    tag_characteristics: bool = False, only_untagged: bool = False,
                    tag_brands: bool = False, filter_subcategory_id: str = None):
        tagger_state['running'] = True
        tagger_state['current_product'] = None
        tagger_state['progress'] = {
            'total': 0, 'processed': 0, 'updated': 0,
            'skipped': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
        }

        sep = {'timestamp': datetime.now().strftime("%H:%M:%S"), 'message': '─' * 40, 'level': 'separator'}
        tagger_state['logs'].append(sep)
        try:
            _ext.socketio.emit('tagger_log_update', sep)
        except Exception:
            pass

        self.tokens_used = 0
        self.estimated_cost = 0.0

        # tag_brands requer imagens
        _need_images = use_images or tag_brands or (tag_characteristics and use_images)

        try:
            self.log_message(f"Buscando produtos de '{estabelecimento_id}'...", "info")
            products = self._get_products(estabelecimento_id, categories, _need_images, filter_subcategory_id)

            total = len(products)
            tagger_state['progress']['total'] = total
            self.log_message(f"{total} produtos encontrados", "info")

            if dry_run:
                self.log_message("MODO DRY RUN — nenhuma alteração será salva", "warning")

            if only_untagged:
                to_process = [p for p in products if not p['existing_tags']]
                skipped = len(products) - len(to_process)
                if skipped:
                    self._update_progress(skipped=skipped)
                    self.log_message(f"{skipped} produtos ignorados (já têm tags)", "info")
            else:
                to_process = list(products)

            if tag_brands:
                mode = "IA — marcas por imagem"
            elif tag_characteristics and use_images:
                mode = "IA — características + detecção pet por imagem"
            elif tag_characteristics:
                mode = "medidas do nome (g, ml, kg, L)"
            elif use_images:
                mode = "IA — embalagem por imagem"
            else:
                mode = "embalagem por imagem (padrão)"
            self.log_message(
                f"{len(to_process)} produtos para processar — {mode}, "
                f"{self.MAX_WORKERS} workers paralelos",
                "info"
            )
            self._emit_progress()

            if tag_brands:
                def _brands_chunk(chunk, est_id, dr):
                    return self._process_brands_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.IMAGE_BATCH_SIZE, _brands_chunk)
            elif tag_characteristics and use_images:
                def _char_img_chunk(chunk, est_id, dr):
                    return self._process_characteristics_with_images_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.IMAGE_BATCH_SIZE, _char_img_chunk)
            elif tag_characteristics:
                def _char_chunk(chunk, est_id, dr):
                    return self._process_characteristics_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   500, _char_chunk)
            elif use_images:
                def _image_chunk(chunk, est_id, dr):
                    return self._process_image_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.IMAGE_BATCH_SIZE, _image_chunk)

        except Exception as e:
            self.log_message(f"Erro fatal: {e}", "error")
            logger.error(f"Tagger erro fatal: {e}")
        finally:
            tagger_state['running'] = False
            tagger_state['current_product'] = None
            p = tagger_state['progress']
            p['tokens_used'] = self.tokens_used
            p['estimated_cost'] = round(self.estimated_cost, 6)
            self.log_message(
                f"Concluído — {p['updated']} atualizados, {p['skipped']} ignorados, {p['errors']} erros",
                "success"
            )
            self._emit_progress()
            try:
                _ext.socketio.emit('tagger_status_update', {
                    'running': False,
                    'progress': tagger_state['progress'],
                    'current_product': None
                })
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Parallel execution
    # ------------------------------------------------------------------

    def _run_parallel(self, products, estabelecimento_id, dry_run, chunk_size, process_fn):
        chunks = [products[i:i + chunk_size] for i in range(0, len(products), chunk_size)]
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_fn, chunk, estabelecimento_id, dry_run): chunk
                for chunk in chunks
            }
            for future in as_completed(futures):
                if not tagger_state['running']:
                    for f in futures:
                        f.cancel()
                    self.log_message("Execução interrompida pelo usuário", "warning")
                    break
                try:
                    future.result()
                except Exception as e:
                    self.log_message(f"Erro em chunk: {e}", "error")

    def _process_characteristics_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Tags de quantidade extraídas do nome (g, ml, kg, L). Sem IA."""
        if not tagger_state['running']:
            return
        updates = []
        updated = skipped = errors = 0
        for product in chunk:
            qty_tags = _quantity_tags_from_name(product['name'])
            if not qty_tags:
                self.log_message(f"{product['name']} → nenhuma medida encontrada", "info")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                final_tags = qty_tags
            else:
                to_add = [t for t in qty_tags if t not in existing]
                if not to_add:
                    self.log_message(f"{product['name']} → tags já existem, pulando", "info")
                    skipped += 1
                    continue
                final_tags = existing + to_add
            self.log_message(f"{product['name']} → {' '.join(final_tags)}", "info")
            updates.append((product['id'], final_tags))
            updated += 1
        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar: {e}", "error")
            errors += updated
            updated = 0
        self._update_progress(updated=updated, skipped=skipped, errors=errors)

    def _process_characteristics_with_images_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Características via IA com suporte a imagens (detecção pet food). Mescla ou substitui."""
        if not tagger_state['running']:
            return
        try:
            char_map = self.get_characteristics_with_images_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao identificar características (imagens): {e}", "error")
            char_map = {}

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            char_tags = [self._format_char_tag(t) for t in (char_map.get(j) or [])]
            if not char_tags:
                self.log_message(f"{product['name']} → nenhuma característica identificada", "info")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                if all(t in existing for t in char_tags):
                    self.log_message(f"{product['name']} → tags já existem, pulando", "info")
                    skipped += 1
                    continue
                final_tags = char_tags
            else:
                to_add = [t for t in char_tags if t not in existing]
                if not to_add:
                    self.log_message(f"{product['name']} → nenhuma tag nova, pulando", "info")
                    skipped += 1
                    continue
                final_tags = existing + to_add
            self.log_message(f"{product['name']} → {' '.join(final_tags)}", "info")
            updates.append((product['id'], final_tags))
            updated += 1
        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar: {e}", "error")
            errors += updated
            updated = 0
        self._update_progress(updated=updated, skipped=skipped, errors=errors)

    def _process_brands_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Identifica marcas via imagem; usa nome como fallback quando imagem não identifica."""
        if not tagger_state['running']:
            return
        try:
            brands_map = self.get_brands_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao identificar marcas (imagem): {e}", "error")
            brands_map = {}

        # Fallback por nome para produtos sem marca identificada pela imagem
        missing = [j for j in range(len(chunk)) if j not in brands_map]
        if missing:
            fallback_products = [chunk[j] for j in missing]
            try:
                fallback_map = self.get_brands_from_names_batch(fallback_products)
                for fi, brand in fallback_map.items():
                    if fi < len(missing):
                        brands_map[missing[fi]] = brand
            except Exception as e:
                self.log_message(f"Erro ao identificar marcas (nome): {e}", "error")

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            brand_tag = brands_map.get(j)
            if not brand_tag:
                self.log_message(f"{product['name']} → marca não identificada", "info")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                if brand_tag in existing:
                    self.log_message(f"{product['name']} → marca já existe, pulando", "info")
                    skipped += 1
                    continue
                final_tags = [brand_tag] + [t for t in existing if not t == brand_tag]
            else:
                if brand_tag in existing:
                    self.log_message(f"{product['name']} → marca já existe, pulando", "info")
                    skipped += 1
                    continue
                final_tags = existing + [brand_tag]
            self.log_message(f"{product['name']} → {brand_tag}", "info")
            updates.append((product['id'], final_tags))
            updated += 1
        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar: {e}", "error")
            errors += updated
            updated = 0
        self._update_progress(updated=updated, skipped=skipped, errors=errors)

    @staticmethod
    def _normalize_packaging(raw: str) -> str:
        """Normaliza tipo de embalagem para tag canônica."""
        lower = raw.lower().strip()
        return _PACKAGING_NORMALIZE.get(lower, lower).replace(' ', '')

    def _process_image_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Tags completas via IA (nome + imagem).
        overwrite=True → limpa tags existentes e substitui pelas novas.
        overwrite=False → mescla com tags existentes sem duplicar."""
        if not tagger_state['running']:
            return
        try:
            tags_map = self.get_packaging_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao gerar tags: {e}", "error")
            tags_map = {}

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            name = product['name']
            new_tags = _filter_packaging_tags(tags_map.get(j) or [], name)
            if not new_tags:
                self.log_message(f"{name} → nenhuma tag gerada", "warning")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                final_tags = new_tags
            else:
                to_add = [t for t in new_tags if t not in existing]
                if not to_add:
                    self.log_message(f"{name} → nenhuma tag nova, pulando", "info")
                    skipped += 1
                    continue
                final_tags = existing + to_add
            self.log_message(f"{name} → {' '.join(final_tags)}", "info")
            updates.append((product['id'], final_tags))
            updated += 1

        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar: {e}", "error")
            errors += updated
            updated = 0
        self._update_progress(updated=updated, skipped=skipped, errors=errors)
