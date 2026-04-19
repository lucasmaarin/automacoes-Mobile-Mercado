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


# Palavras conectoras que não viram tags
_STOP_WORDS = {
    'e', 'de', 'da', 'do', 'das', 'dos', 'para', 'com', 'em',
    'no', 'na', 'nos', 'nas', 'a', 'o', 'as', 'os', 'um', 'uma',
    'ao', 'aos', 'à', 'às', 'por', 'pelo', 'pela', 'pelos', 'pelas',
    'ou', 'que', 'se', 'seu', 'sua', 'seus', 'suas'
}

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


def _tags_from_name(name: str) -> list:
    """Gera tags a partir do nome sem usar IA — divide por palavras, filtra stop words."""
    words = name.split()
    tags = []
    seen = set()
    for word in words:
        # Remove caracteres que não sejam letras, números, acentos ou %
        clean = re.sub(r'[^\w%]', '', word, flags=re.UNICODE)
        if not clean:
            continue
        lower = clean.lower()
        # Filtra stop words e tokens muito curtos (1 char) que não sejam numéricos
        if lower in _STOP_WORDS:
            continue
        if len(lower) < 2 and not lower.isdigit():
            continue
        tag = '#' + lower
        if tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return tags


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
        # Estilo de vida / Dieta
        'vegano', 'vegetariano', 'fitness', 'saudavel', 'low_carb',
        'sem_gluten', 'sem_lactose', 'sem_acucar', 'diet', 'light',
        # Restrições / Alergênicos
        'contem_lactose', 'contem_gluten', 'contem_acucar', 'contem_alcool',
        'contem_cafeina', 'contem_amendoim', 'contem_soja',
        # Tipo Funcional
        'alcoolico', 'nao_alcoolico', 'energetico', 'hidratante',
        'estimulante', 'calmante',
        # Uso / Contexto
        'limpeza', 'higiene_pessoal', 'consumo_imediato', 'preparo_culinario',
        'infantil', 'pet',
        # Risco / Logística
        'inflamavel', 'perecivel', 'congelado', 'refrigerado', 'fragil',
    ]

    _CHARACTERISTICS_PROMPT = (
        "Voce recebera uma lista de nomes de produtos de supermercado.\n"
        "Para cada produto, retorne APENAS as tags de caracteristicas que voce tem CERTEZA ABSOLUTA que se aplicam, "
        "baseando-se exclusivamente no nome do produto.\n"
        "Se tiver qualquer duvida sobre uma tag, NAO a inclua.\n"
        "Use SOMENTE tags da lista fornecida — nao invente outras.\n"
        "Responda SOMENTE com JSON: {\"1\": [\"tag1\", \"tag2\"], \"2\": [], ...}\n"
        "Se nenhuma tag se aplica com certeza, retorne lista vazia para aquele produto.\n"
        "Sem markdown, sem explicacoes.\n\n"
        "TAGS VALIDAS:\n"
        "Composicao/Origem: derivado_do_leite, origem_animal, origem_vegetal, integral, desnatado, organico, natural, ultraprocessado\n"
        "Dieta: vegano, vegetariano, fitness, saudavel, low_carb, sem_gluten, sem_lactose, sem_acucar, diet, light\n"
        "Alergenos: contem_lactose, contem_gluten, contem_acucar, contem_alcool, contem_cafeina, contem_amendoim, contem_soja\n"
        "Funcional: alcoolico, nao_alcoolico, energetico, hidratante, estimulante, calmante\n"
        "Uso: limpeza, higiene_pessoal, consumo_imediato, preparo_culinario, infantil, pet\n"
        "Logistica: inflamavel, perecivel, congelado, refrigerado, fragil\n"
    )

    _PACKAGING_PROMPT = (
        "Voce e um agente especializado em catalogar produtos de supermercado atraves de tags de busca.\n"
        "Voce recebera o nome comercial e a foto de cada produto.\n"
        "Regra absoluta: so aplique uma tag se houver evidencia explicita no nome ou na foto. Nunca suponha, nunca infira alem do visivel.\n\n"
        "Gere tags nas seguintes categorias, nesta ordem:\n"
        "1. Categoria generica — o tipo de produto em sua forma mais simples. Ex: #agua, #biscoito, #molho.\n"
        "2. Marca — extraia do nome comercial. Se nao estiver no nome, extraia da foto. Minusculas, sem acento, sem espaco. Ex: #nissin, #barilla. Se nao identificar, omita.\n"
        "3. Especificadores — sabor, variante, caracteristica declarada no nome ou visivel na foto. Ex: #sem gas, #calabresa, #picante. Minusculas, sem acento, palavras compostas separadas por espaco.\n"
        "4. Embalagem — identifique pelo formato e material visiveis na foto. Use o nome como apoio secundario. Se nao for possivel identificar com certeza, omita.\n"
        "5. Quantidade — exatamente como aparece no nome: #85g, #1-5l, #800g. Se nao aparecer no nome, omita.\n"
        "6. Apelidos populares — somente termos consagrados: #lamen para macarray instantaneo, #salgadinho para snacks. Se nao houver apelido obvio, omita.\n"
        "7. Contexto de uso — somente se evidente: #infantil se o rotulo indicar faixa etaria, #preparo culinario para extratos e molhos. Se houver duvida, omita.\n\n"
        "Se a foto estiver em baixa resolucao, cortada ou ilegivel a ponto de impedir identificacao de embalagem ou marca, "
        "gere as tags possiveis pelo nome e adicione #revisar ao final.\n\n"
        "Formato de saida: JSON onde a chave e o numero do produto (string) e o valor e uma string com as tags separadas por espaco.\n"
        "Todas as tags em minusculas, sem acento, palavras compostas separadas por espaco, iniciando com #.\n"
        "Nenhum texto adicional. Nenhuma explicacao.\n"
        'Exemplo: {"1": "#agua #crystal #sem gas #garrafa #500ml", "2": "#biscoito #oreo #recheado #pacote #144g", "3": null}'
    )

    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0.0
        self._lock = threading.Lock()

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
        """Retorna {index: ['#tag1', '#tag2', ...]} com todas as tags geradas pelo novo prompt."""
        user_content = [{"type": "text", "text": self._PACKAGING_PROMPT + "\n\nProdutos:\n"}]

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
                    max_tokens=80 * len(products),
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
                    if v and isinstance(v, str):
                        tags = [t.strip().replace('-', ' ') for t in v.split() if t.strip().startswith('#')]
                        if tags:
                            result[idx] = tags
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

    # ------------------------------------------------------------------
    # Firestore helpers
    # ------------------------------------------------------------------

    def _get_products(self, estabelecimento_id: str, categories: list, use_images: bool) -> list:
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
                    tag_characteristics: bool = False, only_untagged: bool = False):
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

        try:
            self.log_message(f"Buscando produtos de '{estabelecimento_id}'...", "info")
            products = self._get_products(estabelecimento_id, categories, use_images)

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

            if tag_characteristics:
                mode = "IA — características do produto"
            elif use_images:
                mode = "IA — embalagem por imagem"
            else:
                mode = "palavras do nome"
            self.log_message(
                f"{len(to_process)} produtos para processar — {mode}, "
                f"{self.MAX_WORKERS} workers paralelos",
                "info"
            )
            self._emit_progress()

            if tag_characteristics:
                def _char_chunk(chunk, est_id, dr):
                    return self._process_characteristics_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   20, _char_chunk)
            elif use_images:
                def _image_chunk(chunk, est_id, dr):
                    return self._process_image_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.IMAGE_BATCH_SIZE, _image_chunk)
            else:
                def _name_chunk(chunk, est_id, dr):
                    return self._process_name_chunk(chunk, est_id, dr, overwrite)
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   500, _name_chunk)

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

    def _process_name_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Tags geradas diretamente das palavras do nome — sem IA.
        overwrite=False → mescla com tags existentes (acrescenta sem remover).
        overwrite=True  → substitui, mas pula se as novas tags já existem todas no produto."""
        if not tagger_state['running']:
            return
        updates = []
        updated = skipped = errors = 0
        for product in chunk:
            name = product['name']
            new_tags = _tags_from_name(name)
            if not new_tags:
                self.log_message(f"{name} → sem tags geradas", "warning")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                # Pula se todas as novas tags já existem no produto
                if all(t in existing for t in new_tags):
                    self.log_message(f"{name} → tags já existem, pulando", "info")
                    skipped += 1
                    continue
                final_tags = new_tags
            else:
                # Mescla: acrescenta apenas tags que ainda não existem
                to_add = [t for t in new_tags if t not in existing]
                if not to_add:
                    self.log_message(f"{name} → nenhuma tag nova para acrescentar, pulando", "info")
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

    def _process_characteristics_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Tags de características via IA (sem imagem). Mescla ou substitui conforme overwrite."""
        if not tagger_state['running']:
            return
        try:
            char_map = self.get_characteristics_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao identificar características: {e}", "error")
            char_map = {}

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            char_tags = ['#' + t.replace('_', ' ') for t in (char_map.get(j) or [])]
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
                    self.log_message(f"{product['name']} → nenhuma tag nova para acrescentar, pulando", "info")
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

    @staticmethod
    def _normalize_packaging(raw: str) -> str:
        """Normaliza tipo de embalagem para tag canônica."""
        lower = raw.lower().strip()
        return _PACKAGING_NORMALIZE.get(lower, lower).replace(' ', '')

    def _process_image_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=True):
        """Tags completas via IA (nome + imagem).
        overwrite=True → substitui tags existentes.
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
            new_tags = tags_map.get(j)
            if not new_tags:
                self.log_message(f"{name} → nenhuma tag gerada", "warning")
                skipped += 1
                continue
            existing = list(product.get('existing_tags') or [])
            if overwrite:
                if all(t in existing for t in new_tags):
                    self.log_message(f"{name} → tags já existem, pulando", "info")
                    skipped += 1
                    continue
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
