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


class ProductTagger:
    # Sem imagens: 40 nomes por chamada, 3 workers paralelos
    BATCH_SIZE = 40
    # Com imagens: 5 produtos+imagens por chamada, 3 workers paralelos
    IMAGE_BATCH_SIZE = 5
    MAX_WORKERS = 3

    INPUT_COST  = 0.00015 / 1000   # gpt-4o-mini
    OUTPUT_COST = 0.0006  / 1000

    _SYSTEM_PROMPT = (
        "Você é um especialista em categorização de produtos de supermercado. "
        "Gere tags relevantes para produtos com base no nome e, quando disponível, na imagem.\n\n"
        "Regras:\n"
        "- Gere de 3 a 8 tags por produto\n"
        "- SEMPRE inclua a marca do produto como tag quando identificável no nome "
        "(ex: Nestlé → #Nestle, Coca-Cola → #CocaCola, Piracanjuba → #Piracanjuba)\n"
        "- Tags do nome: marca (obrigatório), tipo do produto, sabor/variante, atributos especiais "
        "(ex: #ZeroLactose, #Integral, #Light, #Diet, #Proteina)\n"
        "- Tags da imagem (se fornecida): APENAS o tipo de embalagem identificado "
        "(ex: #Garrafa, #Lata, #Caixinha, #Sache, #Pote, #Copo, #Bandeja, #Bag, #Vidro, #TetraPak)\n"
        "- Cada tag começa com # e usa PascalCase sem espaços\n"
        "- Responda SOMENTE com JSON, sem texto adicional"
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
        """Baixa imagem e retorna (base64_str, media_type). Retorna (None, None) se falhar."""
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

    def _image_data_url(self, url: str) -> str | None:
        b64, media_type = self._fetch_image_base64(url)
        if b64:
            return f"data:{media_type};base64,{b64}"
        return None

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def get_tags_batch(self, names: list, max_retries: int = 8) -> dict:
        """Batch de nomes (sem imagens). Retorna {index: [tags]}."""
        numbered = "\n".join(f"{i + 1}. {n}" for i, n in enumerate(names))
        user_prompt = (
            f"Gere tags para os {len(names)} produtos abaixo.\n\n"
            f"{numbered}\n\n"
            "Responda SOMENTE com JSON: chave = número do produto (string), valor = array de tags com #.\n"
            'Exemplo: {"1": ["#Biscoito", "#Wafer", "#Bauducco"], "2": ["#Leite", "#Integral"]}'
        )
        for attempt in range(max_retries):
            try:
                resp = _ext.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": self._SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=2000,
                    temperature=0.3
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    idx = int(k) - 1
                    if isinstance(v, list):
                        result[idx] = [str(t) for t in v if str(t).startswith('#')]
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

    def get_tags_image_batch(self, products: list, max_retries: int = 8) -> dict:
        """Batch com imagens (até IMAGE_BATCH_SIZE produtos). Retorna {index: [tags]}.
        products: lista de dicts com 'name' e 'image_url'.
        """
        # Monta conteúdo: para cada produto, texto + imagem (se disponível)
        user_content = []
        user_content.append({
            "type": "text",
            "text": (
                f"Gere tags para os {len(products)} produtos abaixo. "
                "Para cada produto considere o nome E a imagem da embalagem.\n\n"
                "Responda SOMENTE com JSON: chave = número do produto (string), valor = array de tags com #.\n"
                'Exemplo: {"1": ["#Leite", "#TetraPak"], "2": ["#Biscoito", "#Lata"]}\n\n'
            )
        })

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
                    messages=[
                        {"role": "system", "content": self._SYSTEM_PROMPT},
                        {"role": "user", "content": user_content}
                    ],
                    max_tokens=800,
                    temperature=0.3
                )
                self._record_usage(resp.usage)
                data = json.loads(self._clean_json(resp.choices[0].message.content))
                result = {}
                for k, v in data.items():
                    idx = int(k) - 1
                    if isinstance(v, list):
                        result[idx] = [str(t) for t in v if str(t).startswith('#')]
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
        """updates: lista de (product_id, tags)"""
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
                    use_images: bool = False, overwrite: bool = False):
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

            # Separa os que já têm tags (se não for sobrescrever)
            to_process = []
            skipped_count = 0
            for p in products:
                if p['existing_tags'] and not overwrite:
                    skipped_count += 1
                else:
                    to_process.append(p)

            if skipped_count:
                self._update_progress(skipped=skipped_count)
                self.log_message(f"{skipped_count} produtos ignorados (já têm tags)", "info")

            self.log_message(
                f"{len(to_process)} produtos para processar — "
                f"{'com imagens' if use_images else 'sem imagens'}, "
                f"{self.MAX_WORKERS} workers paralelos",
                "info"
            )
            self._emit_progress()

            if use_images:
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.IMAGE_BATCH_SIZE, self._process_image_chunk)
            else:
                self._run_parallel(to_process, estabelecimento_id, dry_run,
                                   self.BATCH_SIZE, self._process_name_chunk)

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
                    # Cancela futures pendentes
                    for f in futures:
                        f.cancel()
                    self.log_message("Execução interrompida pelo usuário", "warning")
                    break
                try:
                    future.result()
                except Exception as e:
                    self.log_message(f"Erro em chunk: {e}", "error")

    def _process_name_chunk(self, chunk, estabelecimento_id, dry_run):
        """Processa um chunk de produtos sem imagens."""
        if not tagger_state['running']:
            return
        names = [p['name'] for p in chunk]
        try:
            tags_map = self.get_tags_batch(names)
        except Exception as e:
            self.log_message(f"Erro no batch de nomes: {e}", "error")
            self._update_progress(errors=len(chunk))
            return

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            tags = tags_map.get(j, [])
            name = product['name']
            if tags:
                self.log_message(f"{name} → {', '.join(tags)}", "info")
                updates.append((product['id'], tags))
                updated += 1
            else:
                self.log_message(f"{name} → sem tags geradas", "warning")
                skipped += 1

        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar batch: {e}", "error")
            errors += updated
            updated = 0

        self._update_progress(updated=updated, skipped=skipped, errors=errors)

    def _process_image_chunk(self, chunk, estabelecimento_id, dry_run):
        """Processa um chunk de produtos com imagens."""
        if not tagger_state['running']:
            return
        try:
            tags_map = self.get_tags_image_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro no batch de imagens: {e}", "error")
            self._update_progress(errors=len(chunk))
            return

        updates = []
        updated = skipped = errors = 0
        for j, product in enumerate(chunk):
            tags = tags_map.get(j, [])
            name = product['name']
            has_img = bool(product.get('image_url'))
            suffix = " [img]" if has_img else ""
            if tags:
                self.log_message(f"{name} → {', '.join(tags)}{suffix}", "info")
                updates.append((product['id'], tags))
                updated += 1
            else:
                self.log_message(f"{name} → sem tags geradas", "warning")
                skipped += 1

        try:
            self._save_tags_batch(estabelecimento_id, updates, dry_run)
        except Exception as e:
            self.log_message(f"Erro ao salvar batch: {e}", "error")
            errors += updated
            updated = 0

        self._update_progress(updated=updated, skipped=skipped, errors=errors)
