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
    IMAGE_BATCH_SIZE = 5
    MAX_WORKERS = 3

    INPUT_COST  = 0.00015 / 1000   # gpt-4o-mini
    OUTPUT_COST = 0.0006  / 1000

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

    _PACKAGING_SIMPLIFIED_PROMPT = (
        "Voce recebera o nome e a foto de cada produto de supermercado.\n"
        "Identifique o tipo de embalagem APENAS nestas duas categorias:\n"
        "- \"SAC\": embalagem bag/saco plastico (arroz, feijao, farinha, granola, amendoim em saco, etc.)\n"
        "- \"VIDRO\": garrafa de vidro (azeite, vinagre, molhos, cerveja em garrafa de vidro, etc.)\n"
        "Se a embalagem nao for saco nem garrafa de vidro, retorne null.\n"
        "Use a foto como fonte principal. Se a foto nao estiver disponivel, use o nome.\n"
        "Responda com JSON: {\"1\": \"SAC\", \"2\": null, \"3\": \"VIDRO\", ...}\n"
        "Sem markdown, sem explicacoes."
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

    @staticmethod
    def _is_leite_caixinha(name: str) -> bool:
        """True se leite longa vida (não de coco, não em pó)."""
        tokens = name.lower().strip().split()
        if not tokens or tokens[0] != 'leite':
            return False
        rest = ' '.join(tokens[1:])
        return 'de coco' not in rest and 'em p' not in rest

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def get_packaging_simplified_batch(self, products: list, max_retries: int = 8) -> dict:
        """Retorna {index: 'SAC' | 'VIDRO'} para saco e garrafa de vidro."""
        prompt = self._PACKAGING_SIMPLIFIED_PROMPT
        if self._user_additions:
            prompt = prompt + '\n\nInstruções adicionais:\n' + self._user_additions
        user_content = [{"type": "text", "text": prompt + "\n\nProdutos:\n"}]
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
                    max_tokens=10 * len(products),
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
                    if v in ('SAC', 'VIDRO'):
                        result[idx] = v
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
                    tag_brands: bool = False, filter_subcategory_id: str = None,
                    create_backup: bool = True):
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
            start_time = datetime.now().strftime("%H:%M:%S")
            self.log_message(f"Processo iniciado às {start_time} — estabelecimento: {estabelecimento_id}", "info")
            products = self._get_products(estabelecimento_id, categories, True, filter_subcategory_id)

            total = len(products)
            tagger_state['progress']['total'] = total
            self.log_message(f"{total} produtos encontrados", "info")

            if create_backup and products:
                try:
                    from utils import create_backup_file
                    backup_data = [{'id': p['id'], 'name': p.get('name', ''),
                                    'tags': list(p.get('existing_tags') or [])} for p in products]
                    filename = create_backup_file('tagger', estabelecimento_id, backup_data)
                    self.log_message(f"Backup criado: {filename}", "info")
                    _ext.socketio.emit('backup_created', {'filename': filename, 'automation': 'tagger'})
                except Exception as e:
                    self.log_message(f"Aviso: não foi possível criar backup: {e}", "warning")

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

            self.log_message(
                f"{len(to_process)} produtos para processar — "
                f"leite (regra) + embalagem + marca (IA), {self.MAX_WORKERS} workers paralelos",
                "info"
            )
            self._emit_progress()

            def _combined_chunk(chunk, est_id, dr):
                return self._process_combined_chunk(chunk, est_id, dr, overwrite)

            self._run_parallel(to_process, estabelecimento_id, dry_run,
                               self.IMAGE_BATCH_SIZE, _combined_chunk)

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

    def _process_combined_chunk(self, chunk, estabelecimento_id, dry_run, overwrite=False):
        """Leite caixinha (regra) + bag/garrafa de vidro (IA) + marca (IA)."""
        if not tagger_state['running']:
            return

        try:
            packaging_map = self.get_packaging_simplified_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao identificar embalagem: {e}", "error")
            packaging_map = {}

        try:
            brands_map = self.get_brands_batch(chunk)
        except Exception as e:
            self.log_message(f"Erro ao identificar marcas (imagem): {e}", "error")
            brands_map = {}

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
            name = product['name']
            new_tags = []

            if self._is_leite_caixinha(name):
                new_tags += ['#caixa', '#caixinha']

            pkg = packaging_map.get(j)
            if pkg == 'SAC':
                new_tags += ['#saco', '#saquinho']
            elif pkg == 'VIDRO':
                new_tags += ['#garrafa', '#garrafa de vidro']

            brand_tag = brands_map.get(j)
            if brand_tag:
                new_tags.append(brand_tag)

            existing = list(product.get('existing_tags') or [])

            if not new_tags:
                self.log_message(f"{name} → sem tags identificadas, pulando", "info")
                skipped += 1
                continue

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
