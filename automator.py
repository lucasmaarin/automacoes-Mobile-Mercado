from typing import List, Dict, Any
from datetime import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import openai as _openai_module
from extensions import openai_client, socketio, _is_quota_error, emit_quota_exceeded
from utils import to_json_safe, firestore_default, safe_sample, record_daily_usage, automation_state, undo_store, _undo_lock
from utils import get_today_stats
from config import logger

class FirestoreProductAutomator:
    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0
        self.input_token_cost  = 0.00015 / 1000
        self.output_token_cost = 0.00060 / 1000
        self._lock = threading.Lock()
        self._prompt_suffix = "\n\nNome atual: {produto_nome}\n\nNome melhorado (diferente do original):"
        self.default_prompt_template = """
Voce e um especialista em nomenclatura de produtos para um aplicativo de supermercado.

IMPORTANTE: Sua tarefa e SEMPRE MELHORAR o nome do produto. NUNCA retorne o nome original inalterado.

... (prompt truncado no modulo para brevidade) ...
"""
        saved = self.load_prompt_from_firestore()
        self.prompt_template = saved if saved else self.default_prompt_template

    def load_prompt_from_firestore(self) -> str:
        try:
            doc_ref = self.db.collection('Automacoes').document('padronizador_nomes')
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                prompt = data.get('prompt', '').strip()
                if prompt:
                    logger.info("Instrucoes carregadas do Firestore (Automacoes/padronizador_nomes)")
                    return prompt
            return None
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar prompt do Firestore: {e}")
            return None

    def save_prompt_to_firestore(self, prompt: str) -> bool:
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
        try:
            socketio.emit('renamer_log_update', log_entry)
        except Exception:
            pass
        logger.info(f"{level.upper()}: {message}")

    def update_progress(self, current_product=None):
        if current_product:
            automation_state['current_product'] = current_product
        try:
            socketio.emit('renamer_progress_update', {
                'progress': automation_state['progress'],
                'current_product': automation_state['current_product']
            })
        except Exception:
            pass

    def get_available_categories(self, estabelecimento_id: str) -> List[Dict]:
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('ProductCategories'))
            categories = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if data:
                    categories.append({
                        'id': data.get('id', doc.id),
                        'name': data.get('name', ''),
                        'isActive': data.get('isActive', True),
                    })
            return categories
        except Exception as e:
            logger.error(f"Erro ao carregar categorias: {e}")
            return []

    def get_products_from_firestore(self, estabelecimento_id: str, categories: List[str]) -> List[Dict]:
        try:
            col_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products'))
            products = []
            for doc in col_ref.stream():
                data = doc.to_dict()
                if not data or not data.get('name'):
                    continue
                if categories:
                    prod_cats = data.get('categoriesIds', [])
                    if not any(c in prod_cats for c in categories):
                        continue
                products.append({'id': doc.id, 'name': data['name']})
            return products
        except Exception as e:
            logger.error(f"Erro ao carregar produtos: {e}")
            return []

    def get_improved_product_name(self, product_name: str, custom_prompt: str = None) -> str:
        prompt = custom_prompt if custom_prompt else self.prompt_template
        full_prompt = prompt + self._prompt_suffix.format(produto_nome=product_name)
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": full_prompt}],
                    max_tokens=100,
                    temperature=0.3,
                )
            except _openai_module.RateLimitError as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados.", "error")
                    emit_quota_exceeded()
                    raise
                wait = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s, 4s, 8s
                self.log_message(f"Rate limit atingido, aguardando {wait:.1f}s (tentativa {attempt + 1}/{max_retries})...", "warning")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    raise
                continue
            except Exception as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados.", "error")
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

    def format_product_name(self, name: str) -> str:
        if not name:
            return name
        return ' '.join(w.capitalize() for w in name.strip().split())

    def manual_improve_name(self, product_name: str, custom_prompt: str = None) -> str:
        return self.get_improved_product_name(product_name, custom_prompt)

    def update_product_in_firestore(self, product_id: str, estabelecimento_id: str,
                                     new_name: str, old_name: str, dry_run: bool = False) -> bool:
        if dry_run:
            self.log_message(f"[DRY RUN] '{old_name}' → '{new_name}'", "warning")
            return True
        try:
            doc_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products')
                       .document(product_id))
            doc_ref.update({'name': new_name})
            with _undo_lock:
                undo_store['renamer'].append({
                    'product_id': product_id,
                    'estabelecimento_id': estabelecimento_id,
                    'old_name': old_name,
                    'new_name': new_name,
                })
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar produto {product_id}: {e}")
            return False

    def process_products_batch(self, products: List[Dict], estabelecimento_id: str,
                                delay: float, dry_run: bool, custom_prompt: str):
        total = len(products)

        def _process_one(args):
            i, product = args
            if not automation_state['running']:
                return
            pid, pname = product['id'], product['name']
            self.log_message(f"[{i}/{total}] {pname}", "info")
            self.update_progress({'id': pid, 'name': pname, 'index': i, 'total': total})
            try:
                new_name = self.get_improved_product_name(pname, custom_prompt)
                new_name = self.format_product_name(new_name)
            except Exception as e:
                self.log_message(f"  Erro OpenAI: {e}", "error")
                with self._lock:
                    automation_state['progress']['errors'] += 1
                    automation_state['progress']['processed'] += 1
                self.update_progress()
                return

            if new_name == pname:
                self.log_message(f"  -> Sem alteração", "info")
                with self._lock:
                    automation_state['progress']['unchanged'] += 1
                    automation_state['progress']['processed'] += 1
                    automation_state['progress']['tokens_used'] = self.tokens_used
                    automation_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress()
            else:
                self.log_message(f"  -> '{pname}' => '{new_name}'", "success")
                ok = self.update_product_in_firestore(pid, estabelecimento_id, new_name, pname, dry_run)
                with self._lock:
                    if ok:
                        automation_state['progress']['updated'] += 1
                    else:
                        automation_state['progress']['errors'] += 1
                    automation_state['progress']['processed'] += 1
                    automation_state['progress']['tokens_used'] = self.tokens_used
                    automation_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress()

            if delay > 0:
                time.sleep(delay)

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(_process_one, enumerate(products, 1)))

    def run_automation(self, estabelecimento_id: str, categories: List[str],
                       delay: float = 1.0, dry_run: bool = False, custom_prompt: str = None):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            automation_state['running'] = True
            automation_state['logs'] = []
            with _undo_lock:
                undo_store['renamer'].clear()

            self.log_message(f"Iniciando renomeação para: {estabelecimento_id}", "info")
            self.log_message(f"Categorias: {categories}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualização será feita", "warning")

            products = self.get_products_from_firestore(estabelecimento_id, categories)
            if not products:
                self.log_message("Nenhum produto encontrado", "warning")
                automation_state['running'] = False
                return False

            total = len(products)
            automation_state['progress'] = {
                'total': total, 'processed': 0, 'updated': 0,
                'unchanged': 0, 'errors': 0,
                'tokens_used': 0, 'estimated_cost': 0.0,
            }
            self.update_progress()
            self.log_message(f"Processando {total} produtos (4 paralelos)", "info")

            self.process_products_batch(products, estabelecimento_id, delay, dry_run, custom_prompt)

            prog = automation_state['progress']
            self.log_message("=== RESULTADO ===", "info")
            self.log_message(
                f"Total: {prog['total']} | Atualizados: {prog['updated']} | "
                f"Sem alteração: {prog['unchanged']} | Erros: {prog['errors']}", "info"
            )
            self.log_message(f"Tokens: {self.tokens_used:,} | Custo: ${self.estimated_cost:.4f}", "info")

            automation_state['running'] = False
            automation_state['current_product'] = None
            self.update_progress()
            return True
        except Exception as e:
            self.log_message(f"Erro: {e}", "error")
            automation_state['running'] = False
            automation_state['current_product'] = None
            self.update_progress()
            return False
