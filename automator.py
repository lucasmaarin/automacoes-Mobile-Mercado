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
        self.input_token_cost  = 0.0025 / 1000   # gpt-4o
        self.output_token_cost = 0.01   / 1000   # gpt-4o
        self._lock = threading.Lock()
        self._prompt_suffix = "\n\nNome atual: {produto_nome}\n\nNome melhorado (diferente do original):"
        self.default_prompt_template = """Voce e um especialista em nomenclatura de produtos para um aplicativo de supermercado.

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
- Elimine abreviacoes tecnicas e coloque a palavra completa, exemplo COZ > cozido ou cozinha dependendo do produto, Bdj > bandeja, (primeiras letras das abreviacoes transformadas em letra minusculas) se o nome for passar 7 tokens, apenas retire a abreviacao
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
- "LIMAO" -> "Limão"
- "HIDROT" -> "Hidrotonico"
- "BEB" -> "Bebida"
- "CERV" -> "Cerveja"
- "GHOSTFORCE" -> "Ghost Force"
- "Bdj" -> "Bandeja" (ou retirar se o nome ultrapassar 7 tokens)
- "DIET" -> "Diet"
- "MACA" -> "Maçã"
- "RECH AVEL" -> "Recheio de Avelã"
- "SALG" -> "Salgadinho" (Se for um salgadinho e nao um salgado de festa)
- "CX 48" -> "Caixa 48 Unidades"

Abreviacoes para expandir:
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
- KINORR = Knorr
- DF = Defumado
- XZ = Xadrez
- TEMP = Temperada
- CEB = Cebola
- SACHET = Sache

REGRA OBRIGATORIA: O nome que voce retornar DEVE ser diferente e melhor que o original.

Palavras ou abreviacoes para retirar completamente do nome:
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
- PT/0012/U
- PC10UN
- 12X140ML
- 1X200ML
- 6XFD
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
- FD/0001/UN
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

MARCAS CONHECIDAS (use para identificar o tipo do produto quando o nome for ambiguo):
- Pingo D'Ouro → produto do tipo Salgadinho
- Jubes Hipnose → produto do tipo Bala de Goma
- Look → produto do tipo Biscoito Wafer
- Todechini → produto do tipo Bolacha de Agua e Sal
- Nugget Pasta → produto do tipo Graxa para Sapatos (qualquer cor: Preta, Marrom, Neutra, etc.)"""
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

    def get_products_from_firestore(self, estabelecimento_id: str, categories: List[str],
                                     filter_subcategory_id: str = None,
                                     use_images: bool = False) -> List[Dict]:
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
                if filter_subcategory_id:
                    prod_subs = data.get('subcategoriesIds', [])
                    if filter_subcategory_id not in prod_subs:
                        continue
                image_url = None
                if use_images:
                    imgs = data.get('images') or []
                    if imgs and isinstance(imgs[0], dict):
                        image_url = imgs[0].get('fileUrl')
                products.append({
                    'id': doc.id,
                    'name': data['name'],
                    'description': (data.get('description') or '').strip(),
                    'image_url': image_url,
                })
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
                    model="gpt-4o",
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

    def get_improved_names_batch(self, product_names: List[str], custom_prompt: str = None,
                                  image_urls: List[str] = None) -> List[str]:
        """Melhora N nomes em uma única chamada. Retorna lista de nomes melhorados na mesma ordem."""
        prompt = custom_prompt if custom_prompt else self.prompt_template
        header = (
            f"{prompt}\n\n"
            f"Melhore os nomes dos produtos abaixo. "
            f"Responda APENAS no formato exato, uma linha por produto:\n"
            f"N. nome_melhorado\n\n"
            f"Produtos:"
        )
        has_images = bool(image_urls and any(image_urls))
        numbered = "\n".join(f"{i+1}. {name}" for i, name in enumerate(product_names))
        text_only_content = f"{header}\n{numbered}"
        if has_images:
            content = [{"type": "text", "text": header}]
            for i, (name, img_url) in enumerate(zip(product_names, image_urls or [])):
                content.append({"type": "text", "text": f"\n{i+1}. {name}"})
                if img_url:
                    content.append({"type": "image_url", "image_url": {"url": img_url, "detail": "low"}})
            msg_content = content
        else:
            msg_content = text_only_content
        results = list(product_names)  # fallback: mantém o original
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": msg_content}],
                    max_tokens=60 * len(product_names),
                    temperature=0.3,
                )
            except _openai_module.RateLimitError as e:
                if _is_quota_error(e):
                    self.log_message("ERRO: Créditos da API OpenAI esgotados.", "error")
                    emit_quota_exceeded()
                    raise
                wait = 0.5 * (2 ** attempt)
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
                err_str = str(e).lower()
                if has_images and ('image' in err_str or 'downloading' in err_str or '400' in err_str):
                    self.log_message(f"Erro ao processar imagens do batch, reprocessando sem imagens...", "warning")
                    msg_content = text_only_content
                    has_images = False
                    continue
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
        raw = response.choices[0].message.content.strip()
        for line in raw.split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                dot_idx = line.index('.')
                n = int(line[:dot_idx].strip()) - 1
                if 0 <= n < len(product_names):
                    results[n] = line[dot_idx + 1:].strip()
            except (ValueError, IndexError):
                continue
        return results

    def format_product_name(self, name: str) -> str:
        if not name:
            return name
        return ' '.join(w.capitalize() for w in name.strip().split())

    def manual_improve_name(self, product_name: str, custom_prompt: str = None) -> str:
        return self.get_improved_product_name(product_name, custom_prompt)

    def update_product_in_firestore(self, product_id: str, estabelecimento_id: str,
                                     new_name: str, old_name: str, dry_run: bool = False,
                                     new_description: str = None) -> bool:
        if dry_run:
            desc_msg = f" + descrição='{new_description}'" if new_description is not None else ""
            self.log_message(f"[DRY RUN] '{old_name}' → '{new_name}'{desc_msg}", "warning")
            return True
        try:
            doc_ref = (self.db.collection('estabelecimentos')
                       .document(estabelecimento_id)
                       .collection('Products')
                       .document(product_id))
            update_data = {'name': new_name}
            if new_description is not None:
                update_data['description'] = new_description
            doc_ref.update(update_data)
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
                                delay: float, dry_run: bool, custom_prompt: str,
                                use_images: bool = False):
        total = len(products)
        BATCH_SIZE = 20
        batches = [products[s:s + BATCH_SIZE] for s in range(0, total, BATCH_SIZE)]

        def _process_batch(args):
            batch_start, batch = args
            if not automation_state['running']:
                return
            names = [p['name'] for p in batch]
            image_urls = [p.get('image_url') for p in batch] if use_images else None
            self.update_progress({'id': batch[0]['id'], 'name': names[0], 'index': batch_start + 1, 'total': total})
            try:
                new_names = self.get_improved_names_batch(names, custom_prompt, image_urls)
            except Exception as e:
                self.log_message(f"  Erro OpenAI no batch: {e}", "error")
                with self._lock:
                    automation_state['progress']['errors'] += len(batch)
                    automation_state['progress']['processed'] += len(batch)
                    automation_state['progress']['tokens_used'] = self.tokens_used
                    automation_state['progress']['estimated_cost'] = self.estimated_cost
                self.update_progress()
                return

            for j, (product, new_name) in enumerate(zip(batch, new_names)):
                if not automation_state['running']:
                    return
                pid, pname = product['id'], product['name']
                i = batch_start + j + 1
                self.log_message(f"[{i}/{total}] {pname}", "info")
                new_name = self.format_product_name(new_name)
                name_changed = new_name != pname
                # Preenche description com o novo nome quando está vazia
                needs_desc = not product.get('description', '')
                new_description = new_name if needs_desc else None
                if not name_changed and not needs_desc:
                    self.log_message(f"  -> Sem alteração", "info")
                    with self._lock:
                        automation_state['progress']['unchanged'] += 1
                        automation_state['progress']['processed'] += 1
                        automation_state['progress']['tokens_used'] = self.tokens_used
                        automation_state['progress']['estimated_cost'] = self.estimated_cost
                    self.update_progress()
                else:
                    if name_changed:
                        desc_suffix = " (+ descrição)" if needs_desc else ""
                        self.log_message(f"  -> '{pname}' => '{new_name}'{desc_suffix}", "success")
                    else:
                        self.log_message(f"  -> Descrição preenchida: '{new_name}'", "success")
                    ok = self.update_product_in_firestore(pid, estabelecimento_id, new_name, pname, dry_run, new_description)
                    with self._lock:
                        if ok:
                            automation_state['progress']['updated'] += 1
                        else:
                            automation_state['progress']['errors'] += 1
                        automation_state['progress']['processed'] += 1
                        automation_state['progress']['tokens_used'] = self.tokens_used
                        automation_state['progress']['estimated_cost'] = self.estimated_cost
                    self.update_progress()

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(_process_batch, ((i * BATCH_SIZE, b) for i, b in enumerate(batches))))

    def run_automation(self, estabelecimento_id: str, categories: List[str],
                       delay: float = 1.0, dry_run: bool = False, custom_prompt: str = None,
                       filter_subcategory_id: str = None, use_images: bool = False):
        try:
            self.tokens_used = 0
            self.estimated_cost = 0
            automation_state['running'] = True
            automation_state['logs'] = []
            automation_state['current_product'] = None
            automation_state['progress'] = {
                'total': 0, 'processed': 0, 'updated': 0,
                'unchanged': 0, 'errors': 0, 'tokens_used': 0, 'estimated_cost': 0.0
            }
            with _undo_lock:
                undo_store['renamer'].clear()
            try:
                socketio.emit('renamer_status_update', {
                    'running': True,
                    'progress': automation_state['progress'],
                    'current_product': None,
                })
                socketio.emit('renamer_logs_update', {'logs': []})
            except Exception:
                pass

            self.log_message(f"Iniciando renomeação para: {estabelecimento_id}", "info")
            self.log_message(f"Categorias: {categories}", "info")
            if filter_subcategory_id:
                self.log_message(f"Filtro de subcategoria: {filter_subcategory_id}", "info")
            if dry_run:
                self.log_message("MODO DRY RUN - Nenhuma atualização será feita", "warning")

            if use_images:
                self.log_message("Analise de imagens ativada", "info")
            products = self.get_products_from_firestore(estabelecimento_id, categories, filter_subcategory_id, use_images)
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
            self.log_message(f"Processando {total} produtos em lotes de 20 (2 paralelos)", "info")

            self.process_products_batch(products, estabelecimento_id, delay, dry_run, custom_prompt, use_images)

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
            try:
                socketio.emit('renamer_status_update', {
                    'running': False,
                    'progress': automation_state['progress'],
                    'current_product': None,
                })
            except Exception:
                pass
            return True
        except Exception as e:
            self.log_message(f"Erro: {e}", "error")
            automation_state['running'] = False
            automation_state['current_product'] = None
            self.update_progress()
            try:
                socketio.emit('renamer_status_update', {
                    'running': False,
                    'progress': automation_state['progress'],
                    'current_product': None,
                })
            except Exception:
                pass
            return False
