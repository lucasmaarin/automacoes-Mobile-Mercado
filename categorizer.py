import threading
from datetime import datetime
from config import logger
from extensions import openai_client, socketio, _is_quota_error, emit_quota_exceeded
from utils import categorizer_state, categorizer_targeted_state, _undo_lock, undo_store, record_daily_usage

class ProductCategorizerAgent:
    DEFAULT_CAT_SYSTEM_PROMPT = (
        "Voce e um especialista em categorizacao de produtos de supermercado. "
        "Responda APENAS com o ID solicitado, sem explicacoes, sem pontuacao extra."
    )

    def __init__(self, db_client):
        self.db = db_client
        self.tokens_used = 0
        self.estimated_cost = 0
        self.input_token_cost  = 0.00015 / 1000
        self.output_token_cost = 0.00060 / 1000
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

    # Métodos auxiliares e de execução do categorizador foram mantidos no original.
