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
        self.cat_user_additions = self._load_user_additions()
        self.cat_system_prompt = self._build_system_prompt()

    def _build_system_prompt(self) -> str:
        if self.cat_user_additions:
            return self.DEFAULT_CAT_SYSTEM_PROMPT + '\n\nInstruções adicionais:\n' + self.cat_user_additions
        return self.DEFAULT_CAT_SYSTEM_PROMPT

    def _load_user_additions(self) -> str:
        try:
            doc = self.db.collection('Automacoes').document('defenir_catsub').get()
            if doc.exists:
                additions = (doc.to_dict() or {}).get('user_additions', '')
                if additions:
                    return additions
        except Exception as e:
            logger.warning(f"Nao foi possivel carregar instrucoes adicionais do categorizador: {e}")
        return ''

    def save_user_additions_to_firestore(self, additions: str) -> bool:
        try:
            self.db.collection('Automacoes').document('defenir_catsub').set({
                'user_additions': additions,
                'updated_at': datetime.now().isoformat(),
            }, merge=True)
            self.cat_user_additions = additions
            self.cat_system_prompt = self._build_system_prompt()
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar instrucoes adicionais do categorizador: {e}")
            return False
