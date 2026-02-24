from typing import List, Dict, Any
from datetime import datetime

from config import logger
from utils import safe_sample, to_json_safe
from extensions import socketio
from utils import explorer_state

class FirestoreStructureExplorer:
    def __init__(self, db_client):
        self.db = db_client

    def log_message(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        explorer_state['logs'].append(log_entry)
        if len(explorer_state['logs']) > 100:
            explorer_state['logs'] = explorer_state['logs'][-100:]
        try:
            socketio.emit('explorer_log_update', log_entry)
        except Exception:
            pass
        logger.info(f"{level.upper()}: {message}")

    def update_progress(self):
        try:
            socketio.emit('explorer_progress_update', {
                'progress': explorer_state['progress'],
                'current_path': explorer_state['current_path']
            })
        except Exception:
            pass

    # Métodos get_document_structure, analyze_field_type, get_collection_structure,
    # explore_firestore_path, explore_root_collections, get_path_suggestions seguem a mesma
    # implementação do arquivo original e podem ser colocados aqui conforme necessário.

    def parse_path(self, path: str):
        """Parse caminho Firestore em componentes"""
        path = path.strip('/')
        return path.split('/') if path else []

    def explore_firestore_path(self, path: str, max_docs: int = 10):
        """Explora um caminho específico no Firestore"""
        try:
            components = self.parse_path(path)
            if not components:
                return {"error": "Caminho vazio"}
            
            is_collection_path = len(components) % 2 == 1
            
            if is_collection_path:
                # É um caminho de coleção
                collection_ref = self.db.collection(components[0])
                for i in range(1, len(components), 2):
                    if i + 1 < len(components):
                        collection_ref = collection_ref.document(components[i]).collection(components[i + 1])
                
                docs_out = []
                docs = collection_ref.limit(max_docs).stream()
                for doc in docs:
                    raw = doc.to_dict() or {}
                    docs_out.append({"id": doc.id, "fields": to_json_safe(raw)})
                
                return {
                    "type": "collection",
                    "name": components[-1],
                    "path": path,
                    "documents": docs_out,
                    "count": len(docs_out)
                }
            else:
                # É um caminho de documento
                doc_ref = self.db.collection(components[0])
                for i in range(1, len(components), 2):
                    doc_ref = doc_ref.document(components[i])
                    if i + 1 < len(components):
                        doc_ref = doc_ref.collection(components[i + 1])
                
                doc = doc_ref.get()
                if not doc.exists:
                    return {"error": f"Documento não encontrado: {path}"}
                
                return {
                    "type": "document",
                    "name": components[-1],
                    "path": path,
                    "data": to_json_safe(doc.to_dict() or {})
                }
        except Exception as e:
            return {"error": str(e)}

    def get_path_suggestions(self, partial_path: str, limit: int = 10):
        """Retorna sugestões de caminhos baseado no prefixo"""
        try:
            components = self.parse_path(partial_path)
            if not components:
                # Retorna coleções raiz
                collections = self.db.collections()
                return [{"path": col.id, "type": "collection"} for col in list(collections)[:limit]]
            
            # Se é um número par de componentes, estamos em um documento
            # e precisamos sugerir subcoleções
            if len(components) % 2 == 0:
                doc_ref = self.db.collection(components[0])
                for i in range(1, len(components), 2):
                    doc_ref = doc_ref.document(components[i])
                    if i + 1 < len(components):
                        doc_ref = doc_ref.collection(components[i + 1])
                
                try:
                    subcollections = doc_ref.collections()
                    suggestions = []
                    for idx, subcol in enumerate(subcollections):
                        if idx >= limit:
                            break
                        full_path = f"{partial_path}/{subcol.id}" if partial_path else subcol.id
                        suggestions.append({
                            "path": full_path,
                            "type": "collection",
                            "name": subcol.id
                        })
                    return suggestions
                except Exception:
                    return []
            
            return []
        except Exception as e:
            self.log_message(f"Erro ao gerar sugestões: {str(e)}", "error")
            return []


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
