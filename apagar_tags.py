"""
Apaga as tags de todos os produtos do estabelecimento q0IPIusmpEq3pHbMyfWY.
Uso: python apagar_tags.py
     python apagar_tags.py --dry-run   (simula sem salvar)
"""

import sys
import json
import os
from dotenv import load_dotenv

load_dotenv()

import firebase_admin
from firebase_admin import credentials, firestore

ESTABELECIMENTO_ID = 'q0IPIusmpEq3pHbMyfWY'
BATCH_LIMIT = 500  # Firestore suporta até 500 ops por batch


def init_firebase():
    if firebase_admin._apps:
        return firestore.client()
    creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON', '').strip()
    if creds_json:
        cred = credentials.Certificate(json.loads(creds_json))
    else:
        creds_path = os.getenv('FIREBASE_CREDENTIALS_PATH', 'appmobileprod-19505-firebase-admin.json')
        cred = credentials.Certificate(creds_path)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def apagar_tags(dry_run: bool = False):
    db = init_firebase()

    col_ref = (
        db.collection('estabelecimentos')
        .document(ESTABELECIMENTO_ID)
        .collection('Products')
    )

    print(f"Buscando produtos de '{ESTABELECIMENTO_ID}'...")
    docs = list(col_ref.stream())
    total = len(docs)
    print(f"{total} produtos encontrados.")

    # Filtra só os que têm tags não-vazias
    com_tags = [
        doc for doc in docs
        if (doc.to_dict() or {}).get('tags')
    ]
    print(f"{len(com_tags)} produtos com tags para limpar.")

    if not com_tags:
        print("Nada a fazer.")
        return

    if dry_run:
        print("\n[DRY RUN] Produtos que seriam atualizados:")
        for doc in com_tags:
            data = doc.to_dict() or {}
            print(f"  {doc.id} — {data.get('name', '?')} — tags: {data.get('tags')}")
        print(f"\n[DRY RUN] {len(com_tags)} produtos seriam limpos. Nenhuma alteração foi salva.")
        return

    # Confirma antes de salvar
    resp = input(f"\nApagar tags de {len(com_tags)} produtos? [s/N] ").strip().lower()
    if resp != 's':
        print("Operação cancelada.")
        return

    # Processa em batches de 500
    atualizados = 0
    erros = 0
    chunks = [com_tags[i:i + BATCH_LIMIT] for i in range(0, len(com_tags), BATCH_LIMIT)]

    for chunk_idx, chunk in enumerate(chunks):
        batch = db.batch()
        for doc in chunk:
            ref = (
                db.collection('estabelecimentos')
                .document(ESTABELECIMENTO_ID)
                .collection('Products')
                .document(doc.id)
            )
            batch.update(ref, {'tags': []})
        try:
            batch.commit()
            atualizados += len(chunk)
            print(f"  Batch {chunk_idx + 1}/{len(chunks)} — {atualizados}/{len(com_tags)} atualizados")
        except Exception as e:
            erros += len(chunk)
            print(f"  Erro no batch {chunk_idx + 1}: {e}")

    print(f"\nConcluído — {atualizados} limpos, {erros} erros.")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    apagar_tags(dry_run=dry_run)
