"""
Script para remover a categoria 'Dúvidas' dos produtos em todos os estabelecimentos.

Para cada estabelecimento:
1. Busca produtos onde categoriesIds contém 'KfmNz06cADwmzQI6UvSf' (Dúvidas)
2. Remove Dúvidas dos campos de categoria
   - Se o produto só tem Dúvidas: campos ficam vazios (sem categoria)
   - Se tem outras categorias além de Dúvidas: mantém as demais

Configuração:
  DRY_RUN = True   → apenas loga, não salva nada
  DRY_RUN = False  → aplica no Firestore
  ESTABLISHMENT_IDS → lista vazia = todos os estabelecimentos
"""

import json
import os
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

DRY_RUN = True   # Mude para False para aplicar
BATCH_LIMIT = 450

DUVIDAS_CAT_ID = 'KfmNz06cADwmzQI6UvSf'
DUVIDAS_SUB_ID = 'ZrXWoj6AFT0OpiIeAz4T'

# Deixe vazio para processar todos os estabelecimentos
ESTABLISHMENT_IDS = []


def init_firestore():
    if not firebase_admin._apps:
        creds_json = os.getenv("FIREBASE_CREDENTIALS_JSON", "").strip()
        if creds_json:
            cred = credentials.Certificate(json.loads(creds_json))
            firebase_admin.initialize_app(cred)
        elif os.getenv("FIREBASE_CREDENTIALS_PATH") and os.path.exists(
            os.getenv("FIREBASE_CREDENTIALS_PATH")
        ):
            cred = credentials.Certificate(os.getenv("FIREBASE_CREDENTIALS_PATH"))
            firebase_admin.initialize_app(cred)
        else:
            firebase_admin.initialize_app()
    return firestore.client()


def get_establishment_ids(db):
    if ESTABLISHMENT_IDS:
        return ESTABLISHMENT_IDS
    ids = set()
    try:
        doc = db.collection('Automacoes').document('config').get()
        if doc.exists:
            data = doc.to_dict() or {}
            for key in ('establishmentIds', 'establishment_ids', 'ids'):
                val = data.get(key)
                if isinstance(val, list):
                    ids.update(str(v) for v in val if v)
    except Exception as e:
        print(f"  [aviso] Nao foi possivel ler Automacoes/config: {e}")
    try:
        for doc in db.collection('estabelecimentos').list_documents():
            ids.add(doc.id)
    except Exception as e:
        print(f"  [aviso] Nao foi possivel listar estabelecimentos: {e}")
    return sorted(ids)


def get_duvidas_products(db, est_id):
    try:
        docs = list(
            db.collection('estabelecimentos')
            .document(est_id)
            .collection('Products')
            .where('categoriesIds', 'array_contains', DUVIDAS_CAT_ID)
            .stream()
        )
        return [(d.id, d.to_dict()) for d in docs]
    except Exception as e:
        print(f"    [erro] Ao buscar produtos: {e}")
        return []


def remove_duvidas(db, est_id, products):
    if DRY_RUN:
        print(f"    [DRY RUN] {len(products)} produtos seriam atualizados:")
        for pid, data in products[:10]:
            name = data.get('name', pid)
            cats = data.get('categoriesIds', [])
            subs = data.get('subcategoriesIds', [])
            print(f"      {name[:60]}")
            print(f"        cats={cats} | subs={subs}")
        if len(products) > 10:
            print(f"      ... e mais {len(products) - 10}")
        return 0, 0

    updated = errors = 0
    batch_ops = []

    for pid, data in products:
        cats    = [c for c in (data.get('categoriesIds')   or []) if c != DUVIDAS_CAT_ID]
        subs    = [s for s in (data.get('subcategoriesIds') or []) if s != DUVIDAS_SUB_ID]
        shelves = [s for s in (data.get('shelves')          or [])
                   if s.get('productCategoryId') != DUVIDAS_CAT_ID]
        shelf_ids = [s for s in (data.get('shelvesIds')    or [])
                     if not s.startswith(DUVIDAS_CAT_ID + '_')]

        batch_ops.append((pid, {
            'categoriesIds':    cats,
            'subcategoriesIds': subs,
            'shelves':          shelves,
            'shelvesIds':       shelf_ids,
        }))

    for i in range(0, len(batch_ops), BATCH_LIMIT):
        chunk = batch_ops[i:i + BATCH_LIMIT]
        db_batch = db.batch()
        for pid, update in chunk:
            ref = (db.collection('estabelecimentos')
                   .document(est_id)
                   .collection('Products')
                   .document(pid))
            db_batch.update(ref, update)
        try:
            db_batch.commit()
            updated += len(chunk)
        except Exception as e:
            print(f"    [erro] Batch commit falhou: {e}")
            errors += len(chunk)

    return updated, errors


def main():
    print("=== fix_duvidas.py ===")
    print(f"Modo: {'DRY RUN (sem alteracoes)' if DRY_RUN else 'APLICANDO ALTERACOES'}\n")

    db = init_firestore()
    est_ids = get_establishment_ids(db)
    print(f"{len(est_ids)} estabelecimentos encontrados\n")

    total_updated = total_errors = 0

    for est_id in est_ids:
        print(f"[{est_id}]")
        products = get_duvidas_products(db, est_id)

        if not products:
            print(f"  Nenhum produto em Duvidas\n")
            continue

        print(f"  {len(products)} produtos em Duvidas")
        updated, errors = remove_duvidas(db, est_id, products)

        if not DRY_RUN:
            print(f"  Atualizados: {updated} | Erros: {errors}")
            total_updated += updated
            total_errors  += errors
        print()

    print("=== RESULTADO ===")
    if not DRY_RUN:
        print(f"Produtos atualizados: {total_updated}")
        print(f"Erros: {total_errors}")
    else:
        print("(Dry run — nenhuma alteracao foi feita)")
        print("Mude DRY_RUN = False para aplicar.")


if __name__ == '__main__':
    main()
