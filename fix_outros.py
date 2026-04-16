"""
Script para remover a categoria 'outros'/'outrosOutros' de produtos em estabelecimentos
onde essa categoria não existe.

Para cada estabelecimento:
1. Verifica se a categoria 'outros' existe em ProductCategories
2. Se NÃO existir, busca todos os produtos com categoriesIds contendo 'outros'
3. Limpa os campos de categoria/subcategoria desses produtos

Configuração:
  DRY_RUN = True   → apenas loga, não salva nada
  DRY_RUN = False  → aplica as alterações no Firestore
  ESTABLISHMENT_IDS → lista vazia = todos os estabelecimentos de Automacoes/config + padrão
"""

import json
import os
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

DRY_RUN = True  # Mude para False para aplicar as alterações
OUTROS_CAT_ID = 'outros'
OUTROS_SUB_ID = 'outrosOutros'
BATCH_LIMIT = 450

# Deixe vazio para processar todos os estabelecimentos conhecidos
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

    # Padrão: lê IDs de Automacoes/config
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

    # Complementa lendo os documentos de estabelecimentos diretamente
    try:
        for doc in db.collection('estabelecimentos').list_documents():
            ids.add(doc.id)
    except Exception as e:
        print(f"  [aviso] Nao foi possivel listar estabelecimentos: {e}")

    return sorted(ids)


def outros_category_exists(db, est_id):
    """Retorna True se a categoria 'outros' existe em ProductCategories do estabelecimento."""
    try:
        doc = (db.collection('estabelecimentos')
               .document(est_id)
               .collection('ProductCategories')
               .document(OUTROS_CAT_ID)
               .get())
        return doc.exists
    except Exception as e:
        print(f"    [erro] Ao verificar categoria outros em {est_id}: {e}")
        return True  # conservador: assume que existe se não conseguiu verificar


def get_products_with_outros(db, est_id):
    """Busca todos os produtos onde categoriesIds contém 'outros'."""
    try:
        query = (db.collection('estabelecimentos')
                 .document(est_id)
                 .collection('Products')
                 .where('categoriesIds', 'array_contains', OUTROS_CAT_ID))
        docs = list(query.stream())
        return [(d.id, d.to_dict()) for d in docs]
    except Exception as e:
        print(f"    [erro] Ao buscar produtos com outros em {est_id}: {e}")
        return []


def clear_outros_fields(db, est_id, products):
    """
    Remove a categoria 'outros' dos campos de categoria do produto.
    Se for a única categoria, zera todos os campos.
    Se houver outras categorias, mantém as demais.
    """
    if DRY_RUN:
        print(f"    [DRY RUN] {len(products)} produtos seriam limpos")
        for pid, data in products[:5]:
            cats = data.get('categoriesIds', [])
            subs = data.get('subcategoriesIds', [])
            print(f"      {pid} | cats={cats} | subs={subs}")
        if len(products) > 5:
            print(f"      ... e mais {len(products) - 5}")
        return 0, 0

    updated = 0
    errors = 0
    batch_ops = []

    for pid, data in products:
        cats = [c for c in (data.get('categoriesIds') or []) if c != OUTROS_CAT_ID]
        subs = [s for s in (data.get('subcategoriesIds') or []) if s != OUTROS_SUB_ID]
        shelves = [s for s in (data.get('shelves') or [])
                   if s.get('productCategoryId') != OUTROS_CAT_ID]
        shelf_ids = [s for s in (data.get('shelvesIds') or [])
                     if not s.startswith(OUTROS_CAT_ID + '_')]

        update = {
            'categoriesIds': cats,
            'subcategoriesIds': subs,
            'shelves': shelves,
            'shelvesIds': shelf_ids,
        }
        batch_ops.append((pid, update))

    # Commit em lotes de BATCH_LIMIT
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
    print("=== fix_outros.py ===")
    print(f"Modo: {'DRY RUN (sem alteracoes)' if DRY_RUN else 'APLICANDO ALTERACOES'}\n")

    db = init_firestore()
    est_ids = get_establishment_ids(db)
    print(f"{len(est_ids)} estabelecimentos encontrados\n")

    total_updated = 0
    total_errors = 0
    total_skipped_ests = 0

    for est_id in est_ids:
        print(f"[{est_id}]")

        if outros_category_exists(db, est_id):
            print(f"  Categoria 'outros' existe — estabelecimento ignorado\n")
            total_skipped_ests += 1
            continue

        print(f"  Categoria 'outros' NAO existe — buscando produtos...")
        products = get_products_with_outros(db, est_id)

        if not products:
            print(f"  Nenhum produto com categoria 'outros' encontrado\n")
            continue

        print(f"  {len(products)} produtos encontrados")
        updated, errors = clear_outros_fields(db, est_id, products)

        if not DRY_RUN:
            print(f"  Atualizados: {updated} | Erros: {errors}")
            total_updated += updated
            total_errors += errors
        print()

    print("=== RESULTADO ===")
    print(f"Estabelecimentos com 'outros' (ignorados): {total_skipped_ests}")
    if not DRY_RUN:
        print(f"Produtos limpos: {total_updated}")
        print(f"Erros: {total_errors}")
    else:
        print("(Dry run — nenhuma alteracao foi feita)")


if __name__ == '__main__':
    main()
