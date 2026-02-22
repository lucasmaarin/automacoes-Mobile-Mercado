# scripts — Utilitários de desenvolvimento

Esta pasta contém scripts auxiliares e ferramentas locais usados durante desenvolvimento e manutenção do projeto. Eles NÃO fazem parte do runtime da aplicação web principal e geralmente dependem de credenciais locais e variáveis de ambiente.

Arquivos principais
- `firestore_for_json.py` — Explorador avançado da estrutura do Firestore (Flask + WebSocket). Uso para inspeção e exportação.
- `firestore_simple.py` — Versão simplificada do explorador (API simples para testes rápidos).
- `nome_cat.py` / `nome_cats.py` — Scripts/serviços para padronizar nomes de produtos usando OpenAI e atualizar documentos no Firestore.

Observações importantes
- Estes scripts normalmente precisam de credenciais do Firebase e da chave OpenAI definidas nas variáveis de ambiente:
  - `FIREBASE_CREDENTIALS_PATH` — caminho para o JSON de credenciais do Firebase (opcional se ADC estiver configurado).
  - `OPENAI_API_KEY` — chave da OpenAI (quando aplicável).
- Os arquivos `*.py` desta pasta estão ignorados por `.gitignore` (padrão `scripts/*.py`) para evitar commitar credenciais ou execuções locais. O `README.md` nesta pasta é rastreado e documenta o conteúdo.

Como usar (exemplo rápido)
1. Ative seu ambiente virtual:

```powershell
& .\venv\Scripts\Activate.ps1
```

2. Exporte as variáveis de ambiente ou crie um `.env` com as chaves necessárias.

3. Execute um script de inspeção (exemplo):

```powershell
python scripts\firestore_simple.py
```

Recomendações
- Antes de modificar ou excluir qualquer script, faça um commit em uma branch separada ou faça um ZIP de backup da pasta `scripts/`.
- Para mover algum utilitário para produção, extraia apenas as funções necessárias e integre-as no código principal com testes apropriados.

Se quiser, eu posso:
- Remover definitivamente esses scripts (após backup/commit).
- Criar uma branch de backup com os scripts antes de qualquer exclusão.
- Converter um dos scripts em um módulo reutilizável e testável dentro do app.

---
Gerado em: 22 de fevereiro de 2026
