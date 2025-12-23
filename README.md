# workspace

Projeto modular em Python com Streamlit (frontend) e FastAPI (webhook do bot) para integrar com Chatwoot e OpenAI.

## Estrutura
- `app/`: aplicação Streamlit com páginas (Bot Studio, Configurações, Relatórios, Gestão, Analytics).
- `src/`: lógica de negócio (DB, engine do bot, métricas/Chatwoot, relatórios).
- `data/raw/bot_config.db`: banco SQLite compartilhado entre o app e o serviço de webhook.
- `app/modules/bot/bot_start.py`: serviço FastAPI (webhook Chatwoot) usando o mesmo banco/configuração.

## Dependências
Instale com:
```bash
pip install -r requirements.txt
```

## Executando
- App Streamlit:
```bash
cd app
streamlit run main.py
```
- Serviço FastAPI (webhook):
```bash
uvicorn app.modules.bot.bot_start:app --reload --host 0.0.0.0 --port 8000
```

## Configuração
1. Crie um `.env` na raiz (mesmo nível de `README.md`) com ao menos:
   - `OPENAI_API_KEY=...`
   - `ALLOWED_INBOX_ID=...` (opcional, default 88473)
2. Acesse a página **Configurações** no app Streamlit para salvar parâmetros do modelo, Chatwoot e horários.
3. Use **Bot Studio** para gerenciar perfis/prompts.
4. **Relatórios** consulta mensagens/conversas via API do Chatwoot (usa as credenciais salvas).
5. **Analytics** exibe métricas e logs persistidos em `conversation_logs`.

## Banco de dados
O schema é criado automaticamente em `data/raw/bot_config.db`. Se já possui um arquivo existente, copie-o para esse caminho antes de rodar.
