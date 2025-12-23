#!/bin/bash

# Interrompe o script se houver qualquer erro
set -e

echo "🚀 Iniciando a criação da estrutura do projeto..."

# 1. Criação dos Arquivos na Raiz
echo "📄 Criando arquivos de configuração na raiz..."
touch .gitignore
touch requirements.txt

# 2. Criação dos Diretórios Principais
echo "📂 Criando diretórios..."
mkdir -p data/raw
mkdir -p src/{utils,bot,reports,management,analytics}
mkdir -p app/{components,pages}
mkdir -p app/modules/{bot,settings,reports,management,analytics}

# 3. Populando a pasta SRC (Backend)
echo "🐍 Criando arquivos do Backend (src)..."
touch src/__init__.py
touch src/utils/{__init__.py,database.py,db_init.py,formatters.py}
touch src/bot/{__init__.py,engine.py,rules.py}
touch src/reports/{__init__.py,generator.py}
touch src/management/{__init__.py,auth_service.py}
touch src/analytics/{__init__.py,metrics.py}

# 4. Populando a pasta APP (Frontend)
echo "🎨 Criando arquivos do Frontend (app)..."
touch app/main.py
touch app/components/{sidebar.py,cards.py}

# 5. Criando os Menus Laterais (Pages)
echo "📑 Criando menus de navegação..."
touch app/pages/01_Bot_Studio.py
touch app/pages/02_Configuracoes.py
touch app/pages/03_Relatorios.py
touch app/pages/04_Gestao.py
touch app/pages/05_Analytics.py

# 6. Criando o Conteúdo das Abas (Modules)
echo "🖥️  Criando interfaces dos módulos..."
touch app/modules/__init__.py

# Módulo Bot (Incluindo o novo bot_start.py)
touch app/modules/bot/{bot_start.py,settings.py,profiles.py,report.py,monitoring.py}

# Módulo Settings
touch app/modules/settings/system.py

# Módulo Reports
touch app/modules/reports/{operations.py,general.py}

# Módulo Management
touch app/modules/management/{user_groups.py,audit.py}

# Módulo Analytics
touch app/modules/analytics/conversations.py

echo "✅ Estrutura completa criada com sucesso!"
echo "   Execute 'tree' para visualizar a árvore de arquivos." 
