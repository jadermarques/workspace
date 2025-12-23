#!/bin/bash

# Define que o script deve parar se houver algum erro
set -e

echo "📂 Criando estrutura de diretórios do projeto..."

# 1. Cria a pasta de dados (incluindo subpasta raw)
mkdir -p data/raw

# 2. Cria a estrutura do Backend (src)
# O uso de chaves {} permite criar várias pastas de uma vez
mkdir -p src/{utils,bot,reports,management,analytics,settings}

# 3. Cria a estrutura do Frontend (app)
mkdir -p app/{components,pages}

# 4. Cria a estrutura dos Módulos Visuais (conteúdo das abas)
mkdir -p app/modules/{bot,settings,reports,management,analytics}

echo "✅ Estrutura de diretórios criada com sucesso!"
echo "   Você pode verificar com o comando: tree"
