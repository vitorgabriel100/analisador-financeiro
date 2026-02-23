# Analisador Financeiro

Projeto em Python para análise de transações financeiras fictícias, com foco em
limpeza de dados, geração de indicadores e visualização gráfica.

> ⚠️ Todos os dados utilizados neste projeto são **100% fictícios**.

## Funcionalidades
- Limpeza e padronização de dados financeiros
- Geração de relatório no terminal
- Criação de gráficos:
  - Gastos por categoria
  - Gastos mensais
- Organização de dados em pipeline (raw → processed)
- Registro de logs do processo

## Estrutura do projeto
analisador-financeiro/
├─ data/
│ ├─ raw/transactions_raw.csv
│ └─ processed/transactions_clean.csv
├─ logs/cleaning.log
├─ reports/charts/
├─ src/
│ ├─ main.py
│ ├─ cleaner.py
│ ├─ analyzer.py
│ └─ visualizer.py
└─ requirements.txt

Saídas geradas

CSV tratado: data/processed/transactions_clean.csv

Gráficos em: reports/charts/

Log do processo: logs/cleaning.log

## Como executar
1. Instale as dependências:
```bash
pip install -r requirements.txt

2. Execute o pipeline:
python src/main.py

Objetivo do projeto

Este projeto foi desenvolvido com foco em aprendizado prático de análise de dados
e automação, servindo como base para futuras evoluções com Machine Learning.
