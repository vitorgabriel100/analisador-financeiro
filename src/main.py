from pathlib import Path
from src.cleaner import run_cleaning_pipeline
from src.analyzer import load_data, generate_report
from src.visualizer import generate_charts

# Diretório raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = BASE_DIR / "data/raw/transactions_raw.csv"
CLEAN_DATA_PATH = BASE_DIR / "data/processed/transactions_clean.csv"


def main():
    print(" Iniciando pipeline financeiro...\n")

    # Limpeza de dados
    print("Limpando dados...")
    run_cleaning_pipeline(
        input_path=str(RAW_DATA_PATH),
        output_path=str(CLEAN_DATA_PATH)
    )

    # Confirmação visual (debug profissional)
    print(f" CSV limpo gerado em:\n {CLEAN_DATA_PATH}\n")

    # Análise
    print(" Gerando análise financeira...")
    df = load_data(str(CLEAN_DATA_PATH))
    generate_report(df)

    # Visualização
    print("\n Gerando gráficos...")
    generate_charts(str(CLEAN_DATA_PATH))

    print("\n Pipeline finalizado com sucesso!")


if __name__ == "__main__":
    main()
