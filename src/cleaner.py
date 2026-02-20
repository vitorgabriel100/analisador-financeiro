import pandas as pd
import logging
from pathlib import Path


# =========================
# Configuração de logging
# =========================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "cleaning.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =========================
# Carregamento
# =========================
def load_raw_data(file_path: str) -> pd.DataFrame:
    """
    Carrega dados brutos de forma robusta
    """
    logging.info("Carregando dados brutos")

    df = pd.read_csv(
        file_path,
        encoding="latin1",
        sep=None,              # detecta ; , \t automaticamente
        engine="python"
    )

    logging.info(f"Colunas brutas: {list(df.columns)}")
    return df


# =========================
# Normalização
# =========================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("\ufeff", "", regex=False)
    )

    logging.info(f"Colunas normalizadas: {list(df.columns)}")
    return df


# =========================
# Limpeza de datas
# =========================
def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns:
        raise KeyError(
            f"Coluna 'date' não encontrada. Colunas atuais: {list(df.columns)}"
        )

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
        dayfirst=True
    )

    invalid_dates = df["date"].isna().sum()
    if invalid_dates > 0:
        logging.warning(f"{invalid_dates} datas inválidas encontradas")

    return df


# =========================
# Limpeza de tipo
# =========================
def clean_type(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns:
        raise KeyError("Coluna 'type' não encontrada")

    df["type"] = (
        df["type"]
        .astype(str)
        .str.lower()
        .str.strip()
        .replace({
            "saída": "saida",
            "entrda": "entrada"
        })
    )
    return df


# =========================
# Limpeza de valores
# =========================
def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata valores monetários considerando formatos brasileiros e internacionais
    """
    def parse_value(v):
        if pd.isna(v):
            return None

        v = str(v).strip()

        # Remove símbolo e espaços
        v = v.replace("R$", "").replace(" ", "")

        # Caso tenha vírgula como separador decimal (pt-BR)
        if "," in v:
            v = v.replace(".", "").replace(",", ".")
        # Caso padrão internacional (decimal com ponto)
        else:
            v = v

        try:
            return float(v)
        except ValueError:
            return None

    df["value"] = df["value"].apply(parse_value)

    null_values = df["value"].isna().sum()
    if null_values > 0:
        logging.warning(f"{null_values} valores inválidos encontrados")

    # Corrigir sinal conforme tipo
    entradas_erradas = df[(df["type"] == "entrada") & (df["value"] < 0)]
    saidas_erradas = df[(df["type"] == "saida") & (df["value"] > 0)]

    if not entradas_erradas.empty:
        df.loc[entradas_erradas.index, "value"] *= -1

    if not saidas_erradas.empty:
        df.loc[saidas_erradas.index, "value"] *= -1

    return df


# =========================
# Categorias
# =========================
def clean_categories(df: pd.DataFrame) -> pd.DataFrame:
    if "category" not in df.columns:
        df["category"] = "outros"
        logging.warning("Coluna 'category' ausente, criada automaticamente")

    df["category"] = df["category"].fillna("outros")
    return df


# =========================
# Remoção de inválidos
# =========================
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    initial_size = len(df)

    df = df.dropna(subset=["date", "value"])

    removed = initial_size - len(df)
    if removed > 0:
        logging.warning(f"{removed} linhas removidas por dados críticos ausentes")

    return df


# =========================
# Salvamento
# =========================
def save_clean_data(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f"Dados limpos salvos em {output_path}")


# =========================
# Pipeline
# =========================
def run_cleaning_pipeline(input_path: str, output_path: str) -> None:
    logging.info("Iniciando pipeline de limpeza")

    df = load_raw_data(input_path)
    df = normalize_columns(df)
    df = clean_dates(df)
    df = clean_type(df)
    df = clean_values(df)
    df = clean_categories(df)
    df = remove_invalid_rows(df)
    save_clean_data(df, output_path)


if __name__ == "__main__":
    run_cleaning_pipeline(
        input_path="data/raw/transactions_raw.csv",
        output_path="data/processed/transactions_clean.csv"
    )
