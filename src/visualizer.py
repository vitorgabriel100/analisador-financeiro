import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


REPORTS_DIR = Path("reports/charts")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def load_data(file_path: str) -> pd.DataFrame:
    """
    Carrega dados limpos
    """
    df = pd.read_csv(file_path, parse_dates=["date"])
    return df


def plot_expenses_by_category(df: pd.DataFrame) -> None:
    """
    Gera gráfico de barras de gastos por categoria
    """
    expenses = df[df["type"] == "saida"]

    grouped = (
        expenses
        .groupby("category")["value"]
        .sum()
        .abs()
        .sort_values(ascending=False)
    )

    plt.figure(figsize=(8, 5))
    grouped.plot(kind="bar")
    plt.title("Gastos por Categoria")
    plt.xlabel("Categoria")
    plt.ylabel("Valor (R$)")
    plt.tight_layout()

    output_path = REPORTS_DIR / "gastos_por_categoria.png"
    plt.savefig(output_path)
    plt.close()


def plot_monthly_expenses(df: pd.DataFrame) -> None:
    """
    Gera gráfico de linha de gastos mensais
    """
    expenses = df[df["type"] == "saida"].copy()
    expenses["month"] = expenses["date"].dt.to_period("M")

    monthly = (
        expenses
        .groupby("month")["value"]
        .sum()
        .abs()
    )

    plt.figure(figsize=(8, 5))
    monthly.plot(kind="line", marker="o")
    plt.title("Evolução Mensal de Gastos")
    plt.xlabel("Mês")
    plt.ylabel("Valor (R$)")
    plt.tight_layout()

    output_path = REPORTS_DIR / "gastos_mensais.png"
    plt.savefig(output_path)
    plt.close()


def generate_charts(file_path: str) -> None:
    """
    Pipeline de geração de gráficos
    """
    df = load_data(file_path)
    plot_expenses_by_category(df)
    plot_monthly_expenses(df)


if __name__ == "__main__":
    generate_charts("data/processed/transactions_clean.csv")
