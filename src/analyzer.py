import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Carrega o CSV de transações já limpas
    """
    df = pd.read_csv(file_path, parse_dates=["date"])
    return df


def calculate_totals(df: pd.DataFrame) -> dict:
    """
    Calcula totais gerais
    """
    total_income = df[df["type"] == "entrada"]["value"].sum()
    total_expense = df[df["type"] == "saida"]["value"].sum()
    balance = df["value"].sum()

    return {
        "total_income": total_income,
        "total_expense": total_expense,
        "balance": balance
    }


def expenses_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa gastos por categoria
    """
    expenses = df[df["type"] == "saida"]
    grouped = (
        expenses
        .groupby("category")["value"]
        .sum()
        .abs()
        .sort_values(ascending=False)
        .reset_index()
    )
    return grouped


def monthly_average_expense(df: pd.DataFrame) -> float:
    """
    Calcula a média mensal de gastos
    """
    expenses = df[df["type"] == "saida"].copy()
    expenses["month"] = expenses["date"].dt.to_period("M")

    monthly_total = expenses.groupby("month")["value"].sum().abs()
    return monthly_total.mean()


def generate_report(df: pd.DataFrame) -> None:
    """
    Gera e exibe o relatório financeiro
    """
    totals = calculate_totals(df)
    category_expenses = expenses_by_category(df)
    avg_monthly_expense = monthly_average_expense(df)

    print("\n RELATÓRIO FINANCEIRO\n")
    print(f"Total de Entradas: R$ {totals['total_income']:.2f}")
    print(f"Total de Saídas:   R$ {totals['total_expense']:.2f}")
    print(f"Saldo Final:      R$ {totals['balance']:.2f}")

    print("\n Gastos por Categoria:")
    print(category_expenses.to_string(index=False))

    print(f"\n Média Mensal de Gastos: R$ {avg_monthly_expense:.2f}")


def main():
    file_path = "data/processed/transactions_clean.csv"
    df = load_data(file_path)
    generate_report(df)


if __name__ == "__main__":
    main()
