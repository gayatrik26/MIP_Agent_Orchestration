import os
import matplotlib
matplotlib.use("Agg")      # 🔥 IMPORTANT: Prevents macOS window crash
import matplotlib.pyplot as plt

# Ensure directory exists
BASE_DIR = "data/report_charts"
os.makedirs(BASE_DIR, exist_ok=True)


def generate_line_chart(df, y="fat", x="timestamp", filename="line_chart.png"):
    """
    Generates a line chart.
    Defaults:
        x = timestamp
        y = fat
    """

    if x not in df.columns:
        raise ValueError(f"Column '{x}' not in DataFrame")

    if y not in df.columns:
        raise ValueError(f"Column '{y}' not found in DataFrame for line chart")

    df = df.sort_values(x)

    plt.figure(figsize=(10, 5))
    plt.plot(df[x], df[y])

    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f"{y.upper()} Trend Over Time")

    file_path = os.path.join(BASE_DIR, filename)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()

    return file_path


def generate_bar_chart(df, y="fat", x="timestamp", filename="bar_chart.png"):
    """
    Generates a bar chart.
    Defaults:
        x = timestamp
        y = fat
    """

    if x not in df.columns:
        raise ValueError(f"Column '{x}' not in DataFrame")

    if y not in df.columns:
        raise ValueError(f"Column '{y}' not found in DataFrame for bar chart")

    df = df.sort_values(x)

    plt.figure(figsize=(10, 5))
    plt.bar(df[x], df[y])

    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f"{y.upper()} Levels")

    file_path = os.path.join(BASE_DIR, filename)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()

    return file_path


def generate_fat_snf_ts_bar(df, filename="fat_snf_ts_bar.png"):
    """
    Grouped bar chart for FAT, SNF, TS.
    """

    required_cols = {"timestamp", "fat", "snf", "ts"}
    if not required_cols.issubset(df.columns):
        raise ValueError("DataFrame missing required columns: timestamp, fat, snf, ts")

    df = df.sort_values("timestamp")
    x = range(len(df))

    plt.figure(figsize=(12, 6))

    plt.bar([i - 0.2 for i in x], df["fat"], width=0.2, label="FAT")
    plt.bar(x, df["snf"], width=0.2, label="SNF")
    plt.bar([i + 0.2 for i in x], df["ts"], width=0.2, label="TS")

    plt.xlabel("timestamp")
    plt.ylabel("values")
    plt.title("FAT vs SNF vs TS Trend")
    plt.legend()

    file_path = os.path.join(BASE_DIR, filename)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()

    return file_path

def generate_supplier_score_bar(df_suppliers, filename="supplier_avg_scores.png"):
    """
    Bar chart: Supplier vs Average Score.
    Expects DataFrame with columns: supplier_id, avg_score
    """
    if not {"supplier_id", "avg_score"}.issubset(df_suppliers.columns):
        raise ValueError("DataFrame must contain 'supplier_id' and 'avg_score' columns.")

    plt.figure(figsize=(10, 5))
    plt.bar(df_suppliers["supplier_id"], df_suppliers["avg_score"])

    plt.xlabel("Supplier")
    plt.ylabel("Average Score")
    plt.title("Average Quality Score by Supplier")
    plt.xticks(rotation=45, ha="right")

    file_path = os.path.join(BASE_DIR, filename)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()

    return file_path


def generate_supplier_adulteration_bar(df_suppliers, filename="supplier_adulteration_rates.png"):
    """
    Bar chart: Supplier vs Adulteration Rate (%).
    Expects DataFrame with columns: supplier_id, adulteration_rate
    """
    if not {"supplier_id", "adulteration_rate"}.issubset(df_suppliers.columns):
        raise ValueError("DataFrame must contain 'supplier_id' and 'adulteration_rate' columns.")

    plt.figure(figsize=(10, 5))
    plt.bar(df_suppliers["supplier_id"], df_suppliers["adulteration_rate"])

    plt.xlabel("Supplier")
    plt.ylabel("Adulteration Rate (%)")
    plt.title("Adulteration Rate by Supplier")
    plt.xticks(rotation=45, ha="right")

    file_path = os.path.join(BASE_DIR, filename)
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()

    return file_path

def generate_adulteration_trend_chart(df, filename="monthly_adulteration_trend.png"):
    df2 = (
        df.groupby(df["timestamp"].dt.date)["is_adulterated"]
          .mean()
          .mul(100)
          .reset_index()
    )

    plt.figure(figsize=(10, 5))
    plt.plot(df2["timestamp"], df2["is_adulterated"])
    plt.title("Daily Adulteration Trend (%)")
    plt.xlabel("Date")
    plt.ylabel("Adulteration (%)")
    plt.xticks(rotation=45)

    path = os.path.join(BASE_DIR, filename)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    return path


def generate_adulteration_supplier_bar(df, filename="supplier_adulteration_rate.png"):
    sup = (
        df.groupby("supplier_id")["is_adulterated"]
        .mean().mul(100).reset_index()
        .sort_values("is_adulterated", ascending=False)
    )

    plt.figure(figsize=(12, 5))
    plt.bar(sup["supplier_id"], sup["is_adulterated"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Adulteration (%)")
    plt.title("Adulteration by Supplier (30 days)")

    path = os.path.join(BASE_DIR, filename)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    return path


def generate_adulteration_route_bar(df, filename="route_adulteration_rate.png"):
    rt = (
        df.groupby("route_id")["is_adulterated"]
        .mean().mul(100).reset_index()
        .sort_values("is_adulterated", ascending=False)
    )

    plt.figure(figsize=(12, 5))
    plt.bar(rt["route_id"], rt["is_adulterated"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Adulteration (%)")
    plt.title("Adulteration by Route (30 days)")

    path = os.path.join(BASE_DIR, filename)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    return path
