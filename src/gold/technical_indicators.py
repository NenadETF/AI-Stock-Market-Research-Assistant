from databricks.connect import DatabricksSession

import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_TABLE = "workspace.gold.stock_daily_metrics"

GOLD_SCHEMA = "workspace.gold"
TARGET_TABLE = "workspace.gold.technical_indicators"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 90)
print("GOLD LAYER - TECHNICAL INDICATORS")
print("=" * 90)

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")


# =============================================================================
# 1. CREATE GOLD SCHEMA
# =============================================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
    """
)

print(f"[OK] Gold schema spremna: {GOLD_SCHEMA}")


# =============================================================================
# 2. LOAD DAILY GOLD METRICS
# =============================================================================

source_df = spark.table(SOURCE_TABLE)

source_count = source_df.count()

print()
print("GOLD SOURCE")
print("-" * 90)
print(f"Broj source redova: {source_count}")


if source_count == 0:
    raise ValueError(
        f"Source tabela {SOURCE_TABLE} je prazna."
    )


# =============================================================================
# 3. SELECT AND NORMALIZE INPUT COLUMNS
# =============================================================================

base_df = source_df.select(
    F.col("ticker").cast("string").alias("ticker"),
    F.col("date").cast("date").alias("date"),
    F.col("close").cast("double").alias("close"),
    F.col("volume").cast("double").alias("volume"),
    F.col("daily_return").cast("double").alias("daily_return"),
    F.col("daily_return_pct").cast("double").alias("daily_return_pct"),
    F.col("volume_ratio").cast("double").alias("volume_ratio"),

    F.col("annualized_volatility_30d_pct")
    .cast("double")
    .alias("annualized_volatility_30d_pct")
)


# =============================================================================
# 4. OUTPUT SCHEMA
# =============================================================================

technical_schema = StructType([

    StructField("ticker", StringType(), False),
    StructField("date", DateType(), False),

    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),

    StructField("daily_return", DoubleType(), True),
    StructField("daily_return_pct", DoubleType(), True),

    StructField("volume_ratio", DoubleType(), True),

    StructField(
        "annualized_volatility_30d_pct",
        DoubleType(),
        True
    ),

    StructField("sma_20", DoubleType(), True),
    StructField("sma_50", DoubleType(), True),

    StructField("ema_12", DoubleType(), True),
    StructField("ema_26", DoubleType(), True),

    StructField("rsi_14", DoubleType(), True),

    StructField("macd", DoubleType(), True),
    StructField("macd_signal", DoubleType(), True),
    StructField("macd_histogram", DoubleType(), True),

    StructField("bollinger_middle", DoubleType(), True),
    StructField("bollinger_upper", DoubleType(), True),
    StructField("bollinger_lower", DoubleType(), True),

    StructField(
        "bollinger_bandwidth_pct",
        DoubleType(),
        True
    ),

    StructField(
        "bollinger_position",
        DoubleType(),
        True
    ),

    StructField(
        "momentum_10d_pct",
        DoubleType(),
        True
    ),

    StructField(
        "price_vs_sma20_pct",
        DoubleType(),
        True
    ),

    StructField(
        "price_vs_sma50_pct",
        DoubleType(),
        True
    ),

    StructField("trend_signal", StringType(), True),
    StructField("rsi_signal", StringType(), True),
    StructField("macd_state", StringType(), True)
])


# =============================================================================
# 5. WILDER RSI
#
# Standardni RSI(14) koristi Wilder smoothing.
#
# Prvi RSI:
#   average gain/loss prvih 14 promjena
#
# Nakon toga:
#
# avg_gain =
# ((previous_avg_gain * 13) + current_gain) / 14
#
# avg_loss =
# ((previous_avg_loss * 13) + current_loss) / 14
# =============================================================================

def calculate_wilder_rsi(close, period=14):

    close = pd.Series(
        close,
        dtype="float64"
    )

    delta = close.diff()

    gains = delta.clip(lower=0)

    losses = -delta.clip(upper=0)

    rsi = pd.Series(
        np.nan,
        index=close.index,
        dtype="float64"
    )

    if len(close) <= period:
        return rsi


    avg_gain = gains.iloc[1:period + 1].mean()

    avg_loss = losses.iloc[1:period + 1].mean()


    def calculate_rsi_value(gain, loss):

        if loss == 0:

            if gain == 0:
                return 50.0

            return 100.0

        rs = gain / loss

        return 100.0 - (
            100.0 / (1.0 + rs)
        )


    rsi.iloc[period] = calculate_rsi_value(
        avg_gain,
        avg_loss
    )


    for i in range(
        period + 1,
        len(close)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains.iloc[i]
        ) / period


        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses.iloc[i]
        ) / period


        rsi.iloc[i] = calculate_rsi_value(
            avg_gain,
            avg_loss
        )


    return rsi


# =============================================================================
# 6. TECHNICAL INDICATOR FUNCTION
#
# Funkcija prima podatke jednog tickera sortirane hronoloski.
#
# Izracunava:
#
# - SMA20
# - SMA50
# - EMA12
# - EMA26
# - RSI14
# - MACD
# - MACD Signal
# - MACD Histogram
# - Bollinger Bands
# - Bollinger Bandwidth
# - Bollinger Position
# - Momentum 10D
# - Price vs SMA20
# - Price vs SMA50
# - Trend signal
# - RSI signal
# - MACD state
# =============================================================================

def calculate_indicators(pdf):

    pdf = pdf.copy()

    pdf = pdf.sort_values(
        "date"
    ).reset_index(drop=True)


    pdf["date"] = (
        pd.to_datetime(pdf["date"])
        .dt.date
    )


    close = pdf["close"].astype(float)


    # =========================================================================
    # SMA 20
    # =========================================================================

    pdf["sma_20"] = (
        close
        .rolling(
            window=20,
            min_periods=20
        )
        .mean()
    )


    # =========================================================================
    # SMA 50
    # =========================================================================

    pdf["sma_50"] = (
        close
        .rolling(
            window=50,
            min_periods=50
        )
        .mean()
    )


    # =========================================================================
    # EMA 12
    #
    # adjust=False daje recursive EMA.
    # =============================================================================

    pdf["ema_12"] = (
        close
        .ewm(
            span=12,
            adjust=False,
            min_periods=12
        )
        .mean()
    )


    # =========================================================================
    # EMA 26
    # =========================================================================

    pdf["ema_26"] = (
        close
        .ewm(
            span=26,
            adjust=False,
            min_periods=26
        )
        .mean()
    )


    # =========================================================================
    # RSI 14
    # =========================================================================

    pdf["rsi_14"] = calculate_wilder_rsi(
        close,
        period=14
    )


    # =========================================================================
    # MACD
    #
    # MACD = EMA12 - EMA26
    # =========================================================================

    pdf["macd"] = (
        pdf["ema_12"]
        - pdf["ema_26"]
    )


    # =========================================================================
    # MACD SIGNAL
    #
    # 9-period EMA MACD linije.
    # =========================================================================

    pdf["macd_signal"] = (
        pdf["macd"]
        .ewm(
            span=9,
            adjust=False,
            min_periods=9
        )
        .mean()
    )


    # =========================================================================
    # MACD HISTOGRAM
    #
    # MACD histogram = MACD - signal
    # =========================================================================

    pdf["macd_histogram"] = (
        pdf["macd"]
        - pdf["macd_signal"]
    )


    # =========================================================================
    # BOLLINGER BANDS
    #
    # Middle = SMA20
    # Upper  = SMA20 + 2 * STD20
    # Lower  = SMA20 - 2 * STD20
    # =========================================================================

    rolling_std_20 = (
        close
        .rolling(
            window=20,
            min_periods=20
        )
        .std()
    )


    pdf["bollinger_middle"] = (
        pdf["sma_20"]
    )


    pdf["bollinger_upper"] = (
        pdf["bollinger_middle"]
        + 2 * rolling_std_20
    )


    pdf["bollinger_lower"] = (
        pdf["bollinger_middle"]
        - 2 * rolling_std_20
    )


    # =========================================================================
    # BOLLINGER BANDWIDTH
    #
    # Pokazuje sirinu Bollinger Bands u procentima.
    #
    # Veca vrijednost -> veca volatilnost.
    # =========================================================================

    pdf["bollinger_bandwidth_pct"] = np.where(

        (
            pdf["bollinger_middle"].notna()
            &
            (pdf["bollinger_middle"] != 0)
        ),

        (
            (
                pdf["bollinger_upper"]
                - pdf["bollinger_lower"]
            )
            / pdf["bollinger_middle"]
        ) * 100,

        np.nan
    )


    # =========================================================================
    # BOLLINGER POSITION
    #
    # 0.0 -> cijena na lower band
    # 0.5 -> cijena u sredini
    # 1.0 -> cijena na upper band
    #
    # Vrijednost moze biti:
    # < 0 ako cijena probije donji band
    # > 1 ako cijena probije gornji band
    # =========================================================================

    band_width = (
        pdf["bollinger_upper"]
        - pdf["bollinger_lower"]
    )


    valid_band = (
        band_width.notna()
        &
        (band_width != 0)
    )


    pdf["bollinger_position"] = np.nan


    pdf.loc[
        valid_band,
        "bollinger_position"
    ] = (
        (
            close[valid_band]
            - pdf.loc[
                valid_band,
                "bollinger_lower"
            ]
        )
        /
        band_width[valid_band]
    )


    # =========================================================================
    # 10-PERIOD MOMENTUM
    #
    # Promjena cijene u odnosu na 10 trading zapisa ranije.
    # =========================================================================

    pdf["momentum_10d_pct"] = (
        (
            close
            / close.shift(10)
        )
        - 1
    ) * 100


    # =========================================================================
    # PRICE VS SMA20
    # =========================================================================

    pdf["price_vs_sma20_pct"] = np.where(

        (
            pdf["sma_20"].notna()
            &
            (pdf["sma_20"] != 0)
        ),

        (
            (
                close
                / pdf["sma_20"]
            )
            - 1
        ) * 100,

        np.nan
    )


    # =========================================================================
    # PRICE VS SMA50
    # =========================================================================

    pdf["price_vs_sma50_pct"] = np.where(

        (
            pdf["sma_50"].notna()
            &
            (pdf["sma_50"] != 0)
        ),

        (
            (
                close
                / pdf["sma_50"]
            )
            - 1
        ) * 100,

        np.nan
    )


    # =========================================================================
    # TREND SIGNAL
    #
    # BULLISH:
    # close > SMA20 > SMA50
    #
    # BEARISH:
    # close < SMA20 < SMA50
    #
    # ostalo:
    # NEUTRAL
    # =========================================================================

    bullish = (
        (close > pdf["sma_20"])
        &
        (pdf["sma_20"] > pdf["sma_50"])
    )


    bearish = (
        (close < pdf["sma_20"])
        &
        (pdf["sma_20"] < pdf["sma_50"])
    )


    pdf["trend_signal"] = np.select(
        [
            bullish,
            bearish
        ],
        [
            "BULLISH",
            "BEARISH"
        ],
        default="NEUTRAL"
    )


    pdf.loc[
        pdf["sma_50"].isna(),
        "trend_signal"
    ] = None


    # =========================================================================
    # RSI SIGNAL
    #
    # >= 70 -> OVERBOUGHT
    # <= 30 -> OVERSOLD
    # ostalo -> NEUTRAL
    # =========================================================================

    pdf["rsi_signal"] = np.select(
        [
            pdf["rsi_14"] >= 70,
            pdf["rsi_14"] <= 30
        ],
        [
            "OVERBOUGHT",
            "OVERSOLD"
        ],
        default="NEUTRAL"
    )


    pdf.loc[
        pdf["rsi_14"].isna(),
        "rsi_signal"
    ] = None


    # =========================================================================
    # MACD STATE
    #
    # MACD > Signal -> BULLISH
    # MACD < Signal -> BEARISH
    # =========================================================================

    pdf["macd_state"] = np.select(
        [
            pdf["macd"] > pdf["macd_signal"],
            pdf["macd"] < pdf["macd_signal"]
        ],
        [
            "BULLISH",
            "BEARISH"
        ],
        default="NEUTRAL"
    )


    pdf.loc[
        pdf["macd_signal"].isna(),
        "macd_state"
    ] = None


    # =========================================================================
    # FINAL COLUMN ORDER
    # =========================================================================

    return pdf[[
        "ticker",
        "date",

        "close",
        "volume",

        "daily_return",
        "daily_return_pct",

        "volume_ratio",
        "annualized_volatility_30d_pct",

        "sma_20",
        "sma_50",

        "ema_12",
        "ema_26",

        "rsi_14",

        "macd",
        "macd_signal",
        "macd_histogram",

        "bollinger_middle",
        "bollinger_upper",
        "bollinger_lower",

        "bollinger_bandwidth_pct",
        "bollinger_position",

        "momentum_10d_pct",

        "price_vs_sma20_pct",
        "price_vs_sma50_pct",

        "trend_signal",
        "rsi_signal",
        "macd_state"
    ]]


# =============================================================================
# 7. COPY GOLD DATA TO LOCAL PANDAS
#
# Dataset trenutno ima samo 3500 redova.
#
# Rekurzivni indikatori kao EMA, RSI i MACD prirodno se racunaju
# sekvencijalno po tickeru.
#
# Ne koristimo applyInPandas jer bi on zahtijevao pokretanje remote
# Python sandboxa na Databricks compute-u.
#
# Tok:
#
# Spark / Delta
#     ->
# local Pandas
#     ->
# technical indicators
#     ->
# Spark
#     ->
# Delta
# =============================================================================

print()
print("Prebacujem Gold market podatke u lokalni Pandas DataFrame...")


base_pdf = (
    base_df
    .orderBy(
        "ticker",
        "date"
    )
    .toPandas()
)


print(f"[OK] Pandas redova: {len(base_pdf)}")


if len(base_pdf) != source_count:

    raise ValueError(
        f"Broj Pandas redova ({len(base_pdf)}) "
        f"nije jednak source broju ({source_count})."
    )


# =============================================================================
# 8. CALCULATE INDICATORS FOR EACH TICKER
# =============================================================================

ticker_results = []


for ticker, ticker_pdf in base_pdf.groupby(
    "ticker",
    sort=True
):

    print(
        f"Racunam tehnicke indikatore za {ticker} "
        f"({len(ticker_pdf)} redova)..."
    )


    calculated_pdf = calculate_indicators(
        ticker_pdf
    )


    ticker_results.append(
        calculated_pdf
    )


# =============================================================================
# 9. COMBINE TICKER RESULTS
# =============================================================================

if not ticker_results:

    raise ValueError(
        "Nije izracunat nijedan ticker."
    )


technical_pdf = pd.concat(
    ticker_results,
    ignore_index=True
)


# =============================================================================
# 10. VALIDATE LOCAL RESULT COUNT
# =============================================================================

if len(technical_pdf) != source_count:

    raise ValueError(
        f"Broj izracunatih redova ({len(technical_pdf)}) "
        f"nije jednak source broju ({source_count})."
    )


print(
    f"[OK] Tehnicki indikatori izracunati: "
    f"{len(technical_pdf)} redova"
)


# =============================================================================
# 11. NORMALIZE PANDAS VALUES
#
# Uklanjamo eventualne:
#
# +inf
# -inf
#
# i pretvaramo ih u NaN.
#
# Kasnije ce biti pretvoreni u Spark NULL.
# =============================================================================

technical_numeric_columns = [

    "sma_20",
    "sma_50",

    "ema_12",
    "ema_26",

    "rsi_14",

    "macd",
    "macd_signal",
    "macd_histogram",

    "bollinger_middle",
    "bollinger_upper",
    "bollinger_lower",

    "bollinger_bandwidth_pct",
    "bollinger_position",

    "momentum_10d_pct",

    "price_vs_sma20_pct",
    "price_vs_sma50_pct"
]


technical_pdf[
    technical_numeric_columns
] = technical_pdf[
    technical_numeric_columns
].replace(
    [np.inf, -np.inf],
    np.nan
)


# =============================================================================
# 12. CONVERT LOCAL PANDAS RESULT BACK TO SPARK
# =============================================================================

print()
print("Konvertujem tehnicke indikatore nazad u Spark DataFrame...")


technical_df = spark.createDataFrame(
    technical_pdf,
    schema=technical_schema
)


print(
    "[OK] Pandas rezultat konvertovan nazad "
    "u Spark DataFrame"
)


# =============================================================================
# 13. NORMALIZE NaN -> NULL IN SPARK
# =============================================================================

for column_name in technical_numeric_columns:

    technical_df = technical_df.withColumn(
        column_name,

        F.when(
            F.isnan(
                F.col(column_name)
            ),
            F.lit(None).cast("double")
        )
        .otherwise(
            F.col(column_name)
        )
    )


# =============================================================================
# 14. ROUND VALUES
# =============================================================================

round_columns = {

    "sma_20": 4,
    "sma_50": 4,

    "ema_12": 4,
    "ema_26": 4,

    "rsi_14": 4,

    "macd": 6,
    "macd_signal": 6,
    "macd_histogram": 6,

    "bollinger_middle": 4,
    "bollinger_upper": 4,
    "bollinger_lower": 4,

    "bollinger_bandwidth_pct": 4,
    "bollinger_position": 4,

    "momentum_10d_pct": 4,

    "price_vs_sma20_pct": 4,
    "price_vs_sma50_pct": 4
}


for column_name, decimals in round_columns.items():

    technical_df = technical_df.withColumn(
        column_name,

        F.round(
            F.col(column_name),
            decimals
        )
    )


# =============================================================================
# 15. PROCESSING TIMESTAMP
# =============================================================================

technical_df = technical_df.withColumn(
    "_gold_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 16. WRITE GOLD DELTA TABLE
#
# Koristimo full refresh zato sto tehnicki indikatori zavise od istorije.
# =============================================================================

(
    technical_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


print()
print(f"[OK] Gold tabela kreirana: {TARGET_TABLE}")


# =============================================================================
# 17. BASIC VALIDATION
# =============================================================================

result_df = spark.table(TARGET_TABLE)

result_count = result_df.count()

ticker_count = (
    result_df
    .select("ticker")
    .distinct()
    .count()
)


print()
print("=" * 90)
print("TECHNICAL INDICATORS SUMMARY")
print("=" * 90)

print(f"Source redova: {source_count}")
print(f"Target redova: {result_count}")
print(f"Broj tickera:  {ticker_count}")


if result_count != source_count:

    raise ValueError(
        f"Target ima {result_count} redova, "
        f"a source ima {source_count}."
    )


# =============================================================================
# 18. PERIOD
# =============================================================================

print()
print("TECHNICAL INDICATORS PERIOD:")


(
    result_df
    .select(
        F.min("date").alias("najstariji"),
        F.max("date").alias("najnoviji")
    )
    .show()
)


# =============================================================================
# 19. ROW COUNT PER TICKER
# =============================================================================

print()
print("BROJ REDOVA PO TICKERU:")


(
    result_df
    .groupBy("ticker")
    .count()
    .orderBy("ticker")
    .show(
        truncate=False
    )
)


# =============================================================================
# 20. LATEST TECHNICAL INDICATORS
# =============================================================================

latest_window = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("date").desc()
    )
)


latest_df = (
    result_df
    .withColumn(
        "_rn",
        F.row_number().over(latest_window)
    )
    .filter(
        F.col("_rn") == 1
    )
    .drop("_rn")
)


print()
print("NAJNOVIJI TEHNICKI INDIKATORI:")


(
    latest_df
    .select(
        "ticker",
        "date",
        "close",

        "sma_20",
        "sma_50",

        "ema_12",
        "ema_26",

        "rsi_14",

        "macd",
        "macd_signal",
        "macd_histogram",

        "momentum_10d_pct",

        "trend_signal",
        "rsi_signal",
        "macd_state"
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 21. NULL STATISTICS
# =============================================================================

print()
print("NULL STATISTIKA TEHNICKIH INDIKATORA:")


result_df.select(

    F.sum(
        F.when(
            F.col("sma_20").isNull(),
            1
        ).otherwise(0)
    ).alias("null_sma20"),


    F.sum(
        F.when(
            F.col("sma_50").isNull(),
            1
        ).otherwise(0)
    ).alias("null_sma50"),


    F.sum(
        F.when(
            F.col("ema_12").isNull(),
            1
        ).otherwise(0)
    ).alias("null_ema12"),


    F.sum(
        F.when(
            F.col("ema_26").isNull(),
            1
        ).otherwise(0)
    ).alias("null_ema26"),


    F.sum(
        F.when(
            F.col("rsi_14").isNull(),
            1
        ).otherwise(0)
    ).alias("null_rsi14"),


    F.sum(
        F.when(
            F.col("macd").isNull(),
            1
        ).otherwise(0)
    ).alias("null_macd"),


    F.sum(
        F.when(
            F.col("macd_signal").isNull(),
            1
        ).otherwise(0)
    ).alias("null_macd_signal"),


    F.sum(
        F.when(
            F.col("bollinger_upper").isNull(),
            1
        ).otherwise(0)
    ).alias("null_bollinger")


).show()


# =============================================================================
# 22. RSI RANGE CHECK
# =============================================================================

invalid_rsi = (
    result_df
    .filter(
        F.col("rsi_14").isNotNull()
        &
        (
            (F.col("rsi_14") < 0)
            |
            (F.col("rsi_14") > 100)
        )
    )
    .count()
)


print()
print(f"Nevalidnih RSI vrijednosti: {invalid_rsi}")


if invalid_rsi != 0:

    raise ValueError(
        f"Pronadjeno {invalid_rsi} nevalidnih RSI vrijednosti."
    )


# =============================================================================
# 23. DUPLICATE BUSINESS KEY CHECK
# =============================================================================

duplicate_count = (
    result_df
    .groupBy(
        "ticker",
        "date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print(
    f"Duplikata po (ticker, date): "
    f"{duplicate_count}"
)


if duplicate_count != 0:

    raise ValueError(
        f"Pronadjeno {duplicate_count} duplikata "
        f"po (ticker, date)."
    )


# =============================================================================
# 24. BOLLINGER CONSISTENCY CHECK
#
# Kada su Bands dostupni:
#
# lower <= middle <= upper
# =============================================================================

invalid_bollinger = (
    result_df
    .filter(
        F.col("bollinger_middle").isNotNull()
        &
        (
            (
                F.col("bollinger_lower")
                >
                F.col("bollinger_middle")
            )
            |
            (
                F.col("bollinger_middle")
                >
                F.col("bollinger_upper")
            )
        )
    )
    .count()
)


print(
    f"Nevalidnih Bollinger vrijednosti: "
    f"{invalid_bollinger}"
)


if invalid_bollinger != 0:

    raise ValueError(
        f"Pronadjeno {invalid_bollinger} "
        f"nevalidnih Bollinger vrijednosti."
    )


# =============================================================================
# 25. EXPECTED NULL COUNTS
#
# Za 7 tickera:
#
# SMA20:
# 19 pocetnih NULL * 7 = 133
#
# SMA50:
# 49 * 7 = 343
#
# EMA12:
# 11 * 7 = 77
#
# EMA26:
# 25 * 7 = 175
#
# RSI14:
# 14 * 7 = 98
#
# MACD:
# zavisi od EMA26 -> 25 * 7 = 175
#
# MACD Signal:
# nakon sto MACD postane dostupan treba jos 8 MACD observacija:
# 33 * 7 = 231
#
# Bollinger:
# 19 * 7 = 133
# =============================================================================

expected_ticker_count = 7


if ticker_count == expected_ticker_count:

    print()
    print("OCEKIVANI NULL BROJEVI ZA 7 TICKERA:")
    print("SMA20:       133")
    print("SMA50:       343")
    print("EMA12:        77")
    print("EMA26:       175")
    print("RSI14:        98")
    print("MACD:        175")
    print("MACD Signal: 231")
    print("Bollinger:   133")


# =============================================================================
# 26. FINAL
# =============================================================================

print()
print("=" * 90)
print("GOLD TECHNICAL INDICATORS COMPLETED")
print("=" * 90)