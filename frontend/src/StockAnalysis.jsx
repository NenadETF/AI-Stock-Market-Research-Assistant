import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  Activity,
  AlertTriangle,
  BarChart3,
  RefreshCw,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  getAnalyticsHistory,
  getAnomalies,
} from "../services/api";


/* =========================================================
   HELPERS
========================================================= */

function normalizeCollection(data) {
  if (Array.isArray(data)) {
    return data;
  }

  if (Array.isArray(data?.value)) {
    return data.value;
  }

  if (Array.isArray(data?.items)) {
    return data.items;
  }

  if (Array.isArray(data?.data)) {
    return data.data;
  }

  return [];
}


function getTicker(item) {
  if (typeof item === "string") {
    return item;
  }

  return item?.ticker || "";
}


function formatPrice(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  return `$${Number(value).toFixed(2)}`;
}


function formatNumber(
  value,
  decimals = 2
) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  return Number(value).toFixed(decimals);
}


function formatPercent(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  const number = Number(value);

  const sign =
    number > 0 ? "+" : "";

  return `${sign}${number.toFixed(2)}%`;
}


function formatVolume(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  const number =
    Number(value);

  if (number >= 1_000_000_000) {
    return `${(
      number /
      1_000_000_000
    ).toFixed(2)}B`;
  }

  if (number >= 1_000_000) {
    return `${(
      number /
      1_000_000
    ).toFixed(2)}M`;
  }

  if (number >= 1_000) {
    return `${(
      number /
      1_000
    ).toFixed(2)}K`;
  }

  return number.toFixed(0);
}


function formatDate(value) {
  if (!value) {
    return "--";
  }

  const normalized =
    value.includes("T")
      ? value
      : `${value}T00:00:00`;

  return new Date(
    normalized
  ).toLocaleDateString(
    "en-GB"
  );
}


function formatChartDate(value) {
  if (!value) {
    return "";
  }

  return new Date(
    value
  ).toLocaleDateString(
    "en-GB",
    {
      day: "2-digit",
      month: "short",
    }
  );
}


function getReturnClass(value) {
  const number =
    Number(value);

  if (number > 0) {
    return "market-positive";
  }

  if (number < 0) {
    return "market-negative";
  }

  return "market-neutral";
}


function getSeverityClass(severity) {
  switch (
    severity?.toUpperCase()
  ) {
    case "CRITICAL":
      return "severity-critical";

    case "HIGH":
      return "severity-high";

    case "MEDIUM":
      return "severity-medium";

    case "LOW":
      return "severity-low";

    default:
      return "severity-none";
  }
}


/* =========================================================
   COMPONENT
========================================================= */

function StockAnalysis({
  watchlist = [],
}) {

  const tickers =
    useMemo(
      () =>
        watchlist
          .map(getTicker)
          .filter(Boolean),
      [watchlist]
    );


  const [
    selectedTicker,
    setSelectedTicker,
  ] = useState("");


  const [
    history,
    setHistory,
  ] = useState([]);


  const [
    anomalyHistory,
    setAnomalyHistory,
  ] = useState([]);


  const [
    loading,
    setLoading,
  ] = useState(false);


  const [
    refreshing,
    setRefreshing,
  ] = useState(false);


  const [
    error,
    setError,
  ] = useState("");


  const [
    warning,
    setWarning,
  ] = useState("");


  /* =======================================================
     SELECT DEFAULT TICKER
  ======================================================= */

  useEffect(() => {

    if (
      tickers.length === 0
    ) {
      setSelectedTicker("");
      return;
    }


    if (
      !selectedTicker ||
      !tickers.includes(
        selectedTicker
      )
    ) {
      setSelectedTicker(
        tickers[0]
      );
    }

  }, [
    tickers,
    selectedTicker,
  ]);


  /* =======================================================
     LOAD ANALYSIS
  ======================================================= */

  const loadAnalysis =
    useCallback(
      async (
        ticker,
        isRefresh = false
      ) => {

        if (!ticker) {
          return;
        }


        try {

          if (isRefresh) {
            setRefreshing(true);
          } else {
            setLoading(true);
          }


          setError("");
          setWarning("");


          const [
            historyResult,
            anomalyResult,
          ] =
            await Promise.allSettled([
              getAnalyticsHistory(
                ticker
              ),

              getAnomalies(
                ticker
              ),
            ]);


          /* -----------------------------------------------
             HISTORY
          ------------------------------------------------ */

          if (
            historyResult.status ===
            "fulfilled"
          ) {

            const rows =
              normalizeCollection(
                historyResult.value
              );


            const sortedRows =
              [...rows].sort(
                (a, b) =>
                  new Date(a.date) -
                  new Date(b.date)
              );


            setHistory(
              sortedRows
            );

          } else {

            console.error(
              "Historical analytics failed:",
              historyResult.reason
            );

            setHistory([]);
          }


          /* -----------------------------------------------
             ANOMALIES
          ------------------------------------------------ */

          if (
            anomalyResult.status ===
            "fulfilled"
          ) {

            const rows =
              normalizeCollection(
                anomalyResult.value
              );


            const sortedRows =
              [...rows].sort(
                (a, b) =>
                  new Date(b.date) -
                  new Date(a.date)
              );


            setAnomalyHistory(
              sortedRows
            );

          } else {

            console.error(
              "Anomaly history failed:",
              anomalyResult.reason
            );

            setAnomalyHistory([]);
          }


          /* -----------------------------------------------
             REQUEST STATUS
          ------------------------------------------------ */

          if (
            historyResult.status ===
              "rejected" &&
            anomalyResult.status ===
              "rejected"
          ) {

            throw new Error(
              "Unable to load stock analysis data."
            );
          }


          if (
            historyResult.status ===
              "rejected" ||
            anomalyResult.status ===
              "rejected"
          ) {

            setWarning(
              "Some analytics data is currently unavailable."
            );
          }

        } catch (err) {

          console.error(
            "Stock analysis load failed:",
            err
          );


          setError(
            err.message ||
            "Unable to load stock analysis."
          );

        } finally {

          setLoading(false);

          setRefreshing(false);
        }

      },
      []
    );


  useEffect(() => {

    if (selectedTicker) {

      loadAnalysis(
        selectedTicker
      );
    }

  }, [
    selectedTicker,
    loadAnalysis,
  ]);


  /* =======================================================
     DERIVED DATA
  ======================================================= */

  const latest =
    history.length > 0
      ? history[
          history.length - 1
        ]
      : null;


  const chartData =
    history.map(
      (row) => ({
        ...row,

        chartDate:
          formatChartDate(
            row.date
          ),
      })
    );


  const anomalyCounts =
    anomalyHistory.reduce(
      (accumulator, item) => {

        const severity =
          item.anomaly_severity
            ?.toUpperCase();


        if (
          severity === "CRITICAL"
        ) {
          accumulator.critical += 1;
        }


        if (
          severity === "HIGH"
        ) {
          accumulator.high += 1;
        }


        if (
          severity === "MEDIUM"
        ) {
          accumulator.medium += 1;
        }


        if (
          severity === "LOW"
        ) {
          accumulator.low += 1;
        }


        return accumulator;

      },
      {
        critical: 0,
        high: 0,
        medium: 0,
        low: 0,
      }
    );


  /* =======================================================
     EMPTY WATCHLIST
  ======================================================= */

  if (
    tickers.length === 0
  ) {

    return (

      <section className="analysis-page">

        <div className="analysis-empty">

          <BarChart3 size={40} />

          <h2>
            Stock Analysis
          </h2>

          <p>
            Add a stock to your
            watchlist before opening
            historical analysis.
          </p>

        </div>

      </section>
    );
  }


  /* =======================================================
     UI
  ======================================================= */

  return (

    <section className="analysis-page">

      {/* ==================================================
          HEADER
      ================================================== */}

      <header className="analysis-header">

        <div>

          <div className="analysis-eyebrow">
            MARKET ANALYTICS
          </div>

          <h1>
            Stock Analysis
          </h1>

          <p>
            Historical Gold analytics,
            moving averages, volatility
            and anomaly intelligence.
          </p>

        </div>


        <button
          type="button"
          className="refresh-button"
          disabled={
            loading ||
            refreshing
          }
          onClick={() =>
            loadAnalysis(
              selectedTicker,
              true
            )
          }
        >

          <RefreshCw
            size={16}
            className={
              refreshing
                ? "refresh-spinning"
                : ""
            }
          />

          {refreshing
            ? "REFRESHING"
            : "REFRESH"}

        </button>

      </header>


      {/* ==================================================
          TICKER SELECTOR
      ================================================== */}

      <div className="analysis-ticker-tabs">

        {tickers.map(
          (ticker) => (

            <button
              key={ticker}
              type="button"
              className={
                selectedTicker ===
                ticker
                  ? "analysis-ticker-tab active"
                  : "analysis-ticker-tab"
              }
              onClick={() =>
                setSelectedTicker(
                  ticker
                )
              }
            >
              {ticker}
            </button>

          )
        )}

      </div>


      {/* ==================================================
          ERROR / WARNING
      ================================================== */}

      {error && (

        <div className="analysis-error">

          <AlertTriangle
            size={18}
          />

          <div>

            <strong>
              Unable to load analysis
            </strong>

            <span>
              {error}
            </span>

          </div>

        </div>
      )}


      {warning && !error && (

        <div className="analysis-warning">

          <AlertTriangle
            size={17}
          />

          {warning}

        </div>
      )}


      {/* ==================================================
          LOADING
      ================================================== */}

      {loading && (

        <div className="analysis-loading">

          <RefreshCw
            size={22}
            className="refresh-spinning"
          />

          Loading {selectedTicker}
          analytics...

        </div>
      )}


      {!loading &&
        !error &&
        latest && (

        <>

          {/* ==============================================
              STOCK SUMMARY
          ============================================== */}

          <section className="analysis-stock-summary">

            <div className="analysis-stock-title">

              <div className="analysis-symbol">

                {selectedTicker.charAt(
                  0
                )}

              </div>


              <div>

                <span>
                  {selectedTicker}
                </span>

                <h2>
                  {formatPrice(
                    latest.close
                  )}
                </h2>

                <small>
                  {formatDate(
                    latest.date
                  )}
                </small>

              </div>

            </div>


            <div
              className={`analysis-daily-change ${getReturnClass(
                latest.daily_return_pct
              )}`}
            >

              {Number(
                latest.daily_return_pct
              ) >= 0 ? (

                <TrendingUp
                  size={18}
                />

              ) : (

                <TrendingDown
                  size={18}
                />

              )}


              <div>

                <strong>
                  {formatPercent(
                    latest.daily_return_pct
                  )}
                </strong>

                <span>
                  Daily return
                </span>

              </div>

            </div>

          </section>


          {/* ==============================================
              KPI
          ============================================== */}

          <section className="analysis-kpi-grid">

            <article className="analysis-kpi">

              <span>
                MA7
              </span>

              <strong>
                {formatPrice(
                  latest.moving_avg_7d
                )}
              </strong>

              <small>
                7-day moving average
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                MA30
              </span>

              <strong>
                {formatPrice(
                  latest.moving_avg_30d
                )}
              </strong>

              <small>
                30-day moving average
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                VOLATILITY 30D
              </span>

              <strong>
                {formatPercent(
                  latest
                    .annualized_volatility_30d_pct
                )}
              </strong>

              <small>
                Annualized volatility
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                VOLUME RATIO
              </span>

              <strong>
                {formatNumber(
                  latest.volume_ratio
                )}
                x
              </strong>

              <small>
                Relative market volume
              </small>

            </article>

          </section>


          {/* ==============================================
              PRICE CHART
          ============================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  PRICE HISTORY
                </span>

                <h3>
                  Close Price &
                  Moving Averages
                </h3>

                <p>
                  Last {
                    history.length
                  } trading sessions
                </p>

              </div>


              <BarChart3
                size={22}
              />

            </div>


            <div className="analysis-chart">

              <ResponsiveContainer
                width="100%"
                height={360}
              >

                <LineChart
                  data={chartData}
                  margin={{
                    top: 10,
                    right: 12,
                    left: 0,
                    bottom: 0,
                  }}
                >

                  <CartesianGrid
                    stroke="#ececf0"
                    strokeDasharray="3 3"
                    vertical={false}
                  />


                  <XAxis
                    dataKey="chartDate"
                    tick={{
                      fill: "#86868b",
                      fontSize: 11,
                    }}
                    axisLine={false}
                    tickLine={false}
                    minTickGap={28}
                  />


                  <YAxis
                    tick={{
                      fill: "#86868b",
                      fontSize: 11,
                    }}
                    tickFormatter={
                      (value) =>
                        `$${Math.round(
                          value
                        )}`
                    }
                    axisLine={false}
                    tickLine={false}
                    width={58}
                    domain={[
                      "auto",
                      "auto",
                    ]}
                  />


                  <Tooltip
                    formatter={
                      (
                        value,
                        name
                      ) => [
                        formatPrice(
                          value
                        ),
                        name,
                      ]
                    }
                    contentStyle={{
                      background:
                        "rgba(255,255,255,0.97)",
                      border:
                        "1px solid rgba(0,0,0,0.08)",
                      borderRadius:
                        "12px",
                      boxShadow:
                        "0 12px 30px rgba(0,0,0,0.10)",
                    }}
                  />


                  <Legend />


                  <Line
                    type="monotone"
                    dataKey="close"
                    name="Close"
                    stroke="#0071e3"
                    strokeWidth={3}
                    dot={false}
                    activeDot={{
                      r: 4,
                    }}
                  />


                  <Line
                    type="monotone"
                    dataKey="moving_avg_7d"
                    name="MA7"
                    stroke="#34c759"
                    strokeWidth={2}
                    dot={false}
                  />


                  <Line
                    type="monotone"
                    dataKey="moving_avg_30d"
                    name="MA30"
                    stroke="#ff9f0a"
                    strokeWidth={2}
                    dot={false}
                  />

                </LineChart>

              </ResponsiveContainer>

            </div>

          </section>


          {/* ==============================================
              MARKET DETAIL
          ============================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  LATEST SESSION
                </span>

                <h3>
                  Market Metrics
                </h3>

              </div>


              <Activity
                size={22}
              />

            </div>


            <div className="analysis-metric-grid">

              <div>
                <span>OPEN</span>

                <strong>
                  {formatPrice(
                    latest.open
                  )}
                </strong>
              </div>


              <div>
                <span>HIGH</span>

                <strong>
                  {formatPrice(
                    latest.high
                  )}
                </strong>
              </div>


              <div>
                <span>LOW</span>

                <strong>
                  {formatPrice(
                    latest.low
                  )}
                </strong>
              </div>


              <div>
                <span>VWAP</span>

                <strong>
                  {formatPrice(
                    latest.vwap
                  )}
                </strong>
              </div>


              <div>
                <span>VOLUME</span>

                <strong>
                  {formatVolume(
                    latest.volume
                  )}
                </strong>
              </div>


              <div>
                <span>
                  TRANSACTIONS
                </span>

                <strong>
                  {formatVolume(
                    latest.transactions
                  )}
                </strong>
              </div>


              <div>
                <span>
                  DAILY RANGE
                </span>

                <strong>
                  {formatPercent(
                    latest
                      .high_low_range_pct
                  )}
                </strong>
              </div>


              <div>
                <span>
                  CLOSE VS VWAP
                </span>

                <strong
                  className={getReturnClass(
                    latest
                      .close_vs_vwap_pct
                  )}
                >
                  {formatPercent(
                    latest
                      .close_vs_vwap_pct
                  )}
                </strong>
              </div>

            </div>

          </section>


          {/* ==============================================
              ANOMALY OVERVIEW
          ============================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  ANOMALY ENGINE
                </span>

                <h3>
                  Historical Anomalies
                </h3>

                <p>
                  {
                    anomalyHistory.length
                  } returned anomaly
                  events
                </p>

              </div>


              <Activity
                size={22}
              />

            </div>


            <div className="analysis-anomaly-summary">

              <div>
                <span>
                  CRITICAL
                </span>

                <strong className="severity-high-text">
                  {
                    anomalyCounts.critical
                  }
                </strong>
              </div>


              <div>
                <span>
                  HIGH
                </span>

                <strong className="severity-high-text">
                  {
                    anomalyCounts.high
                  }
                </strong>
              </div>


              <div>
                <span>
                  MEDIUM
                </span>

                <strong className="severity-medium-text">
                  {
                    anomalyCounts.medium
                  }
                </strong>
              </div>


              <div>
                <span>
                  LOW
                </span>

                <strong className="severity-low-text">
                  {
                    anomalyCounts.low
                  }
                </strong>
              </div>

            </div>


            {anomalyHistory.length >
            0 ? (

              <div className="analysis-table-wrapper">

                <table className="analysis-table">

                  <thead>

                    <tr>
                      <th>DATE</th>
                      <th>SEVERITY</th>
                      <th>SCORE</th>
                      <th>RETURN</th>
                      <th>RSI 14</th>
                      <th>SIGNALS</th>
                      <th>CONDITION</th>
                    </tr>

                  </thead>


                  <tbody>

                    {anomalyHistory.map(
                      (
                        anomaly,
                        index
                      ) => (

                        <tr
                          key={`${anomaly.date}-${index}`}
                        >

                          <td>
                            {formatDate(
                              anomaly.date
                            )}
                          </td>


                          <td>

                            <span
                              className={`severity-badge ${getSeverityClass(
                                anomaly.anomaly_severity
                              )}`}
                            >
                              {
                                anomaly.anomaly_severity
                              }
                            </span>

                          </td>


                          <td>

                            <strong>
                              {formatNumber(
                                anomaly.anomaly_score
                              )}
                            </strong>

                          </td>


                          <td
                            className={getReturnClass(
                              anomaly.daily_return_pct
                            )}
                          >
                            {formatPercent(
                              anomaly.daily_return_pct
                            )}
                          </td>


                          <td>
                            {formatNumber(
                              anomaly.rsi_14
                            )}
                          </td>


                          <td className="analysis-signals-cell">
                            {
                              anomaly.anomaly_type
                            }
                          </td>


                          <td>
                            {
                              anomaly.market_condition
                            }
                          </td>

                        </tr>

                      )
                    )}

                  </tbody>

                </table>

              </div>

            ) : (

              <div className="analysis-no-anomalies">

                No anomaly events
                returned for
                {selectedTicker}.

              </div>

            )}

          </section>

        </>
      )}

    </section>
  );
}


export default StockAnalysis;