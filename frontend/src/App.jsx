import {
  useCallback,
  useEffect,
  useState,
} from "react";

import {
  Activity,
  BarChart3,
  Bot,
  ChevronRight,
  Cpu,
  Database,
  History,
  Newspaper,
  RefreshCw,
  Server,
  ShieldCheck,
  Star,
  Terminal,
  Trash2,
  Wifi,
  X,
} from "lucide-react";

import {
  addToWatchlist,
  getAnomalySummary,
  getLatestAnalytics,
  getWatchlist,
  getDataFreshness,
  refreshMarketData,
  removeFromWatchlist,
} from "./services/api";

import StockAnalysis from "./components/StockAnalysis";
import NewsIntelligence from "./components/NewsIntelligence";
import AIResearch from "./components/AIResearch";
import ResearchHistory from "./components/ResearchHistory";

import "./App.css";


function normalizeWatchlist(data) {
  if (Array.isArray(data)) {
    return data;
  }

  if (Array.isArray(data?.items)) {
    return data.items;
  }

  if (Array.isArray(data?.watchlist)) {
    return data.watchlist;
  }

  return [];
}


function getTicker(item) {
  if (typeof item === "string") {
    return item;
  }

  return item?.ticker || "N/A";
}


function formatTime(date) {
  if (!date) {
    return "--:--:--";
  }

  return new Intl.DateTimeFormat(
    "en-GB",
    {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }
  ).format(date);
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
    number > 0
      ? "+"
      : "";

  return `${sign}${number.toFixed(2)}%`;
}


function getReturnClass(value) {
  const number = Number(value);

  if (number > 0) {
    return "market-positive";
  }

  if (number < 0) {
    return "market-negative";
  }

  return "market-neutral";
}


function formatDate(value) {
  if (!value) {
    return "--";
  }

  const normalizedValue =
    value.includes("T")
      ? value
      : `${value}T00:00:00`;

  return new Date(
    normalizedValue
  ).toLocaleDateString(
    "en-GB"
  );
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


function App() {
  const [
    activePage,
    setActivePage,
  ] = useState("dashboard");


  const [
    watchlist,
    setWatchlist,
  ] = useState([]);


  const [
    analytics,
    setAnalytics,
  ] = useState({});


  const [
    anomalies,
    setAnomalies,
  ] = useState({});


  const [
    loading,
    setLoading,
  ] = useState(true);


  const [
    refreshing,
    setRefreshing,
  ] = useState(false);


  const [
    error,
    setError,
  ] = useState("");


  const [
    backendOnline,
    setBackendOnline,
  ] = useState(false);


  const [
    lastSync,
    setLastSync,
  ] = useState(null);


  const [
    dataFreshness,
    setDataFreshness,
  ] = useState(null);


  const [
    freshnessError,
    setFreshnessError,
  ] = useState("");


  const [
    marketRefreshing,
    setMarketRefreshing,
  ] = useState(false);


  const [
    marketRefreshMessage,
    setMarketRefreshMessage,
  ] = useState("");


  const [
    marketRefreshFailed,
    setMarketRefreshFailed,
  ] = useState(false);


  const [
    showAddModal,
    setShowAddModal,
  ] = useState(false);


  const [
    newStock,
    setNewStock,
  ] = useState({
    ticker: "",
    investment_thesis: "",
    notes: "",
  });


  const [
    savingStock,
    setSavingStock,
  ] = useState(false);


  const [
    deletingTicker,
    setDeletingTicker,
  ] = useState("");


  const [
    watchlistActionError,
    setWatchlistActionError,
  ] = useState("");


  const loadDashboard =
    useCallback(
      async (
        isRefresh = false
      ) => {
        try {
          if (isRefresh) {
            setRefreshing(true);
          } else {
            setLoading(true);
          }

          setError("");

          const watchlistData =
            await getWatchlist();

          const normalizedWatchlist =
            normalizeWatchlist(
              watchlistData
            );

          setWatchlist(
            normalizedWatchlist
          );

          const tickers =
            normalizedWatchlist
              .map(getTicker)
              .filter(
                (ticker) =>
                  ticker &&
                  ticker !== "N/A"
              );

          const results =
            await Promise.allSettled(
              tickers.map(
                async (ticker) => {
                  const [
                    analyticsResult,
                    anomalyResult,
                  ] =
                    await Promise.allSettled([
                      getLatestAnalytics(
                        ticker
                      ),
                      getAnomalySummary(
                        ticker
                      ),
                    ]);

                  if (
                    analyticsResult.status ===
                    "rejected"
                  ) {
                    console.error(
                      `[${ticker}] Latest analytics failed:`,
                      analyticsResult.reason
                    );
                  }

                  if (
                    anomalyResult.status ===
                    "rejected"
                  ) {
                    console.error(
                      `[${ticker}] Anomaly summary failed:`,
                      anomalyResult.reason
                    );
                  }

                  return {
                    ticker,

                    analytics:
                      analyticsResult.status ===
                      "fulfilled"
                        ? analyticsResult.value
                        : null,

                    anomaly:
                      anomalyResult.status ===
                      "fulfilled"
                        ? anomalyResult.value
                        : null,
                  };
                }
              )
            );

          const analyticsMap = {};
          const anomaliesMap = {};

          results.forEach(
            (result) => {
              if (
                result.status !==
                "fulfilled"
              ) {
                console.error(
                  "Ticker data request failed:",
                  result.reason
                );

                return;
              }

              const {
                ticker,
                analytics: tickerAnalytics,
                anomaly,
              } = result.value;

              if (tickerAnalytics) {
                analyticsMap[ticker] =
                  tickerAnalytics;
              }

              if (anomaly) {
                anomaliesMap[ticker] =
                  anomaly;
              }
            }
          );

          setAnalytics(
            analyticsMap
          );

          setAnomalies(
            anomaliesMap
          );

          try {
            const freshnessData =
              await getDataFreshness();

            setDataFreshness(
              freshnessData
            );

            setFreshnessError("");

          } catch (freshnessErr) {
            console.error(
              "Data freshness request failed:",
              freshnessErr
            );

            setDataFreshness(null);

            setFreshnessError(
              freshnessErr.message ||
              "Data freshness unavailable"
            );
          }

          setBackendOnline(true);

          setLastSync(
            new Date()
          );

        } catch (err) {
          console.error(
            "Dashboard load failed:",
            err
          );

          setBackendOnline(false);

          setDataFreshness(null);

          setError(
            err.message ||
            "Unknown API error"
          );

        } finally {
          setLoading(false);

          setRefreshing(false);
        }
      },
      []
    );


  useEffect(() => {
    loadDashboard();
  }, [loadDashboard]);


  useEffect(() => {
    if (!showAddModal) {
      return undefined;
    }

    function handleKeyDown(event) {
      if (
        event.key === "Escape" &&
        !savingStock
      ) {
        setShowAddModal(false);

        setWatchlistActionError("");
      }
    }

    window.addEventListener(
      "keydown",
      handleKeyDown
    );

    return () => {
      window.removeEventListener(
        "keydown",
        handleKeyDown
      );
    };

  }, [
    showAddModal,
    savingStock,
  ]);


  function handleStockInputChange(
    event
  ) {
    const {
      name,
      value,
    } = event.target;

    setNewStock(
      (current) => ({
        ...current,

        [name]:
          name === "ticker"
            ? value.toUpperCase()
            : value,
      })
    );
  }


  function handleOpenAddModal() {
    setWatchlistActionError("");

    setNewStock({
      ticker: "",
      investment_thesis: "",
      notes: "",
    });

    setShowAddModal(true);
  }


  function handleCloseAddModal() {
    if (savingStock) {
      return;
    }

    setShowAddModal(false);

    setWatchlistActionError("");
  }


  async function handleAddStock(
    event
  ) {
    event.preventDefault();

    const ticker =
      newStock.ticker
        .trim()
        .toUpperCase();

    if (!ticker) {
      setWatchlistActionError(
        "Ticker is required."
      );

      return;
    }

    if (ticker.length > 10) {
      setWatchlistActionError(
        "Ticker cannot exceed 10 characters."
      );

      return;
    }

    const alreadyTracked =
      watchlist.some(
        (item) =>
          getTicker(item)
            .toUpperCase() ===
          ticker
      );

    if (alreadyTracked) {
      setWatchlistActionError(
        `${ticker} is already on the watchlist.`
      );

      return;
    }

    try {
      setSavingStock(true);

      setWatchlistActionError("");

      await addToWatchlist({
        ticker,

        investment_thesis:
          newStock
            .investment_thesis
            .trim(),

        notes:
          newStock
            .notes
            .trim(),
      });

      setNewStock({
        ticker: "",
        investment_thesis: "",
        notes: "",
      });

      setShowAddModal(false);

      await loadDashboard(true);

    } catch (err) {
      console.error(
        "Failed to add watchlist item:",
        err
      );

      setWatchlistActionError(
        err.message ||
        "Unable to add stock."
      );

    } finally {
      setSavingStock(false);
    }
  }


  async function handleRemoveStock(
    ticker
  ) {
    const confirmed =
      window.confirm(
        `Remove ${ticker} from your watchlist?`
      );

    if (!confirmed) {
      return;
    }

    try {
      setDeletingTicker(
        ticker
      );

      setWatchlistActionError("");

      await removeFromWatchlist(
        ticker
      );

      setAnalytics(
        (current) => {
          const updated = {
            ...current,
          };

          delete updated[ticker];

          return updated;
        }
      );

      setAnomalies(
        (current) => {
          const updated = {
            ...current,
          };

          delete updated[ticker];

          return updated;
        }
      );

      await loadDashboard(true);

    } catch (err) {
      console.error(
        `Failed to remove ${ticker}:`,
        err
      );

      setWatchlistActionError(
        err.message ||
        `Unable to remove ${ticker}.`
      );

    } finally {
      setDeletingTicker("");
    }
  }


  async function handleRefreshMarketData() {
    if (marketRefreshing) {
      return;
    }

    const confirmed =
      window.confirm(
        "Refresh all market data now? " +
        "The full pipeline can take several minutes."
      );

    if (!confirmed) {
      return;
    }

    try {
      setMarketRefreshing(true);

      setMarketRefreshFailed(false);

      setMarketRefreshMessage(
        "MARKET DATA REFRESH RUNNING — " +
        "Massive API → Bronze → Silver → Gold → Freshness"
      );

      const result =
        await refreshMarketData();

      setMarketRefreshMessage(
        `MARKET DATA REFRESH COMPLETED${
          result?.duration_seconds
            ? ` IN ${result.duration_seconds}s`
            : ""
        }`
      );

      await loadDashboard(true);

    } catch (err) {
      console.error(
        "Full market data refresh failed:",
        err
      );

      setMarketRefreshFailed(true);

      setMarketRefreshMessage(
        err.message ||
        "Market data refresh failed."
      );

    } finally {
      setMarketRefreshing(false);
    }
  }


  return (
    <div className="app">

      <aside className="sidebar">

        <div className="brand">

          <div className="brand-icon">
            <Activity size={29} />
          </div>

          <div>
            <strong>
              Stock Research
            </strong>

            <span>
              AI Intelligence Node
            </span>
          </div>

        </div>


        <div className="sidebar-terminal">

          <Terminal size={14} />

          <span>
            system://market-node
          </span>

          <span className="terminal-cursor">
            _
          </span>

        </div>


        <nav>

          <button
            type="button"
            className={`nav-item ${
              activePage === "dashboard"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActivePage(
                "dashboard"
              )
            }
          >
            <BarChart3 size={19} />
            Dashboard
          </button>


          <button
            type="button"
            className="nav-item"
            onClick={() =>
              setActivePage(
                "dashboard"
              )
            }
            title="Watchlist is currently available on the Dashboard"
          >
            <Star size={19} />
            Watchlist
          </button>


          <button
            type="button"
            className={`nav-item ${
              activePage ===
              "stock-analysis"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActivePage(
                "stock-analysis"
              )
            }
          >
            <Activity size={19} />
            Stock Analysis
          </button>


          <button
            type="button"
            className={`nav-item ${
              activePage ===
              "news-intelligence"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActivePage(
                "news-intelligence"
              )
            }
          >
            <Newspaper size={19} />
            News Intelligence
          </button>


          <button
            type="button"
            className={`nav-item ${
              activePage ===
              "ai-research"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActivePage(
                "ai-research"
              )
            }
          >
            <Bot size={19} />
            AI Research
          </button>


          <button
            type="button"
            className={`nav-item ${
              activePage ===
              "research-history"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActivePage(
                "research-history"
              )
            }
          >
            <History size={19} />
            Research History
          </button>

        </nav>


        <div className="sidebar-bottom">

          <div className="sidebar-status-row">

            <Server size={14} />

            <span>
              API NODE
            </span>

            <strong
              className={
                backendOnline
                  ? "mini-online"
                  : "mini-offline"
              }
            >
              {backendOnline
                ? "ONLINE"
                : "OFFLINE"}
            </strong>

          </div>


          <div className="sidebar-status-row">

            <Database size={14} />

            <span>
              DATA LAYER
            </span>

            <strong
              className={
                dataFreshness?.overall_status ===
                "FRESH"
                  ? "mini-online"
                  : "mini-offline"
              }
            >
              {dataFreshness?.overall_status ||
                (backendOnline
                  ? "UNKNOWN"
                  : "OFFLINE")}
            </strong>

          </div>


          <div className="sidebar-status-row">

            <ShieldCheck size={14} />

            <span>
              SESSION
            </span>

            <strong className="mini-online">
              SECURE
            </strong>

          </div>

        </div>

      </aside>


      <main className="main-content">

        {activePage ===
        "stock-analysis" ? (

          <StockAnalysis
            watchlist={watchlist}
          />

        ) : activePage ===
        "news-intelligence" ? (

          <NewsIntelligence
            watchlist={watchlist}
          />

        ) : activePage ===
        "ai-research" ? (

          <AIResearch
            watchlist={watchlist}
          />

        ) : activePage ===
        "research-history" ? (

          <ResearchHistory />

        ) : (

          <>

            <header className="topbar">

              <div className="page-heading">

                <div className="terminal-label">

                  <span>
                    &gt;
                  </span>

                  MARKET_INTELLIGENCE / OVERVIEW

                </div>


                <h1>
                  Market Overview
                </h1>


                <p>
                  AI Stock Market Research Assistant
                  {" // "}
                  Secure Analytics Node
                </p>

              </div>


              <div className="topbar-actions">

                <button
                  type="button"
                  className="primary-button"
                  onClick={
                    handleRefreshMarketData
                  }
                  disabled={
                    marketRefreshing ||
                    refreshing
                  }
                  title="Run the full market data refresh pipeline"
                >

                  <Database size={17} />

                  {marketRefreshing
                    ? "REFRESHING MARKET DATA..."
                    : "REFRESH MARKET DATA"}

                </button>


                <button
                  type="button"
                  className="refresh-button"
                  onClick={() =>
                    loadDashboard(true)
                  }
                  disabled={
                    refreshing ||
                    marketRefreshing
                  }
                  title="Refresh dashboard view only"
                >

                  <RefreshCw
                    size={17}
                    className={
                      refreshing
                        ? "refresh-spinning"
                        : ""
                    }
                  />


                  {refreshing
                    ? "SYNCING"
                    : "REFRESH"}

                </button>


                <div
                  className={
                    backendOnline
                      ? "status online"
                      : "status offline"
                  }
                >

                  <span className="status-dot" />

                  <Wifi size={15} />

                  {backendOnline
                    ? "API CONNECTED"
                    : "API OFFLINE"}

                </div>

              </div>

            </header>


            {marketRefreshMessage && (

              <div className="dashboard-terminal">

                <Database size={15} />

                <span
                  className={
                    marketRefreshFailed
                      ? "market-negative"
                      : "terminal-success"
                  }
                >
                  {marketRefreshMessage}
                </span>

              </div>
            )}


            <section className="system-bar">

              <div className="system-bar-item">

                <span>
                  NODE
                </span>

                <strong>
                  MARKET-01
                </strong>

              </div>


              <div className="system-bar-item">

                <span>
                  USER
                </span>

                <strong>
                  demo_user
                </strong>

              </div>


              <div className="system-bar-item">

                <span>
                  LAST SYNC
                </span>

                <strong>
                  {formatTime(
                    lastSync
                  )}
                </strong>

              </div>


              <div className="system-bar-item">

                <span>
                  PIPELINE
                </span>

                <strong
                  className={
                    dataFreshness?.overall_status ===
                    "FRESH"
                      ? "system-green"
                      : "market-negative"
                  }
                >
                  {dataFreshness?.overall_status ||
                    (backendOnline
                      ? "UNKNOWN"
                      : "OFFLINE")}
                </strong>

              </div>


              <div className="system-bar-item">

                <span>
                  AGENT
                </span>

                <strong className="system-green">
                  READY
                </strong>

              </div>

            </section>


            <section className="stats-grid">

              <article className="stat-card">

                <div className="stat-card-top">

                  <span>
                    WATCHLIST STOCKS
                  </span>

                  <Star size={17} />

                </div>


                <strong>
                  {loading
                    ? "..."
                    : watchlist.length}
                </strong>


                <small>
                  Active tracking targets
                </small>


                <div className="stat-line" />

              </article>


              <article className="stat-card">

                <div className="stat-card-top">

                  <span>
                    MARKET DATA
                  </span>

                  <Database size={17} />

                </div>


                <strong
                  className={
                    dataFreshness?.market_data?.status ===
                    "FRESH"
                      ? "positive-value"
                      : dataFreshness
                      ? "market-negative"
                      : ""
                  }
                >

                  {dataFreshness
                    ?.market_data
                    ?.status || "N/A"}

                </strong>


                <small>
                  {dataFreshness
                    ?.market_data
                    ?.latest_date
                    ? `Latest ${formatDate(
                        dataFreshness
                          .market_data
                          .latest_date
                      )} / Expected ${formatDate(
                        dataFreshness
                          .market_data
                          .expected_date
                      )}`
                    : freshnessError
                    ? "Freshness status unavailable"
                    : "Databricks Gold analytics"}
                </small>


                <div className="stat-line" />

              </article>


              <article className="stat-card">

                <div className="stat-card-top">

                  <span>
                    AI RESEARCH
                  </span>

                  <Cpu size={17} />

                </div>


                <strong className="positive-value">
                  READY
                </strong>


                <small>
                  Groq research intelligence
                </small>


                <div className="stat-line" />

              </article>


              <article className="stat-card">

                <div className="stat-card-top">

                  <span>
                    ANOMALY ENGINE
                  </span>

                  <Activity size={17} />

                </div>


                <strong className="positive-value">
                  ACTIVE
                </strong>


                <small>
                  Gold detection layer
                </small>


                <div className="stat-line" />

              </article>

            </section>


            <section className="panel">

              <div className="panel-header">

                <div>

                  <div className="section-terminal">

                    <span>
                      &gt;
                    </span>

                    WATCHLIST_NODE

                  </div>


                  <h2>
                    My Watchlist
                  </h2>


                  <p>
                    Tracked market assets and
                    analytics targets
                  </p>

                </div>


                <button
                  type="button"
                  className="primary-button"
                  onClick={
                    handleOpenAddModal
                  }
                >

                  + INJECT TICKER

                </button>

              </div>


              {watchlistActionError &&
                !showAddModal && (

                  <div className="watchlist-action-error">

                    <strong>
                      WATCHLIST ERROR
                    </strong>

                    <span>
                      {watchlistActionError}
                    </span>

                  </div>
                )}


              {loading && (

                <div className="loading-state">

                  <div className="loading-terminal">

                    <Terminal size={17} />

                    <span>
                      Loading watchlist...
                    </span>

                  </div>

                </div>
              )}


              {error && (

                <div className="error-message">

                  <strong>
                    CONNECTION ERROR
                  </strong>

                  <span>
                    {error}
                  </span>


                  <button
                    type="button"
                    className="retry-button"
                    onClick={() =>
                      loadDashboard()
                    }
                  >

                    RETRY CONNECTION

                  </button>

                </div>
              )}


              {!loading &&
                !error &&
                watchlist.length === 0 && (

                  <div className="empty-state">

                    <Star size={32} />

                    <strong>
                      No tracked stocks
                    </strong>

                    <span>
                      Add a ticker to begin monitoring.
                    </span>

                  </div>
                )}


              {!loading &&
                !error &&
                watchlist.length > 0 && (

                  <div className="watchlist-grid">

                    {watchlist.map(
                      (
                        item,
                        index
                      ) => {

                        const ticker =
                          getTicker(item);

                        const metrics =
                          analytics[ticker];

                        const anomaly =
                          anomalies[ticker];

                        return (

                          <article
                            className="stock-card market-stock-card"
                            key={`${ticker}-${index}`}
                          >

                            <div className="stock-card-header">

                              <div className="stock-identity">

                                <div className="ticker-icon">
                                  {ticker.charAt(0)}
                                </div>


                                <div className="stock-info">

                                  <div className="stock-title-row">

                                    <strong>
                                      {ticker}
                                    </strong>

                                    <span className="tracked-badge">
                                      TRACKED
                                    </span>

                                  </div>


                                  <span className="analytics-date">

                                    {metrics?.date
                                      ? new Date(
                                          metrics.date
                                        ).toLocaleDateString(
                                          "en-GB"
                                        )
                                      : "Analytics unavailable"}

                                  </span>

                                </div>

                              </div>


                              <div className="stock-actions">

                                <button
                                  type="button"
                                  className="remove-stock-button"
                                  onClick={() =>
                                    handleRemoveStock(
                                      ticker
                                    )
                                  }
                                  disabled={
                                    deletingTicker ===
                                    ticker
                                  }
                                  title={`Remove ${ticker}`}
                                >

                                  <Trash2 size={14} />

                                  {deletingTicker ===
                                  ticker
                                    ? "REMOVING"
                                    : "REMOVE"}

                                </button>


                                <ChevronRight
                                  className="stock-arrow"
                                  size={20}
                                />

                              </div>

                            </div>


                            {metrics ? (

                              <>

                                <div className="market-price-section">

                                  <div className="latest-price">

                                    {formatPrice(
                                      metrics.close
                                    )}

                                  </div>


                                  <div
                                    className={`daily-return ${getReturnClass(
                                      metrics.daily_return_pct
                                    )}`}
                                  >

                                    {metrics.daily_return_pct >
                                    0
                                      ? "▲"
                                      : metrics.daily_return_pct <
                                        0
                                      ? "▼"
                                      : "•"}

                                    {" "}

                                    {formatPercent(
                                      metrics.daily_return_pct
                                    )}

                                  </div>

                                </div>


                                <div className="market-metrics-grid">

                                  <div className="market-metric">

                                    <span>
                                      MA7
                                    </span>

                                    <strong>
                                      {formatNumber(
                                        metrics.moving_avg_7d
                                      )}
                                    </strong>

                                  </div>


                                  <div className="market-metric">

                                    <span>
                                      MA30
                                    </span>

                                    <strong>
                                      {formatNumber(
                                        metrics.moving_avg_30d
                                      )}
                                    </strong>

                                  </div>


                                  <div className="market-metric">

                                    <span>
                                      VOL 30D
                                    </span>

                                    <strong>
                                      {formatPercent(
                                        metrics
                                          .annualized_volatility_30d_pct
                                      )}
                                    </strong>

                                  </div>


                                  <div className="market-metric">

                                    <span>
                                      VOLUME
                                    </span>

                                    <strong>
                                      {formatNumber(
                                        metrics.volume_ratio
                                      )}
                                      x
                                    </strong>

                                  </div>

                                </div>


                                <div className="market-secondary">

                                  <div>

                                    <span>
                                      VWAP
                                    </span>

                                    <strong>
                                      {formatPrice(
                                        metrics.vwap
                                      )}
                                    </strong>

                                  </div>


                                  <div>

                                    <span>
                                      RANGE
                                    </span>

                                    <strong>
                                      {formatPercent(
                                        metrics.high_low_range_pct
                                      )}
                                    </strong>

                                  </div>

                                </div>


                                <div className="market-direction">

                                  <div>

                                    <span className="stock-meta-dot" />

                                    MARKET DIRECTION

                                  </div>


                                  <strong
                                    className={getReturnClass(
                                      metrics.daily_return_pct
                                    )}
                                  >

                                    {metrics.daily_direction}

                                  </strong>

                                </div>


                                <div className="anomaly-section">

                                  <div className="anomaly-heading">

                                    <div>

                                      <span className="anomaly-label">
                                        ANOMALY INTELLIGENCE
                                      </span>

                                      <strong>
                                        Historical detection summary
                                      </strong>

                                    </div>


                                    {anomaly ? (

                                      <span
                                        className={`severity-badge ${getSeverityClass(
                                          anomaly.latest_anomaly_severity
                                        )}`}
                                      >

                                        {anomaly
                                          .latest_anomaly_severity ||
                                          "NONE"}

                                      </span>

                                    ) : (

                                      <span className="severity-badge severity-none">
                                        N/A
                                      </span>

                                    )}

                                  </div>


                                  {anomaly ? (

                                    <>

                                      <div className="anomaly-stats">

                                        <div>

                                          <span>
                                            TOTAL
                                          </span>

                                          <strong>
                                            {
                                              anomaly
                                                .total_anomalies
                                            }
                                          </strong>

                                        </div>


                                        <div>

                                          <span>
                                            HIGH
                                          </span>

                                          <strong className="severity-high-text">
                                            {
                                              anomaly
                                                .high_count
                                            }
                                          </strong>

                                        </div>


                                        <div>

                                          <span>
                                            MED
                                          </span>

                                          <strong className="severity-medium-text">
                                            {
                                              anomaly
                                                .medium_count
                                            }
                                          </strong>

                                        </div>


                                        <div>

                                          <span>
                                            LOW
                                          </span>

                                          <strong className="severity-low-text">
                                            {
                                              anomaly
                                                .low_count
                                            }
                                          </strong>

                                        </div>

                                      </div>


                                      <div className="latest-anomaly">

                                        <div>

                                          <span>
                                            LATEST EVENT
                                          </span>

                                          <strong>
                                            {formatDate(
                                              anomaly
                                                .latest_anomaly_date
                                            )}
                                          </strong>

                                        </div>


                                        <div>

                                          <span>
                                            SCORE
                                          </span>

                                          <strong>
                                            {formatNumber(
                                              anomaly
                                                .latest_anomaly_score
                                            )}
                                          </strong>

                                        </div>

                                      </div>


                                      <div className="anomaly-condition">

                                        <span>
                                          MARKET CONDITION
                                        </span>

                                        <strong>
                                          {
                                            anomaly
                                              .latest_market_condition ||
                                            "--"
                                          }
                                        </strong>

                                      </div>


                                      <div className="anomaly-type">

                                        <span>
                                          SIGNALS
                                        </span>

                                        <strong>
                                          {
                                            anomaly
                                              .latest_anomaly_type ||
                                            "--"
                                          }
                                        </strong>

                                      </div>

                                    </>

                                  ) : (

                                    <div className="anomaly-unavailable">
                                      Anomaly summary unavailable
                                    </div>

                                  )}

                                </div>

                              </>

                            ) : (

                              <div className="analytics-unavailable">

                                <Terminal size={14} />

                                Gold analytics unavailable

                              </div>

                            )}

                          </article>
                        );
                      }
                    )}

                  </div>
                )}

            </section>


            <div className="dashboard-terminal">

              <Terminal size={15} />

              <span className="terminal-command">
                Stock Research
              </span>

              <span>
                ·
              </span>

              <span
                className={
                  dataFreshness?.overall_status ===
                  "FRESH"
                    ? "terminal-success"
                    : "market-negative"
                }
              >

                {dataFreshness?.summary
                  ? `${
                      dataFreshness.summary
                        .fresh_checks
                    }/${
                      dataFreshness.summary
                        .total_checks
                    } freshness checks passed`
                  : freshnessError
                  ? "Freshness service unavailable"
                  : "Core services operational"}

              </span>

            </div>

          </>
        )}

      </main>


      {showAddModal && (

        <div
          className="modal-overlay"
          onMouseDown={
            (event) => {

              if (
                event.target ===
                  event.currentTarget &&
                !savingStock
              ) {
                handleCloseAddModal();
              }
            }
          }
        >

          <div className="inject-modal">

            <div className="modal-header">

              <div>

                <div className="section-terminal">
                  WATCHLIST / NEW STOCK
                </div>

                <h2>
                  Add Market Target
                </h2>

                <p>
                  Add a ticker to your active
                  research watchlist.
                </p>

              </div>


              <button
                type="button"
                className="modal-close"
                onClick={
                  handleCloseAddModal
                }
                disabled={savingStock}
                aria-label="Close modal"
              >

                <X size={20} />

              </button>

            </div>


            <form
              className="inject-form"
              onSubmit={
                handleAddStock
              }
            >

              <label>

                <span>
                  TICKER *
                </span>

                <input
                  type="text"
                  name="ticker"
                  value={
                    newStock.ticker
                  }
                  onChange={
                    handleStockInputChange
                  }
                  placeholder="e.g. TSLA"
                  maxLength={10}
                  autoFocus
                  required
                  disabled={savingStock}
                />

              </label>


              <label>

                <span>
                  INVESTMENT THESIS
                </span>

                <textarea
                  name="investment_thesis"
                  value={
                    newStock
                      .investment_thesis
                  }
                  onChange={
                    handleStockInputChange
                  }
                  maxLength={2000}
                  rows={5}
                  placeholder="Why are you tracking this stock?"
                  disabled={savingStock}
                />

                <small>
                  {
                    newStock
                      .investment_thesis
                      .length
                  }
                  /2000
                </small>

              </label>


              <label>

                <span>
                  NOTES
                </span>

                <textarea
                  name="notes"
                  value={
                    newStock.notes
                  }
                  onChange={
                    handleStockInputChange
                  }
                  maxLength={2000}
                  rows={3}
                  placeholder="Optional research notes..."
                  disabled={savingStock}
                />

                <small>
                  {
                    newStock
                      .notes
                      .length
                  }
                  /2000
                </small>

              </label>


              {watchlistActionError && (

                <div className="modal-error">

                  <strong>
                    Unable to add ticker
                  </strong>

                  <span>
                    {
                      watchlistActionError
                    }
                  </span>

                </div>
              )}


              <div className="modal-actions">

                <button
                  type="button"
                  className="secondary-button"
                  onClick={
                    handleCloseAddModal
                  }
                  disabled={savingStock}
                >

                  CANCEL

                </button>


                <button
                  type="submit"
                  className="primary-button"
                  disabled={
                    savingStock ||
                    !newStock
                      .ticker
                      .trim()
                  }
                >

                  {savingStock
                    ? "ADDING..."
                    : "ADD TICKER"}

                </button>

              </div>

            </form>

          </div>

        </div>
      )}

    </div>
  );
}


export default App;