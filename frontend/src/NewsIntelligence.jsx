import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  Activity,
  ExternalLink,
  Globe2,
  Newspaper,
  RefreshCw,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  getNewsHistory,
  getNewsSummary,
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

  return [];
}


function getTicker(item) {
  if (typeof item === "string") {
    return item;
  }

  return item?.ticker || "";
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

  return Number(value)
    .toFixed(decimals);
}


function formatPercent(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  return `${Number(value).toFixed(2)}%`;
}


function formatScore(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "--";
  }

  const number =
    Number(value);

  const sign =
    number > 0
      ? "+"
      : "";

  return `${sign}${number.toFixed(2)}`;
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


function formatDateTime(value) {
  if (!value) {
    return "--";
  }

  return new Date(
    value
  ).toLocaleString(
    "en-GB",
    {
      dateStyle: "medium",
      timeStyle: "short",
    }
  );
}


function formatChartDate(value) {
  if (!value) {
    return "";
  }

  return new Date(
    `${value}T00:00:00`
  ).toLocaleDateString(
    "en-GB",
    {
      day: "2-digit",
      month: "short",
    }
  );
}


function getSentimentClass(
  sentiment
) {
  switch (
    sentiment?.toUpperCase()
  ) {
    case "POSITIVE":
      return "news-positive";

    case "NEGATIVE":
      return "news-negative";

    case "MIXED":
      return "news-mixed";

    default:
      return "news-neutral";
  }
}


function getScoreClass(value) {
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


/* =========================================================
   COMPONENT
========================================================= */

function NewsIntelligence({
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
    summary,
    setSummary,
  ] = useState(null);


  const [
    history,
    setHistory,
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


  /* =======================================================
     DEFAULT TICKER
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
     LOAD NEWS
  ======================================================= */

  const loadNews =
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


          const [
            summaryResult,
            historyResult,
          ] =
            await Promise.allSettled([
              getNewsSummary(
                ticker
              ),

              getNewsHistory(
                ticker
              ),
            ]);


          if (
            summaryResult.status ===
            "fulfilled"
          ) {
            setSummary(
              summaryResult.value
            );
          } else {
            setSummary(null);

            console.error(
              "News summary failed:",
              summaryResult.reason
            );
          }


          if (
            historyResult.status ===
            "fulfilled"
          ) {

            const rows =
              normalizeCollection(
                historyResult.value
              );


            setHistory(
              [...rows].sort(
                (a, b) =>
                  new Date(
                    a.published_date
                  ) -
                  new Date(
                    b.published_date
                  )
              )
            );

          } else {

            setHistory([]);

            console.error(
              "News history failed:",
              historyResult.reason
            );
          }


          if (
            summaryResult.status ===
              "rejected" &&
            historyResult.status ===
              "rejected"
          ) {
            throw new Error(
              "Unable to load news intelligence."
            );
          }

        } catch (err) {

          console.error(
            "News Intelligence load failed:",
            err
          );


          setError(
            err.message ||
            "Unable to load news intelligence."
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
      loadNews(
        selectedTicker
      );
    }

  }, [
    selectedTicker,
    loadNews,
  ]);


  /* =======================================================
     CHART DATA
  ======================================================= */

  const chartData =
    history.map(
      (row) => ({
        ...row,

        chartDate:
          formatChartDate(
            row.published_date
          ),
      })
    );


  /* =======================================================
     EMPTY WATCHLIST
  ======================================================= */

  if (
    tickers.length === 0
  ) {

    return (

      <section className="news-page">

        <div className="analysis-empty">

          <Newspaper size={40} />

          <h2>
            News Intelligence
          </h2>

          <p>
            Add a stock to your
            watchlist before opening
            news intelligence.
          </p>

        </div>

      </section>
    );
  }


  /* =======================================================
     UI
  ======================================================= */

  return (

    <section className="news-page">


      {/* ==================================================
          PAGE HEADER
      ================================================== */}

      <header className="analysis-header">

        <div>

          <div className="analysis-eyebrow">
            NEWS ANALYTICS
          </div>

          <h1>
            News Intelligence
          </h1>

          <p>
            Market news volume,
            sentiment analytics and
            publisher intelligence.
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
            loadNews(
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
          TICKERS
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
          ERROR
      ================================================== */}

      {error && (

        <div className="analysis-error">

          <Activity size={18} />

          <div>

            <strong>
              Unable to load news
            </strong>

            <span>
              {error}
            </span>

          </div>

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
          news intelligence...

        </div>
      )}


      {!loading &&
        !error &&
        summary && (

        <>


          {/* ==================================================
              COMPANY SUMMARY
          ================================================== */}

          <section className="news-company-card">

            <div className="news-company-identity">

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
                  {summary.company_name}
                </h2>

                <small>
                  Data as of{" "}
                  {formatDate(
                    summary.data_as_of
                  )}
                </small>

              </div>

            </div>


            <div className="news-overall-sentiment">

              {Number(
                summary.sentiment_score_30d
              ) >= 0 ? (

                <TrendingUp
                  size={20}
                />

              ) : (

                <TrendingDown
                  size={20}
                />

              )}


              <div>

                <span>
                  30D SENTIMENT SCORE
                </span>

                <strong
                  className={getScoreClass(
                    summary.sentiment_score_30d
                  )}
                >

                  {formatScore(
                    summary.sentiment_score_30d
                  )}

                </strong>

              </div>


              <span
                className={`news-sentiment-badge ${getSentimentClass(
                  summary
                    .dominant_sentiment_30d
                )}`}
              >

                {
                  summary
                    .dominant_sentiment_30d
                }

              </span>

            </div>

          </section>


          {/* ==================================================
              KPI
          ================================================== */}

          <section className="analysis-kpi-grid">

            <article className="analysis-kpi">

              <span>
                TOTAL NEWS
              </span>

              <strong>
                {summary.total_news_count}
              </strong>

              <small>
                Available articles
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                NEWS 7D
              </span>

              <strong>
                {summary.news_7d_count}
              </strong>

              <small>
                Last seven days
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                NEWS 30D
              </span>

              <strong>
                {summary.news_30d_count}
              </strong>

              <small>
                Last thirty days
              </small>

            </article>


            <article className="analysis-kpi">

              <span>
                PUBLISHERS
              </span>

              <strong>
                {summary.publisher_count_30d}
              </strong>

              <small>
                Unique 30D sources
              </small>

            </article>

          </section>


          {/* ==================================================
              SENTIMENT DISTRIBUTION
          ================================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  SENTIMENT DISTRIBUTION
                </span>

                <h3>
                  30-Day News Sentiment
                </h3>

                <p>
                  Sentiment coverage{" "}
                  {formatPercent(
                    summary
                      .sentiment_coverage_30d_pct
                  )}
                </p>

              </div>


              <Activity size={22} />

            </div>


            <div className="news-sentiment-grid">

              <div className="news-sentiment-stat">

                <span>
                  POSITIVE
                </span>

                <strong className="news-positive-text">
                  {
                    summary
                      .positive_30d_count
                  }
                </strong>

                <small>
                  {formatPercent(
                    summary
                      .positive_share_30d_pct
                  )}
                </small>

              </div>


              <div className="news-sentiment-stat">

                <span>
                  NEGATIVE
                </span>

                <strong className="news-negative-text">
                  {
                    summary
                      .negative_30d_count
                  }
                </strong>

                <small>
                  {formatPercent(
                    summary
                      .negative_share_30d_pct
                  )}
                </small>

              </div>


              <div className="news-sentiment-stat">

                <span>
                  NEUTRAL
                </span>

                <strong className="news-neutral-text">
                  {
                    summary
                      .neutral_30d_count
                  }
                </strong>

                <small>
                  {formatPercent(
                    summary
                      .neutral_share_30d_pct
                  )}
                </small>

              </div>


              <div className="news-sentiment-stat">

                <span>
                  AVG / DAY
                </span>

                <strong>
                  {formatNumber(
                    summary
                      .avg_news_per_day_30d
                  )}
                </strong>

                <small>
                  News volume
                </small>

              </div>

            </div>


            <div className="news-distribution-bar">

              <div
                className="news-distribution-positive"
                style={{
                  width:
                    `${summary
                      .positive_share_30d_pct}%`,
                }}
              />

              <div
                className="news-distribution-neutral"
                style={{
                  width:
                    `${summary
                      .neutral_share_30d_pct}%`,
                }}
              />

              <div
                className="news-distribution-negative"
                style={{
                  width:
                    `${summary
                      .negative_share_30d_pct}%`,
                }}
              />

            </div>

          </section>


          {/* ==================================================
              TREND CHART
          ================================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  NEWS TREND
                </span>

                <h3>
                  Sentiment & News Volume
                </h3>

                <p>
                  Last {
                    history.length
                  } reported days
                </p>

              </div>


              <Newspaper size={22} />

            </div>


            <div className="analysis-chart">

              <ResponsiveContainer
                width="100%"
                height={360}
              >

                <ComposedChart
                  data={chartData}
                  margin={{
                    top: 10,
                    right: 15,
                    bottom: 0,
                    left: 0,
                  }}
                >

                  <CartesianGrid
                    stroke="#ececf0"
                    strokeDasharray="3 3"
                    vertical={false}
                  />


                  <XAxis
                    dataKey="chartDate"
                    axisLine={false}
                    tickLine={false}
                    minTickGap={25}
                    tick={{
                      fill: "#86868b",
                      fontSize: 11,
                    }}
                  />


                  <YAxis
                    yAxisId="sentiment"
                    domain={[
                      -100,
                      100,
                    ]}
                    axisLine={false}
                    tickLine={false}
                    width={48}
                    tick={{
                      fill: "#86868b",
                      fontSize: 11,
                    }}
                  />


                  <YAxis
                    yAxisId="volume"
                    orientation="right"
                    axisLine={false}
                    tickLine={false}
                    width={38}
                    tick={{
                      fill: "#86868b",
                      fontSize: 11,
                    }}
                  />


                  <Tooltip
                    contentStyle={{
                      background:
                        "rgba(255,255,255,0.98)",
                      border:
                        "1px solid rgba(0,0,0,0.08)",
                      borderRadius:
                        "12px",
                      boxShadow:
                        "0 12px 30px rgba(0,0,0,0.10)",
                    }}
                  />


                  <Legend />


                  <Bar
                    yAxisId="volume"
                    dataKey="news_count"
                    name="News Volume"
                    fill="#dcecff"
                    radius={[
                      4,
                      4,
                      0,
                      0,
                    ]}
                    barSize={14}
                  />


                  <Line
                    yAxisId="sentiment"
                    type="monotone"
                    dataKey="sentiment_score"
                    name="Sentiment Score"
                    stroke="#0071e3"
                    strokeWidth={3}
                    dot={false}
                    activeDot={{
                      r: 4,
                    }}
                  />

                </ComposedChart>

              </ResponsiveContainer>

            </div>

          </section>


          {/* ==================================================
              LATEST ARTICLE
          ================================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  LATEST ARTICLE
                </span>

                <h3>
                  Most Recent News
                </h3>

                <p>
                  {formatDateTime(
                    summary.latest_news_at
                  )}
                </p>

              </div>


              <Globe2 size={22} />

            </div>


            <article className="latest-news-card">

              <div>

                <div className="latest-news-meta">

                  <span>
                    {
                      summary
                        .latest_news_publisher
                    }
                  </span>


                  <span
                    className={`news-sentiment-badge ${getSentimentClass(
                      summary
                        .latest_news_sentiment
                    )}`}
                  >

                    {
                      summary
                        .latest_news_sentiment
                    }

                  </span>

                </div>


                <h4>
                  {
                    summary
                      .latest_news_title
                  }
                </h4>

              </div>


              {summary.latest_news_url && (

                <a
                  href={
                    summary.latest_news_url
                  }
                  target="_blank"
                  rel="noreferrer"
                  className="news-open-button"
                >

                  Open article

                  <ExternalLink
                    size={14}
                  />

                </a>

              )}

            </article>

          </section>


          {/* ==================================================
              HISTORY TABLE
          ================================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  DAILY HISTORY
                </span>

                <h3>
                  News Analytics History
                </h3>

                <p>
                  Daily Gold-layer
                  aggregation
                </p>

              </div>


              <Newspaper size={22} />

            </div>


            <div className="analysis-table-wrapper">

              <table className="analysis-table news-history-table">

                <thead>

                  <tr>

                    <th>DATE</th>

                    <th>NEWS</th>

                    <th>SCORE</th>

                    <th>SENTIMENT</th>

                    <th>POSITIVE</th>

                    <th>NEGATIVE</th>

                    <th>NEUTRAL</th>

                    <th>PUBLISHERS</th>

                  </tr>

                </thead>


                <tbody>

                  {[...history]
                    .reverse()
                    .map(
                      (
                        row,
                        index
                      ) => (

                        <tr
                          key={`${row.published_date}-${index}`}
                        >

                          <td>
                            {formatDate(
                              row.published_date
                            )}
                          </td>


                          <td>
                            <strong>
                              {
                                row.news_count
                              }
                            </strong>
                          </td>


                          <td
                            className={getScoreClass(
                              row.sentiment_score
                            )}
                          >

                            {formatScore(
                              row.sentiment_score
                            )}

                          </td>


                          <td>

                            <span
                              className={`news-sentiment-badge ${getSentimentClass(
                                row
                                  .dominant_sentiment
                              )}`}
                            >

                              {
                                row
                                  .dominant_sentiment
                              }

                            </span>

                          </td>


                          <td className="news-positive-text">
                            {
                              row
                                .positive_count
                            }
                          </td>


                          <td className="news-negative-text">
                            {
                              row
                                .negative_count
                            }
                          </td>


                          <td className="news-neutral-text">
                            {
                              row
                                .neutral_count
                            }
                          </td>


                          <td>
                            {
                              row
                                .publisher_count
                            }
                          </td>

                        </tr>
                      )
                    )}

                </tbody>

              </table>

            </div>

          </section>

        </>
      )}

    </section>
  );
}


export default NewsIntelligence;