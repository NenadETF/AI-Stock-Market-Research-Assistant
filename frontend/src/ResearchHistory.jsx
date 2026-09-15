import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  AlertTriangle,
  Bot,
  CalendarDays,
  FileText,
  History,
  RefreshCw,
  Trash2,
} from "lucide-react";

import {
  deleteResearchReport,
  getResearchHistory,
  getResearchReport,
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


function formatAssessment(value) {
  if (!value) {
    return "UNKNOWN";
  }

  return value.replaceAll(
    "_",
    " "
  );
}


function getAssessmentClass(value) {
  switch (
    value?.toUpperCase()
  ) {
    case "SUPPORTS":
    case "STRONGLY_SUPPORTS":
      return "research-assessment-positive";

    case "PARTIALLY_SUPPORTS":
    case "MIXED":
      return "research-assessment-mixed";

    case "DOES_NOT_SUPPORT":
    case "CONTRADICTS":
      return "research-assessment-negative";

    case "INSUFFICIENT_DATA":
      return "research-assessment-neutral";

    default:
      return "research-assessment-neutral";
  }
}


/* =========================================================
   COMPONENT
========================================================= */

function ResearchHistory() {

  const [
    reports,
    setReports,
  ] = useState([]);


  const [
    selectedReportId,
    setSelectedReportId,
  ] = useState(null);


  const [
    selectedReport,
    setSelectedReport,
  ] = useState(null);


  const [
    selectedTicker,
    setSelectedTicker,
  ] = useState("ALL");


  const [
    loading,
    setLoading,
  ] = useState(true);


  const [
    refreshing,
    setRefreshing,
  ] = useState(false);


  const [
    detailLoading,
    setDetailLoading,
  ] = useState(false);


  const [
    deletingId,
    setDeletingId,
  ] = useState(null);


  const [
    error,
    setError,
  ] = useState("");


  const [
    detailError,
    setDetailError,
  ] = useState("");


  /* =======================================================
     LOAD REPORT LIST
  ======================================================= */

  const loadReports =
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


          const data =
            await getResearchHistory();


          const rows =
            normalizeCollection(
              data
            );


          setReports(rows);


          return rows;

        } catch (err) {

          console.error(
            "Research history load failed:",
            err
          );


          setReports([]);


          setError(
            err.message ||
            "Unable to load research history."
          );


          return [];

        } finally {

          setLoading(false);

          setRefreshing(false);
        }

      },
      []
    );


  /* =======================================================
     INITIAL LOAD
  ======================================================= */

  useEffect(() => {

    let active = true;


    async function initialLoad() {

      const rows =
        await loadReports();


      if (
        active &&
        rows.length > 0
      ) {
        setSelectedReportId(
          rows[0].report_id
        );
      }
    }


    initialLoad();


    return () => {
      active = false;
    };

  }, [loadReports]);


  /* =======================================================
     LOAD REPORT DETAIL
  ======================================================= */

  useEffect(() => {

    if (!selectedReportId) {
      setSelectedReport(null);
      return;
    }


    let active = true;


    async function loadDetail() {

      try {

        setDetailLoading(true);

        setDetailError("");


        const data =
          await getResearchReport(
            selectedReportId
          );


        if (active) {
          setSelectedReport(
            data
          );
        }

      } catch (err) {

        console.error(
          "Research report detail failed:",
          err
        );


        if (active) {

          setSelectedReport(null);


          setDetailError(
            err.message ||
            "Unable to load report detail."
          );
        }

      } finally {

        if (active) {
          setDetailLoading(false);
        }
      }
    }


    loadDetail();


    return () => {
      active = false;
    };

  }, [selectedReportId]);


  /* =======================================================
     AVAILABLE TICKERS
  ======================================================= */

  const tickers =
    useMemo(
      () =>
        [
          ...new Set(
            reports
              .map(
                (report) =>
                  report.ticker
              )
              .filter(Boolean)
          ),
        ].sort(),
      [reports]
    );


  /* =======================================================
     FILTERED REPORTS
  ======================================================= */

  const filteredReports =
    useMemo(
      () => {

        if (
          selectedTicker ===
          "ALL"
        ) {
          return reports;
        }


        return reports.filter(
          (report) =>
            report.ticker ===
            selectedTicker
        );

      },
      [
        reports,
        selectedTicker,
      ]
    );


  /* =======================================================
     TICKER FILTER
  ======================================================= */

  function handleTickerFilter(
    ticker
  ) {

    setSelectedTicker(
      ticker
    );


    const matchingReports =
      ticker === "ALL"
        ? reports
        : reports.filter(
            (report) =>
              report.ticker ===
              ticker
          );


    if (
      matchingReports.length > 0
    ) {
      setSelectedReportId(
        matchingReports[0]
          .report_id
      );
    } else {
      setSelectedReportId(null);
    }
  }


  /* =======================================================
     REFRESH
  ======================================================= */

  async function handleRefresh() {

    const rows =
      await loadReports(true);


    if (
      rows.length === 0
    ) {

      setSelectedReportId(null);

      setSelectedReport(null);

      return;
    }


    const selectedStillExists =
      rows.some(
        (report) =>
          report.report_id ===
          selectedReportId
      );


    if (!selectedStillExists) {

      setSelectedReportId(
        rows[0].report_id
      );
    }
  }


  /* =======================================================
     DELETE REPORT
  ======================================================= */

  async function handleDeleteReport(
    reportId,
    event
  ) {

    if (event) {
      event.stopPropagation();
    }


    const confirmed =
      window.confirm(
        `Delete research report #${reportId}?`
      );


    if (!confirmed) {
      return;
    }


    try {

      setDeletingId(
        reportId
      );


      setError("");


      await deleteResearchReport(
        reportId
      );


      const rows =
        await loadReports(true);


      if (
        reportId ===
        selectedReportId
      ) {

        setSelectedReport(null);


        if (
          rows.length > 0
        ) {
          setSelectedReportId(
            rows[0].report_id
          );
        } else {
          setSelectedReportId(null);
        }

      } else {

        const selectedStillExists =
          rows.some(
            (report) =>
              report.report_id ===
              selectedReportId
          );


        if (
          !selectedStillExists
        ) {
          setSelectedReportId(
            rows[0]
              ?.report_id ||
            null
          );
        }
      }

    } catch (err) {

      console.error(
        "Research report delete failed:",
        err
      );


      setError(
        err.message ||
        "Unable to delete research report."
      );

    } finally {

      setDeletingId(null);
    }
  }


  /* =======================================================
     DERIVED VALUES
  ======================================================= */

  const latestReport =
    reports.length > 0
      ? reports[0]
      : null;


  const reportContent =
    selectedReport?.content ||
    {};


  /* =======================================================
     UI
  ======================================================= */

  return (

    <section className="history-page">


      {/* ==================================================
          HEADER
      ================================================== */}

      <header className="analysis-header">

        <div>

          <div className="analysis-eyebrow">
            RESEARCH ARCHIVE
          </div>

          <h1>
            Research History
          </h1>

          <p>
            Browse, inspect and manage
            previously generated AI
            research reports.
          </p>

        </div>


        <button
          type="button"
          className="refresh-button"
          disabled={
            loading ||
            refreshing
          }
          onClick={
            handleRefresh
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
          KPI
      ================================================== */}

      <section className="analysis-kpi-grid">

        <article className="analysis-kpi">

          <span>
            TOTAL REPORTS
          </span>

          <strong>
            {reports.length}
          </strong>

          <small>
            Saved research reports
          </small>

        </article>


        <article className="analysis-kpi">

          <span>
            STOCKS ANALYZED
          </span>

          <strong>
            {tickers.length}
          </strong>

          <small>
            Unique tickers
          </small>

        </article>


        <article className="analysis-kpi">

          <span>
            LATEST ASSESSMENT
          </span>

          <strong className="history-kpi-assessment">
            {formatAssessment(
              latestReport
                ?.thesis_assessment
            )}
          </strong>

          <small>
            Most recent report
          </small>

        </article>


        <article className="analysis-kpi">

          <span>
            LATEST REPORT
          </span>

          <strong>
            {latestReport
              ? `#${latestReport.report_id}`
              : "--"}
          </strong>

          <small>
            {latestReport
              ? formatDate(
                  latestReport
                    .created_at
                )
              : "No reports"}
          </small>

        </article>

      </section>


      {/* ==================================================
          FILTER
      ================================================== */}

      <div className="history-filter-bar">

        <button
          type="button"
          className={
            selectedTicker === "ALL"
              ? "history-filter active"
              : "history-filter"
          }
          onClick={() =>
            handleTickerFilter(
              "ALL"
            )
          }
        >
          ALL
        </button>


        {tickers.map(
          (ticker) => (

            <button
              key={ticker}
              type="button"
              className={
                selectedTicker ===
                ticker
                  ? "history-filter active"
                  : "history-filter"
              }
              onClick={() =>
                handleTickerFilter(
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

          <AlertTriangle
            size={18}
          />

          <div>

            <strong>
              Research history error
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

      {loading ? (

        <div className="analysis-loading">

          <RefreshCw
            size={22}
            className="refresh-spinning"
          />

          Loading research history...

        </div>

      ) : reports.length === 0 ? (

        <div className="analysis-empty">

          <History size={40} />

          <h2>
            No Research Reports
          </h2>

          <p>
            Generate a report from
            AI Research and it will
            appear here automatically.
          </p>

        </div>

      ) : (

        /* =================================================
           HISTORY LAYOUT
        ================================================= */

        <div className="history-layout">


          {/* ===============================================
              REPORT LIST
          =============================================== */}

          <section className="history-list-panel">

            <div className="history-list-header">

              <div>

                <span>
                  SAVED REPORTS
                </span>

                <strong>
                  {filteredReports.length}
                  {" "}
                  reports
                </strong>

              </div>


              <FileText size={19} />

            </div>


            <div className="history-report-list">

              {filteredReports.map(
                (report) => (

                  <button
                    key={
                      report.report_id
                    }
                    type="button"
                    className={
                      selectedReportId ===
                      report.report_id
                        ? "history-report-card active"
                        : "history-report-card"
                    }
                    onClick={() =>
                      setSelectedReportId(
                        report.report_id
                      )
                    }
                  >

                    <div className="history-report-card-top">

                      <div>

                        <span className="history-report-ticker">
                          {report.ticker}
                        </span>

                        <span className="history-report-id">
                          #{report.report_id}
                        </span>

                      </div>


                      <span
                        className={`history-assessment-badge ${getAssessmentClass(
                          report
                            .thesis_assessment
                        )}`}
                      >

                        {formatAssessment(
                          report
                            .thesis_assessment
                        )}

                      </span>

                    </div>


                    <strong className="history-report-title">

                      {report.title}

                    </strong>


                    <p>
                      {report.question}
                    </p>


                    <div className="history-report-footer">

                      <span>

                        <CalendarDays
                          size={12}
                        />

                        {formatDateTime(
                          report.created_at
                        )}

                      </span>


                      <span
                        role="button"
                        tabIndex={0}
                        className="history-delete-button"
                        title={`Delete report #${report.report_id}`}
                        onClick={
                          (event) =>
                            handleDeleteReport(
                              report.report_id,
                              event
                            )
                        }
                        onKeyDown={
                          (event) => {

                            if (
                              event.key ===
                                "Enter" ||
                              event.key ===
                                " "
                            ) {

                              event.preventDefault();

                              handleDeleteReport(
                                report.report_id,
                                event
                              );
                            }
                          }
                        }
                      >

                        {deletingId ===
                        report.report_id ? (

                          <RefreshCw
                            size={13}
                            className="refresh-spinning"
                          />

                        ) : (

                          <Trash2
                            size={13}
                          />

                        )}

                      </span>

                    </div>

                  </button>

                )
              )}

            </div>

          </section>


          {/* ===============================================
              REPORT DETAIL
          =============================================== */}

          <section className="history-detail-panel">

            {detailLoading ? (

              <div className="history-detail-loading">

                <RefreshCw
                  size={22}
                  className="refresh-spinning"
                />

                Loading report...

              </div>

            ) : detailError ? (

              <div className="analysis-error">

                <AlertTriangle
                  size={18}
                />

                <div>

                  <strong>
                    Unable to load report
                  </strong>

                  <span>
                    {detailError}
                  </span>

                </div>

              </div>

            ) : selectedReport ? (

              <>


                {/* =========================================
                    DETAIL HEADER
                ========================================= */}

                <div className="history-detail-header">

                  <div>

                    <div className="analysis-eyebrow">
                      REPORT #
                      {selectedReport.report_id}
                    </div>

                    <h2>
                      {selectedReport.title}
                    </h2>

                    <p>
                      Generated{" "}
                      {formatDateTime(
                        selectedReport
                          .created_at
                      )}
                    </p>

                  </div>


                  <span
                    className={`history-detail-assessment ${getAssessmentClass(
                      selectedReport
                        .thesis_assessment
                    )}`}
                  >

                    {formatAssessment(
                      selectedReport
                        .thesis_assessment
                    )}

                  </span>

                </div>


                {/* =========================================
                    META
                ========================================= */}

                <div className="history-detail-meta">

                  <div>

                    <span>
                      TICKER
                    </span>

                    <strong>
                      {selectedReport.ticker}
                    </strong>

                  </div>


                  <div>

                    <span>
                      DATA AS OF
                    </span>

                    <strong>
                      {formatDate(
                        selectedReport
                          .data_as_of
                      )}
                    </strong>

                  </div>


                  <div>

                    <span>
                      MODEL
                    </span>

                    <strong>
                      {selectedReport.model}
                    </strong>

                  </div>

                </div>


                {/* =========================================
                    QUESTION
                ========================================= */}

                <div className="history-detail-block">

                  <span>
                    RESEARCH QUESTION
                  </span>

                  <p>
                    {selectedReport.question}
                  </p>

                </div>


                {/* =========================================
                    THESIS
                ========================================= */}

                <div className="history-detail-block">

                  <span>
                    INVESTMENT THESIS
                  </span>

                  <p>
                    {selectedReport
                      .investment_thesis ||
                      "No investment thesis saved."}
                  </p>

                </div>


                {/* =========================================
                    SUMMARY
                ========================================= */}

                <HistoryTextSection
                  label="SUMMARY"
                  title="Executive Summary"
                  text={
                    reportContent
                      .executive_summary
                  }
                />


                {/* =========================================
                    ANALYSIS
                ========================================= */}

                <div className="history-analysis-grid">

                  <HistoryTextSection
                    label="MARKET"
                    title="Market Analysis"
                    text={
                      reportContent
                        .market_analysis
                    }
                  />


                  <HistoryTextSection
                    label="TECHNICAL"
                    title="Technical Analysis"
                    text={
                      reportContent
                        .technical_analysis
                    }
                  />


                  <HistoryTextSection
                    label="NEWS"
                    title="News Analysis"
                    text={
                      reportContent
                        .news_analysis
                    }
                  />


                  <HistoryTextSection
                    label="ANOMALIES"
                    title="Anomaly Analysis"
                    text={
                      reportContent
                        .anomaly_analysis
                    }
                  />

                </div>


                {/* =========================================
                    EVIDENCE
                ========================================= */}

                {reportContent
                  .thesis_alignment && (

                  <div className="history-evidence-grid">

                    <HistoryListSection
                      title="Supporting Evidence"
                      items={
                        reportContent
                          .thesis_alignment
                          .supporting_evidence
                      }
                      type="positive"
                    />


                    <HistoryListSection
                      title="Contradicting Evidence"
                      items={
                        reportContent
                          .thesis_alignment
                          .contradicting_evidence
                      }
                      type="negative"
                    />

                  </div>
                )}


                {/* =========================================
                    CATALYSTS / RISKS
                ========================================= */}

                <div className="history-evidence-grid">

                  <HistoryListSection
                    title="Key Catalysts"
                    items={
                      reportContent
                        .key_catalysts
                    }
                    type="positive"
                  />


                  <HistoryListSection
                    title="Key Risks"
                    items={
                      reportContent
                        .key_risks
                    }
                    type="negative"
                  />

                </div>


                {/* =========================================
                    LIMITATIONS
                ========================================= */}

                <HistoryListSection
                  title="Data Limitations"
                  items={
                    reportContent
                      .data_limitations
                  }
                  type="neutral"
                />


                {/* =========================================
                    CONCLUSION
                ========================================= */}

                <HistoryTextSection
                  label="CONCLUSION"
                  title="Research Conclusion"
                  text={
                    reportContent
                      .conclusion
                  }
                />


                {reportContent
                  .disclaimer && (

                  <div className="history-disclaimer">

                    <Bot size={14} />

                    {
                      reportContent
                        .disclaimer
                    }

                  </div>
                )}

              </>

            ) : (

              <div className="history-detail-empty">

                <FileText
                  size={38}
                />

                <strong>
                  Select a report
                </strong>

                <span>
                  Choose a saved report
                  to view its full analysis.
                </span>

              </div>
            )}

          </section>

        </div>
      )}

    </section>
  );
}


/* =========================================================
   TEXT SECTION
========================================================= */

function HistoryTextSection({
  label,
  title,
  text,
}) {

  if (!text) {
    return null;
  }


  return (

    <section className="history-content-section">

      <span>
        {label}
      </span>

      <h3>
        {title}
      </h3>

      <p>
        {text}
      </p>

    </section>
  );
}


/* =========================================================
   LIST SECTION
========================================================= */

function HistoryListSection({
  title,
  items = [],
  type = "neutral",
}) {

  if (
    !items ||
    items.length === 0
  ) {
    return null;
  }


  return (

    <section
      className={`history-list-section ${type}`}
    >

      <h3>
        {title}
      </h3>


      <ul>

        {items.map(
          (
            item,
            index
          ) => (

            <li key={index}>
              {item}
            </li>

          )
        )}

      </ul>

    </section>
  );
}


export default ResearchHistory;