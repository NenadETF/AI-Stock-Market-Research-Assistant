import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  AlertTriangle,
  Bot,
  CheckCircle2,
  CircleAlert,
  Database,
  Sparkles,
  TrendingUp,
} from "lucide-react";

import {
  createResearchReport,
} from "../services/api";


/* =========================================================
   HELPERS
========================================================= */

function getTicker(item) {
  if (typeof item === "string") {
    return item;
  }

  return item?.ticker || "";
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

  return value
    .replaceAll("_", " ");
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

    default:
      return "research-assessment-neutral";
  }
}


/* =========================================================
   COMPONENT
========================================================= */

function AIResearch({
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
    question,
    setQuestion,
  ] = useState(
    "Does the current market performance, technical momentum, recent news sentiment and anomaly activity support my investment thesis for this stock?"
  );


  const [
    reportResponse,
    setReportResponse,
  ] = useState(null);


  const [
    generating,
    setGenerating,
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
     CURRENT WATCHLIST ITEM
  ======================================================= */

  const selectedWatchlistItem =
    useMemo(
      () =>
        watchlist.find(
          (item) =>
            getTicker(item) ===
            selectedTicker
        ) || null,
      [
        watchlist,
        selectedTicker,
      ]
    );


  const investmentThesis =
    selectedWatchlistItem
      ?.investment_thesis ||
    "";


  /* =======================================================
     CHANGE TICKER
  ======================================================= */

  function handleTickerChange(
    ticker
  ) {

    setSelectedTicker(
      ticker
    );

    setReportResponse(null);

    setError("");
  }


  /* =======================================================
     GENERATE
  ======================================================= */

  async function handleGenerateReport(
    event
  ) {

    event.preventDefault();


    const cleanQuestion =
      question.trim();


    if (!selectedTicker) {

      setError(
        "Please select a ticker."
      );

      return;
    }


    if (!cleanQuestion) {

      setError(
        "Research question is required."
      );

      return;
    }


    if (
      cleanQuestion.length >
      4000
    ) {

      setError(
        "Research question cannot exceed 4000 characters."
      );

      return;
    }


    try {

      setGenerating(true);

      setError("");


      const response =
        await createResearchReport({
          ticker:
            selectedTicker,

          question:
            cleanQuestion,
        });


      setReportResponse(
        response
      );

    } catch (err) {

      console.error(
        "AI Research failed:",
        err
      );


      setError(
        err.message ||
        "Unable to generate research report."
      );

    } finally {

      setGenerating(false);
    }
  }


  /* =======================================================
     EMPTY WATCHLIST
  ======================================================= */

  if (
    tickers.length === 0
  ) {

    return (

      <section className="research-page">

        <div className="analysis-empty">

          <Bot size={42} />

          <h2>
            AI Research
          </h2>

          <p>
            Add a stock to your
            watchlist before generating
            a research report.
          </p>

        </div>

      </section>
    );
  }


  const report =
    reportResponse?.report;


  /* =======================================================
     UI
  ======================================================= */

  return (

    <section className="research-page">


      {/* ==================================================
          HEADER
      ================================================== */}

      <header className="analysis-header">

        <div>

          <div className="analysis-eyebrow">
            RESEARCH AGENT
          </div>

          <h1>
            AI Research
          </h1>

          <p>
            Generate grounded,
            personalized stock research
            using market analytics,
            news sentiment and anomaly
            intelligence.
          </p>

        </div>


        <div className="research-agent-status">

          <span className="research-agent-dot" />

          <div>

            <strong>
              AGENT READY
            </strong>

            <small>
              Groq · GPT OSS 120B
            </small>

          </div>

        </div>

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
                handleTickerChange(
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
          RESEARCH CONFIGURATION
      ================================================== */}

      <section className="research-config-grid">


        {/* THESIS */}

        <article className="analysis-panel research-thesis-panel">

          <div className="analysis-panel-header">

            <div>

              <span>
                INVESTMENT THESIS
              </span>

              <h3>
                {selectedTicker} Thesis
              </h3>

              <p>
                Loaded from your
                Lakebase watchlist.
              </p>

            </div>


            <TrendingUp size={22} />

          </div>


          {investmentThesis ? (

            <p className="research-thesis-text">

              {investmentThesis}

            </p>

          ) : (

            <div className="research-thesis-empty">

              No investment thesis has
              been saved for{" "}
              {selectedTicker}.

            </div>

          )}

        </article>


        {/* CONTEXT */}

        <article className="analysis-panel research-context-panel">

          <div className="analysis-panel-header">

            <div>

              <span>
                RESEARCH CONTEXT
              </span>

              <h3>
                Agent Data Sources
              </h3>

            </div>


            <Database size={22} />

          </div>


          <div className="research-context-list">

            <div>

              <CheckCircle2
                size={16}
              />

              <span>
                Gold market analytics
              </span>

            </div>


            <div>

              <CheckCircle2
                size={16}
              />

              <span>
                Technical indicators
              </span>

            </div>


            <div>

              <CheckCircle2
                size={16}
              />

              <span>
                News sentiment
              </span>

            </div>


            <div>

              <CheckCircle2
                size={16}
              />

              <span>
                Anomaly intelligence
              </span>

            </div>


            <div>

              <CheckCircle2
                size={16}
              />

              <span>
                Personal investment thesis
              </span>

            </div>

          </div>

        </article>

      </section>


      {/* ==================================================
          QUESTION FORM
      ================================================== */}

      <section className="analysis-panel">

        <div className="analysis-panel-header">

          <div>

            <span>
              RESEARCH QUESTION
            </span>

            <h3>
              Ask the Research Agent
            </h3>

            <p>
              The agent will build a
              grounded report using
              the selected stock's
              available data.
            </p>

          </div>


          <Sparkles size={22} />

        </div>


        <form
          className="research-form"
          onSubmit={
            handleGenerateReport
          }
        >

          <textarea
            value={question}
            onChange={
              (event) =>
                setQuestion(
                  event.target.value
                )
            }
            rows={5}
            maxLength={4000}
            placeholder="Enter your research question..."
            disabled={generating}
          />


          <div className="research-form-footer">

            <span>
              {question.length}
              /4000
            </span>


            <button
              type="submit"
              className="primary-button research-generate-button"
              disabled={
                generating ||
                !question.trim() ||
                !selectedTicker
              }
            >

              {generating ? (

                <>
                  <span className="research-button-spinner" />

                  GENERATING REPORT...
                </>

              ) : (

                <>
                  <Bot size={16} />

                  GENERATE RESEARCH
                </>
              )}

            </button>

          </div>

        </form>


        {error && (

          <div className="research-error">

            <AlertTriangle
              size={18}
            />

            <div>

              <strong>
                Research generation failed
              </strong>

              <span>
                {error}
              </span>

            </div>

          </div>
        )}

      </section>


      {/* ==================================================
          GENERATING STATE
      ================================================== */}

      {generating && (

        <section className="research-generating">

          <div className="research-generating-icon">

            <Bot size={30} />

          </div>


          <div>

            <strong>
              Research Agent is analyzing{" "}
              {selectedTicker}
            </strong>

            <span>
              Combining market metrics,
              technical indicators, news
              sentiment and anomaly
              activity...
            </span>

          </div>

        </section>
      )}


      {/* ==================================================
          REPORT
      ================================================== */}

      {!generating &&
        reportResponse &&
        report && (

        <>


          {/* ==============================================
              REPORT HEADER
          ============================================== */}

          <section className="research-report-header">

            <div>

              <div className="analysis-eyebrow">
                GENERATED RESEARCH REPORT
              </div>

              <h2>
                {reportResponse.ticker}
                {" "}
                Research Report
              </h2>

              <p>
                Report #
                {reportResponse.report_id}
                {" · "}
                Generated{" "}
                {formatDateTime(
                  reportResponse.created_at
                )}
              </p>

            </div>


            <div
              className={`research-assessment ${getAssessmentClass(
                report
                  .thesis_alignment
                  ?.assessment
              )}`}
            >

              <span>
                THESIS ASSESSMENT
              </span>

              <strong>
                {formatAssessment(
                  report
                    .thesis_alignment
                    ?.assessment
                )}
              </strong>

            </div>

          </section>


          {/* ==============================================
              REPORT METADATA
          ============================================== */}

          <section className="research-metadata">

            <div>

              <span>
                MODEL
              </span>

              <strong>
                {reportResponse.model}
              </strong>

            </div>


            <div>

              <span>
                MARKET DATA AS OF
              </span>

              <strong>
                {formatDate(
                  reportResponse.data_as_of
                )}
              </strong>

            </div>


            <div>

              <span>
                REPORT DATA AS OF
              </span>

              <strong>
                {formatDate(
                  report.data_as_of
                )}
              </strong>

            </div>


            <div>

              <span>
                REPORT ID
              </span>

              <strong>
                #
                {reportResponse.report_id}
              </strong>

            </div>

          </section>


          {/* ==============================================
              EXECUTIVE SUMMARY
          ============================================== */}

          <ReportSection
            title="Executive Summary"
            label="SUMMARY"
            text={
              report.executive_summary
            }
          />


          {/* ==============================================
              ANALYSIS GRID
          ============================================== */}

          <div className="research-report-grid">

            <ReportSection
              title="Market Analysis"
              label="MARKET"
              text={
                report.market_analysis
              }
            />


            <ReportSection
              title="Technical Analysis"
              label="TECHNICAL"
              text={
                report.technical_analysis
              }
            />


            <ReportSection
              title="News Analysis"
              label="NEWS"
              text={
                report.news_analysis
              }
            />


            <ReportSection
              title="Anomaly Analysis"
              label="ANOMALIES"
              text={
                report.anomaly_analysis
              }
            />

          </div>


          {/* ==============================================
              THESIS ALIGNMENT
          ============================================== */}

          <section className="analysis-panel">

            <div className="analysis-panel-header">

              <div>

                <span>
                  THESIS ALIGNMENT
                </span>

                <h3>
                  Supporting &
                  Contradicting Evidence
                </h3>

              </div>


              <CircleAlert
                size={22}
              />

            </div>


            <div className="research-evidence-grid">


              <div className="research-evidence positive">

                <h4>
                  Supporting Evidence
                </h4>


                <ul>

                  {report
                    .thesis_alignment
                    ?.supporting_evidence
                    ?.map(
                      (
                        item,
                        index
                      ) => (

                        <li
                          key={index}
                        >
                          {item}
                        </li>

                      )
                    )}

                </ul>

              </div>


              <div className="research-evidence negative">

                <h4>
                  Contradicting Evidence
                </h4>


                <ul>

                  {report
                    .thesis_alignment
                    ?.contradicting_evidence
                    ?.map(
                      (
                        item,
                        index
                      ) => (

                        <li
                          key={index}
                        >
                          {item}
                        </li>

                      )
                    )}

                </ul>

              </div>

            </div>

          </section>


          {/* ==============================================
              CATALYSTS / RISKS
          ============================================== */}

          <section className="research-report-grid">

            <ListSection
              title="Key Catalysts"
              label="CATALYSTS"
              items={
                report.key_catalysts
              }
              type="positive"
            />


            <ListSection
              title="Key Risks"
              label="RISKS"
              items={
                report.key_risks
              }
              type="negative"
            />

          </section>


          {/* ==============================================
              LIMITATIONS
          ============================================== */}

          <ListSection
            title="Data Limitations"
            label="LIMITATIONS"
            items={
              report.data_limitations
            }
            type="neutral"
          />


          {/* ==============================================
              CONCLUSION
          ============================================== */}

          <section className="analysis-panel research-conclusion">

            <div className="analysis-panel-header">

              <div>

                <span>
                  CONCLUSION
                </span>

                <h3>
                  Research Conclusion
                </h3>

              </div>


              <Bot size={22} />

            </div>


            <p>
              {report.conclusion}
            </p>


            {report.disclaimer && (

              <div className="research-disclaimer">

                <CircleAlert
                  size={15}
                />

                {report.disclaimer}

              </div>
            )}

          </section>

        </>
      )}

    </section>
  );
}


/* =========================================================
   REPORT SECTION
========================================================= */

function ReportSection({
  label,
  title,
  text,
}) {

  if (!text) {
    return null;
  }


  return (

    <section className="analysis-panel research-report-section">

      <div className="research-section-label">
        {label}
      </div>

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

function ListSection({
  label,
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

    <section className="analysis-panel research-list-section">

      <div className="research-section-label">
        {label}
      </div>

      <h3>
        {title}
      </h3>


      <ul
        className={`research-list ${type}`}
      >

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


export default AIResearch;