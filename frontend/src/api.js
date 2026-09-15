const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ||
  "http://127.0.0.1:8000";

const DEMO_USER_ID =
  import.meta.env.VITE_DEMO_USER_ID || "1";


/* =========================================================
   BASE REQUEST
========================================================= */

async function request(path, options = {}) {
  const response = await fetch(
    `${API_BASE_URL}${path}`,
    {
      headers: {
        "Content-Type": "application/json",
        ...options.headers,
      },
      ...options,
    }
  );

  if (!response.ok) {
    let message =
      `API error: ${response.status}`;

    try {
      const data =
        await response.json();

      if (data.detail) {
        message = data.detail;
      }
    } catch {
      // Response body is not JSON.
    }

    throw new Error(message);
  }

  if (response.status === 204) {
    return null;
  }

  return response.json();
}


/* =========================================================
   WATCHLIST
========================================================= */

export function getWatchlist(
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/watchlist`
  );
}


export function addToWatchlist(
  data,
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/watchlist`,
    {
      method: "POST",

      body: JSON.stringify({
        ticker:
          data.ticker
            .trim()
            .toUpperCase(),

        investment_thesis:
          data.investment_thesis ||
          null,

        notes:
          data.notes ||
          null,
      }),
    }
  );
}


export function removeFromWatchlist(
  ticker,
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/watchlist/${ticker
      .trim()
      .toUpperCase()}`,
    {
      method: "DELETE",
    }
  );
}


/* =========================================================
   MARKET ANALYTICS
========================================================= */

export function getLatestAnalytics(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/latest`
  );
}


export function getAnalyticsHistory(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/history`
  );
}


/* =========================================================
   NEWS INTELLIGENCE
========================================================= */

export function getNewsSummary(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/news/summary`
  );
}


export function getNewsHistory(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/news/history`
  );
}


/* =========================================================
   ANOMALIES
========================================================= */

export function getAnomalySummary(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/anomalies/summary`
  );
}


export function getAnomalies(
  ticker
) {
  return request(
    `/analytics/${ticker
      .trim()
      .toUpperCase()}/anomalies`
  );
}


/* =========================================================
   AI RESEARCH
========================================================= */

export function createResearchReport(
  data,
  userId = DEMO_USER_ID
) {
  return request(
    "/research",
    {
      method: "POST",

      body: JSON.stringify({
        user_id:
          Number(userId),

        ticker:
          data.ticker
            .trim()
            .toUpperCase(),

        question:
          data.question
            .trim(),
      }),
    }
  );
}


/* =========================================================
   RESEARCH HISTORY
========================================================= */

export function getResearchHistory(
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/research`
  );
}


export function getResearchReport(
  reportId,
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/research/${reportId}`
  );
}


export function deleteResearchReport(
  reportId,
  userId = DEMO_USER_ID
) {
  return request(
    `/users/${userId}/research/${reportId}`,
    {
      method: "DELETE",
    }
  );
}


/* =========================================================
   SYSTEM STATUS / DATA FRESHNESS
========================================================= */

export function getDataFreshness() {
  return request(
    "/system/data-freshness"
  );
}


/* =========================================================
   FULL MARKET DATA REFRESH
========================================================= */

export function refreshMarketData() {
  return request(
    "/system/refresh-market-data",
    {
      method: "POST",
    }
  );
}


/* =========================================================
   CONFIG
========================================================= */

export {
  API_BASE_URL,
  DEMO_USER_ID,
};