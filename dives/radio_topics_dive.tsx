// Kept byte-for-byte identical to .dive-preview/src/dive.tsx whenever this is
// the dive you're actively previewing — mirror any change there too. (Only
// one dive can be live in the preview app at a time; see README.md.)
import { useMemo } from "react";
import { useSQLQuery, useDiveState } from "@motherduck/react-sql-query";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Loader2 } from "lucide-react";

export const REQUIRED_DATABASES = [
  {
    type: "database",
    path: "md:f1",
    alias: "f1",
  },
];

const N = (v: unknown): number => (v != null ? Number(v) : 0);

const FCT_DRIVER_TOPIC_RACE = '"f1"."marts"."fct_driver_topic_race"';
const FCT_RADIO_MESSAGES = '"f1"."marts"."fct_radio_messages"';

const PALETTE = ["#0777b3", "#bd4e35", "#2d7a00", "#e18727", "#638CAD", "#8e44ad"];
const OTHER_COLOR = "#adadad";
const TOP_N_TOPICS = 6;

// Short suffix distinguishing same-weekend sessions (a circuit can now have
// up to three: Qualifying, Sprint, and Race all show radio traffic). "Race"
// gets no suffix since it's still the common case and the plain circuit
// label should keep meaning "the race" wherever only Race data exists.
function sessionSuffix(sessionName: unknown): string {
  if (sessionName == null) return "";
  const name = String(sessionName);
  if (name === "Race") return "";
  if (name === "Qualifying") return " (Q)";
  if (name === "Sprint") return " (S)";
  return ` (${name})`;
}

function raceLabel(row: Record<string, unknown>): string {
  return `${String(row.circuit_short_name)}${sessionSuffix(row.session_name)} — ${String(row.session_date).slice(0, 10)}`;
}

// Circuit + short session suffix, no date — used for the chart's x-axis,
// where raceLabel's full "circuit — date" text was long enough (especially
// rotated) to run over into the plotted bars. The session dropdown still
// uses raceLabel: there's no rotation/overflow issue there, and the date
// helps distinguish same-circuit sessions across seasons when "All" is picked.
function chartRaceLabel(row: Record<string, unknown>): string {
  return `${String(row.circuit_short_name)}${sessionSuffix(row.session_name)}`;
}

// A specific year, the "all" sentinel (no year filter), or null while the
// season list is still loading.
type Season = number | "all" | null;

/** SQL fragment restricting to a single season, or "" for "all"/null (no
 * filter — every query here already validated `season` against the known
 * season list or the "all" sentinel before it reaches SQL). */
function seasonFilter(season: Season): string {
  return season != null && season !== "all" ? `year = ${season}` : "";
}

/** Joins non-empty SQL conditions with `and`, prefixed with `where` — so
 * callers can freely mix an optional season filter with other conditions
 * without juggling `where`/`and` placement themselves. */
function whereClause(...conditions: string[]): string {
  const parts = conditions.filter(Boolean);
  return parts.length ? `where ${parts.join(" and ")}` : "";
}

export default function RadioTopicsDive() {
  const [view, setView] = useDiveState<"season" | "race">("view", "season");

  const seasonsQ = useSQLQuery(`
    select distinct year from ${FCT_DRIVER_TOPIC_RACE} order by year desc
  `);
  const seasons = (Array.isArray(seasonsQ.data) ? seasonsQ.data : []).map((r) => N(r.year));

  // Same URL-state-can't-be-trusted-verbatim rule as `entity`/`session`
  // below: only accept a season that's actually one of this query's results
  // (or the "all" sentinel, which is always valid).
  const [season, setSeason] = useDiveState<Season>("season_year", null);
  const effectiveSeason: Season =
    season === "all" || (season != null && seasons.includes(season))
      ? season
      : (seasons[0] ?? null);

  return (
    <div className="p-6" style={{ background: "#f8f8f8", maxWidth: 960, margin: "0 auto" }}>
      <h1 className="text-2xl font-semibold" style={{ color: "#231f20" }}>
        Team Radio Topics
      </h1>
      <p className="text-sm mb-6" style={{ color: "#6a6a6a" }}>
        What drivers and teams talk about on team radio, transcribed with Whisper and
        topic-modeled with BERTopic, tracked across the season.
      </p>

      <div className="flex gap-2 mb-4">
        <button
          onClick={() => setSeason("all")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: effectiveSeason === "all" ? "#231f20" : "transparent",
            color: effectiveSeason === "all" ? "#fff" : "#6a6a6a",
            border: `1px solid ${effectiveSeason === "all" ? "#231f20" : "#ddd"}`,
          }}
        >
          All
        </button>
        {seasons.map((y) => (
          <button
            key={y}
            onClick={() => setSeason(y)}
            className="text-sm px-3 py-1 rounded"
            style={{
              background: y === effectiveSeason ? "#231f20" : "transparent",
              color: y === effectiveSeason ? "#fff" : "#6a6a6a",
              border: `1px solid ${y === effectiveSeason ? "#231f20" : "#ddd"}`,
            }}
          >
            {y}
          </button>
        ))}
      </div>

      <div className="flex gap-2 mb-6">
        <button
          onClick={() => setView("season")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: view !== "race" ? "#0777b3" : "transparent",
            color: view !== "race" ? "#fff" : "#6a6a6a",
          }}
        >
          Season evolution
        </button>
        <button
          onClick={() => setView("race")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: view === "race" ? "#0777b3" : "transparent",
            color: view === "race" ? "#fff" : "#6a6a6a",
          }}
        >
          Session detail
        </button>
      </div>

      {view === "race" ? (
        <RaceTopicsDetail season={effectiveSeason} />
      ) : (
        <SeasonTopicsEvolution season={effectiveSeason} />
      )}
    </div>
  );
}

function SeasonTopicsEvolution({ season }: { season: Season }) {
  const [groupBy, setGroupBy] = useDiveState<"driver" | "team" | "all">("groupBy", "driver");

  const entitiesQ = useSQLQuery(
    `
      select distinct
        ${groupBy === "driver" ? "driver_acronym as entity" : "team_name as entity"}
      from ${FCT_DRIVER_TOPIC_RACE}
      ${whereClause(seasonFilter(season))}
      order by entity
    `,
    // "all" has no single entity to pick, so there's nothing for this query
    // to feed — skip it rather than fetch a dropdown that won't be shown.
    { enabled: season != null && groupBy !== "all" },
  );
  const entities = Array.isArray(entitiesQ.data) ? entitiesQ.data : [];

  // useDiveState persists to a URL fragment, so `entity` can come from a
  // shared/crafted link rather than the <select> below — only ever trust a
  // value that's actually one of this query's own results before it goes
  // into SQL, instead of interpolating whatever the URL says verbatim.
  const [entity, setEntity] = useDiveState<string | null>("entity", null);
  const knownEntities = new Set(entities.map((row) => String(row.entity)));
  const effectiveEntity =
    entity != null && knownEntities.has(entity)
      ? entity
      : entities.length
        ? String(entities[0].entity)
        : null;

  const rowsQ = useSQLQuery(
    `
      select
        session_key,
        circuit_short_name,
        session_name,
        session_date,
        topic_label,
        sum(message_count) as message_count
      from ${FCT_DRIVER_TOPIC_RACE}
      ${whereClause(
        seasonFilter(season),
        groupBy === "all"
          ? ""
          : `${groupBy === "driver" ? "driver_acronym" : "team_name"} = '${effectiveEntity}'`,
      )}
      group by 1, 2, 3, 4, 5
      order by session_date
    `,
    { enabled: season != null && (groupBy === "all" || effectiveEntity != null) },
  );
  const rows = Array.isArray(rowsQ.data) ? rowsQ.data : [];

  const { chartData, topics } = useMemo(() => buildStackedSeries(rows), [rows]);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="flex gap-2">
          {(["driver", "team", "all"] as const).map((g) => (
            <button
              key={g}
              onClick={() => {
                setGroupBy(g);
                setEntity(null);
              }}
              className="text-xs px-2 py-1 rounded"
              style={{
                background: groupBy === g ? "#231f20" : "transparent",
                color: groupBy === g ? "#fff" : "#6a6a6a",
                border: `1px solid ${groupBy === g ? "#231f20" : "#ddd"}`,
              }}
            >
              {g === "all" ? "All" : `By ${g}`}
            </button>
          ))}
        </div>

        {groupBy === "all" ? null : entitiesQ.isLoading ? (
          <div className="h-8 w-40 bg-gray-200 animate-pulse rounded" />
        ) : (
          <select
            className="text-sm border rounded px-2 py-1"
            style={{ color: "#231f20", borderColor: "#ddd" }}
            value={effectiveEntity ?? ""}
            onChange={(e) => setEntity(e.target.value)}
          >
            {entities.map((row) => (
              <option key={String(row.entity)} value={String(row.entity)}>
                {String(row.entity)}
              </option>
            ))}
          </select>
        )}
      </div>

      {rowsQ.isLoading ? (
        <div className="flex items-center gap-2" style={{ color: "#6a6a6a", height: 320 }}>
          <Loader2 className="animate-spin" size={16} /> Loading radio topics…
        </div>
      ) : rowsQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {rowsQ.error?.message}</p>
      ) : chartData.length === 0 ? (
        <p style={{ color: "#6a6a6a" }}>
          No radio messages found{groupBy === "all" ? "" : ` for ${effectiveEntity}`}.
        </p>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="race"
              fontSize={11}
              interval={0}
              angle={-30}
              textAnchor="end"
              height={80}
              tickMargin={12}
            />
            <YAxis fontSize={11} label={{ value: "Radio messages", angle: -90, position: "insideLeft", fontSize: 11 }} />
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            {topics.map((t, i) => (
              <Bar
                key={t}
                dataKey={t}
                stackId="topics"
                fill={t === "Other" ? OTHER_COLOR : PALETTE[i % PALETTE.length]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

/** Pivots long rows (one per race x topic) into one row per race with a
 * column per topic, keeping only the entity's top N topics by total volume
 * across the season and folding the rest into "Other" so the chart stays
 * readable even when BERTopic finds many small topics. */
function buildStackedSeries(rows: Record<string, unknown>[]) {
  const totalsByTopic = new Map<string, number>();
  for (const r of rows) {
    const topic = String(r.topic_label);
    totalsByTopic.set(topic, (totalsByTopic.get(topic) ?? 0) + N(r.message_count));
  }
  const topTopics = [...totalsByTopic.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, TOP_N_TOPICS)
    .map(([topic]) => topic);
  const topTopicSet = new Set(topTopics);
  const hasOther = totalsByTopic.size > topTopics.length;

  const byRace = new Map<number, Record<string, unknown>>();
  for (const r of rows) {
    const sessionKey = N(r.session_key);
    if (!byRace.has(sessionKey)) {
      byRace.set(sessionKey, { session_key: sessionKey, race: chartRaceLabel(r) });
    }
    const row = byRace.get(sessionKey)!;
    const topic = String(r.topic_label);
    const bucket = topTopicSet.has(topic) ? topic : "Other";
    row[bucket] = N(row[bucket]) + N(r.message_count);
  }

  const chartData = [...byRace.values()].sort((a, b) => N(a.session_key) - N(b.session_key));
  const topics = hasOther ? [...topTopics, "Other"] : topTopics;
  return { chartData, topics };
}

function RaceTopicsDetail({ season }: { season: Season }) {
  const sessionsQ = useSQLQuery(
    `
      select distinct session_key, circuit_short_name, session_name, session_date
      from ${FCT_DRIVER_TOPIC_RACE}
      ${whereClause(seasonFilter(season))}
      order by session_date
    `,
    { enabled: season != null },
  );
  const sessions = Array.isArray(sessionsQ.data) ? sessionsQ.data : [];

  // Same known-values guard as `entity` above — a `session` left over from a
  // previously-selected season (or a crafted link) shouldn't silently be
  // trusted just because it's non-null.
  const [sessionKey, setSessionKey] = useDiveState<number | null>("session", null);
  const knownSessionKeys = new Set(sessions.map((s) => N(s.session_key)));
  const effectiveSessionKey =
    sessionKey != null && knownSessionKeys.has(sessionKey)
      ? sessionKey
      : sessions.length
        ? N(sessions[sessions.length - 1].session_key)
        : null;

  const messagesQ = useSQLQuery(
    `
      select driver_acronym, team_colour, lap_number, message_date, topic_label, transcript_text
      from ${FCT_RADIO_MESSAGES}
      where session_key = ${effectiveSessionKey}
      order by message_date
    `,
    { enabled: effectiveSessionKey != null },
  );
  const messageRows = Array.isArray(messagesQ.data) ? messagesQ.data : [];

  // The topic breakdown is just a tally over messageRows — every message
  // already carries its topic_label — so derive it client-side instead of
  // firing a second query for the same session that duplicates work the
  // browser can do for free from data it already fetched.
  const breakdownRows = useMemo(() => {
    const counts = new Map<string, number>();
    for (const r of messageRows) {
      const topic = String(r.topic_label);
      counts.set(topic, (counts.get(topic) ?? 0) + 1);
    }
    return [...counts.entries()]
      .map(([topic_label, message_count]) => ({ topic_label, message_count }))
      .sort((a, b) => b.message_count - a.message_count);
  }, [messageRows]);
  const totalMessages = breakdownRows.reduce((sum, r) => sum + r.message_count, 0);

  return (
    <div>
      <div className="mb-4">
        {sessionsQ.isLoading ? (
          <div className="h-8 w-64 bg-gray-200 animate-pulse rounded" />
        ) : (
          <select
            className="text-sm border rounded px-2 py-1"
            style={{ color: "#231f20", borderColor: "#ddd" }}
            value={effectiveSessionKey ?? ""}
            onChange={(e) => setSessionKey(Number(e.target.value))}
          >
            {sessions.map((s) => (
              <option key={String(s.session_key)} value={N(s.session_key)}>
                {raceLabel(s)}
              </option>
            ))}
          </select>
        )}
      </div>

      <h2 className="text-sm font-semibold mb-2" style={{ color: "#231f20" }}>
        Topic breakdown
      </h2>
      {messagesQ.isLoading ? (
        <div className="animate-pulse space-y-2">
          <div className="h-4 bg-gray-200 rounded w-3/4" />
          <div className="h-4 bg-gray-200 rounded w-1/2" />
        </div>
      ) : messagesQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {messagesQ.error?.message}</p>
      ) : (
        <table className="w-full text-sm mb-6" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #ddd", color: "#6a6a6a", textAlign: "left" }}>
              <th className="py-2">Topic</th>
              <th className="py-2 text-right">Messages</th>
              <th className="py-2 text-right">Share</th>
            </tr>
          </thead>
          <tbody>
            {breakdownRows.map((r) => (
              <tr key={String(r.topic_label)} style={{ borderBottom: "1px solid #eee" }}>
                <td className="py-1" style={{ color: "#231f20", fontWeight: 600 }}>
                  {String(r.topic_label)}
                </td>
                <td className="py-1 text-right" style={{ color: "#231f20" }}>{N(r.message_count)}</td>
                <td className="py-1 text-right" style={{ color: "#6a6a6a" }}>
                  {totalMessages > 0 ? `${((N(r.message_count) / totalMessages) * 100).toFixed(0)}%` : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2 className="text-sm font-semibold mb-2" style={{ color: "#231f20" }}>
        Radio messages
      </h2>
      {messagesQ.isLoading ? (
        <div className="flex items-center gap-2" style={{ color: "#6a6a6a" }}>
          <Loader2 className="animate-spin" size={16} /> Loading messages…
        </div>
      ) : messagesQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {messagesQ.error?.message}</p>
      ) : (
        <table className="w-full text-sm" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #ddd", color: "#6a6a6a", textAlign: "left" }}>
              <th className="py-2">Driver</th>
              <th className="py-2">Lap</th>
              <th className="py-2">Topic</th>
              <th className="py-2">Transcript</th>
            </tr>
          </thead>
          <tbody>
            {messageRows.map((r, i) => (
              <tr key={i} style={{ borderBottom: "1px solid #eee" }}>
                <td className="py-1" style={{ color: `#${String(r.team_colour)}`, fontWeight: 600 }}>
                  {String(r.driver_acronym)}
                </td>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{r.lap_number != null ? N(r.lap_number) : "—"}</td>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{String(r.topic_label)}</td>
                <td className="py-1" style={{ color: "#231f20" }}>{String(r.transcript_text)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
