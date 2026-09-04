// Kept byte-for-byte identical to .dive-preview/src/dive.tsx whenever this is
// the dive you're actively previewing — mirror any change there too. (Only
// one dive can be live in the preview app at a time; see README.md.)
import { useMemo } from "react";
import { useSQLQuery, useDiveState } from "@motherduck/react-sql-query";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Loader2 } from "lucide-react";

// Must stay on one line — the Blueprints deployer strips this declaration with
// a single-line regex when it uploads the source, and blueprint.yml's
// requiredResources is what MotherDuck actually mounts. A multi-line form
// leaves the array body behind as a syntax error. Guarded by `make test`.
export const REQUIRED_DATABASES = [{ type: "database", path: "md:f1", alias: "f1" }];

const N = (v: unknown): number => (v != null ? Number(v) : 0);

const FCT_DRIVER_TOPIC_RACE = '"f1"."marts"."fct_driver_topic_race"';
const FCT_RADIO_MESSAGES = '"f1"."marts"."fct_radio_messages"';

// Sized to TOP_N_SERIES + OTHER_BUCKET_SLACK so every series shown without an
// "Other" bucket (see buildStackedSeries) still gets its own color instead of
// the palette wrapping around and reusing one.
const PALETTE = ["#0777b3", "#bd4e35", "#2d7a00", "#e18727", "#638CAD", "#8e44ad", "#c2185b", "#00897b"];
const OTHER_COLOR = "#adadad";
const TOP_N_SERIES = 6;
// Folding a mere handful of leftover series into "Other" doesn't actually
// simplify the chart -- it just swaps one small series' real label for an
// equally-small "Other" one. Only bucket once the long tail is more than
// this many series deep; otherwise show everything.
const OTHER_BUCKET_SLACK = 2;

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
  const [view, setView] = useDiveState<"season" | "topic" | "race">("view", "season");

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
            background: view === "season" ? "#0777b3" : "transparent",
            color: view === "season" ? "#fff" : "#6a6a6a",
          }}
        >
          Season evolution
        </button>
        <button
          onClick={() => setView("topic")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: view === "topic" ? "#0777b3" : "transparent",
            color: view === "topic" ? "#fff" : "#6a6a6a",
          }}
        >
          Topic over season
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
      ) : view === "topic" ? (
        <TopicOverSeason season={effectiveSeason} />
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
        year,
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
      group by 1, 2, 3, 4, 5, 6
      order by session_date
    `,
    { enabled: season != null && (groupBy === "all" || effectiveEntity != null) },
  );
  const rows = Array.isArray(rowsQ.data) ? rowsQ.data : [];

  const { chartData, series: topics } = useMemo(
    () => buildStackedSeries(rows, (r) => String(r.topic_label)),
    [rows],
  );

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
        <SeasonSeriesChart season={season} chartData={chartData} series={topics} />
      )}
    </div>
  );
}

/** Pivots long rows (one per race x series) into one row per race with a
 * column per series, keeping only the top N series by total volume across
 * the season and folding the rest into "Other" so the chart stays readable —
 * unless there are only a handful more than TOP_N_SERIES (see
 * OTHER_BUCKET_SLACK), in which case it's fine to just show them all instead
 * of bucketing a small remainder behind an "Other" that's no simpler than
 * showing it directly.
 * `seriesKey` picks the pivot dimension out of each row — a topic label when
 * charting one entity's topic mix, or a team/driver name when charting one
 * topic's spread across the field. */
function buildStackedSeries(
  rows: Record<string, unknown>[],
  seriesKey: (row: Record<string, unknown>) => string,
) {
  const totalsBySeries = new Map<string, number>();
  for (const r of rows) {
    const key = seriesKey(r);
    totalsBySeries.set(key, (totalsBySeries.get(key) ?? 0) + N(r.message_count));
  }
  const sortedSeries = [...totalsBySeries.entries()].sort((a, b) => b[1] - a[1]);
  const showAllSeries = sortedSeries.length <= TOP_N_SERIES + OTHER_BUCKET_SLACK;
  const topSeries = (showAllSeries ? sortedSeries : sortedSeries.slice(0, TOP_N_SERIES)).map(
    ([key]) => key,
  );
  const topSeriesSet = new Set(topSeries);
  const hasOther = !showAllSeries;

  const byRace = new Map<number, Record<string, unknown>>();
  for (const r of rows) {
    const sessionKey = N(r.session_key);
    if (!byRace.has(sessionKey)) {
      byRace.set(sessionKey, { session_key: sessionKey, year: N(r.year), race: chartRaceLabel(r) });
    }
    const row = byRace.get(sessionKey)!;
    const key = seriesKey(r);
    const bucket = topSeriesSet.has(key) ? key : "Other";
    row[bucket] = N(row[bucket]) + N(r.message_count);
  }

  const chartData = [...byRace.values()].sort((a, b) => N(a.session_key) - N(b.session_key));
  const series = hasOther ? [...topSeries, "Other"] : topSeries;
  return { chartData, series };
}

/** Splits sorted chartData into one array per season (by the `year` every
 * row carries), oldest season first — used to render one mini chart per
 * season instead of a single chart with every season's races crammed onto
 * one x-axis. */
function groupBySeasonYear(
  chartData: Record<string, unknown>[],
): { year: number; data: Record<string, unknown>[] }[] {
  const byYear = new Map<number, Record<string, unknown>[]>();
  for (const row of chartData) {
    const year = N(row.year);
    if (!byYear.has(year)) byYear.set(year, []);
    byYear.get(year)!.push(row);
  }
  return [...byYear.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([year, data]) => ({ year, data }));
}

/** Color-keyed legend shared across a season's stack of mini charts (or a
 * single chart), rendered once above the chart(s) rather than per-chart so
 * it doesn't repeat once "All" seasons splits into several charts. */
function SeriesLegend({ series }: { series: string[] }) {
  if (series.length <= 1) return null;
  return (
    <div className="flex flex-wrap gap-3 mb-3">
      {series.map((s, i) => (
        <div key={s} className="flex items-center gap-1 text-xs" style={{ color: "#6a6a6a" }}>
          <span
            style={{
              display: "inline-block",
              width: 10,
              height: 10,
              borderRadius: "50%",
              background: s === "Other" ? OTHER_COLOR : PALETTE[i % PALETTE.length],
            }}
          />
          {s}
        </div>
      ))}
    </div>
  );
}

function TopicLineChart({
  chartData,
  series,
  height = 340,
}: {
  chartData: Record<string, unknown>[];
  series: string[];
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={chartData}>
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
        {series.map((s, i) => (
          <Line
            key={s}
            type="monotone"
            dataKey={s}
            stroke={s === "Other" ? OTHER_COLOR : PALETTE[i % PALETTE.length]}
            strokeWidth={2}
            dot={{ r: 2 }}
            activeDot={{ r: 4 }}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/** Shared season-aware chart: a single line chart for one season, or — when
 * "All" seasons is selected — one smaller line chart per season stacked
 * vertically, so the x-axis never has to fit every race from every season
 * at once. */
function SeasonSeriesChart({
  season,
  chartData,
  series,
}: {
  season: Season;
  chartData: Record<string, unknown>[];
  series: string[];
}) {
  if (season !== "all") {
    return (
      <>
        <SeriesLegend series={series} />
        <TopicLineChart chartData={chartData} series={series} />
      </>
    );
  }

  const bySeason = groupBySeasonYear(chartData);
  return (
    <>
      <SeriesLegend series={series} />
      <div className="space-y-6">
        {bySeason.map(({ year, data }) => (
          <div key={year}>
            <h3 className="text-xs font-semibold mb-1" style={{ color: "#231f20" }}>
              {year}
            </h3>
            <TopicLineChart chartData={data} series={series} height={220} />
          </div>
        ))}
      </div>
    </>
  );
}

function TopicOverSeason({ season }: { season: Season }) {
  const topicsQ = useSQLQuery(
    `
      select topic_label, sum(message_count) as total_messages
      from ${FCT_DRIVER_TOPIC_RACE}
      ${whereClause(seasonFilter(season))}
      group by 1
      order by total_messages desc
    `,
    { enabled: season != null },
  );
  const topicOptions = Array.isArray(topicsQ.data) ? topicsQ.data : [];

  // Same known-values guard as `entity`/`session` elsewhere in this dive — a
  // `topic` left over from a previously-selected season (or a crafted link)
  // shouldn't be trusted just because it's non-null.
  const [topic, setTopic] = useDiveState<string | null>("topic", null);
  const knownTopics = new Set(topicOptions.map((row) => String(row.topic_label)));
  const effectiveTopic =
    topic != null && knownTopics.has(topic)
      ? topic
      : topicOptions.length
        ? String(topicOptions[0].topic_label)
        : null;

  const [breakdown, setBreakdown] = useDiveState<"team" | "driver" | "total">(
    "topicBreakdown",
    "team",
  );

  const rowsQ = useSQLQuery(
    `
      select
        session_key,
        year,
        circuit_short_name,
        session_name,
        session_date,
        ${breakdown === "total" ? "'Messages'" : breakdown === "team" ? "team_name" : "driver_acronym"} as entity,
        sum(message_count) as message_count
      from ${FCT_DRIVER_TOPIC_RACE}
      ${whereClause(seasonFilter(season), `topic_label = '${effectiveTopic}'`)}
      group by 1, 2, 3, 4, 5, 6
      order by session_date
    `,
    { enabled: season != null && effectiveTopic != null },
  );
  const rows = Array.isArray(rowsQ.data) ? rowsQ.data : [];

  const { chartData, series } = useMemo(
    () => buildStackedSeries(rows, (r) => String(r.entity)),
    [rows],
  );
  const totalMessages = rows.reduce((sum, r) => sum + N(r.message_count), 0);
  const raceCount = new Set(rows.map((r) => N(r.session_key))).size;

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        {topicsQ.isLoading ? (
          <div className="h-8 w-64 bg-gray-200 animate-pulse rounded" />
        ) : (
          <select
            className="text-sm border rounded px-2 py-1"
            style={{ color: "#231f20", borderColor: "#ddd" }}
            value={effectiveTopic ?? ""}
            onChange={(e) => setTopic(e.target.value)}
          >
            {topicOptions.map((row) => (
              <option key={String(row.topic_label)} value={String(row.topic_label)}>
                {String(row.topic_label)}
              </option>
            ))}
          </select>
        )}

        <div className="flex gap-2">
          {(
            [
              ["team", "By team"],
              ["driver", "By driver"],
              ["total", "Total"],
            ] as const
          ).map(([b, label]) => (
            <button
              key={b}
              onClick={() => setBreakdown(b)}
              className="text-xs px-2 py-1 rounded"
              style={{
                background: breakdown === b ? "#231f20" : "transparent",
                color: breakdown === b ? "#fff" : "#6a6a6a",
                border: `1px solid ${breakdown === b ? "#231f20" : "#ddd"}`,
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {rowsQ.isLoading ? (
        <div className="flex items-center gap-2" style={{ color: "#6a6a6a", height: 320 }}>
          <Loader2 className="animate-spin" size={16} /> Loading topic history…
        </div>
      ) : rowsQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {rowsQ.error?.message}</p>
      ) : chartData.length === 0 ? (
        <p style={{ color: "#6a6a6a" }}>No radio messages found for this topic.</p>
      ) : (
        <>
          <p className="text-sm mb-2" style={{ color: "#6a6a6a" }}>
            {totalMessages} messages across {raceCount} races
          </p>
          <SeasonSeriesChart season={season} chartData={chartData} series={series} />
        </>
      )}
    </div>
  );
}

// Sessions run in this order across a weekend when more than one is present
// (a normal weekend has just Qualifying + Race; a sprint weekend inserts
// Sprint between them). Anything else (e.g. practice sessions, if ever
// included) falls back to sorting by its own earliest message.
const SESSION_ORDER = ["Qualifying", "Sprint", "Race"];
const TIMELINE_BUCKETS_PER_SESSION = 20;

/** Buckets a weekend's raw (session_name, message_date) rows into fixed-size
 * time buckets per session, then concatenates the sessions in running order
 * so the whole weekend reads as one continuous timeline — each session
 * rescaled to the same bucket count regardless of its real duration, so a
 * short Sprint isn't squeezed into a sliver next to a long Race. */
function buildWeekendTimeline(rows: Record<string, unknown>[]) {
  const bySession = new Map<string, number[]>();
  for (const r of rows) {
    const name = String(r.session_name);
    const t = new Date(String(r.message_date)).getTime();
    if (Number.isNaN(t)) continue;
    if (!bySession.has(name)) bySession.set(name, []);
    bySession.get(name)!.push(t);
  }

  const sessionNames = [...bySession.keys()].sort((a, b) => {
    const ai = SESSION_ORDER.indexOf(a);
    const bi = SESSION_ORDER.indexOf(b);
    if (ai !== -1 && bi !== -1) return ai - bi;
    if (ai !== -1) return -1;
    if (bi !== -1) return 1;
    return Math.min(...bySession.get(a)!) - Math.min(...bySession.get(b)!);
  });

  const points: { index: number; count: number; session_name: string }[] = [];
  const segments: { session_name: string; start: number; end: number }[] = [];
  let cursor = 0;
  for (const name of sessionNames) {
    const times = bySession.get(name)!.sort((a, b) => a - b);
    const min = times[0];
    const span = Math.max(times[times.length - 1] - min, 1);
    const counts = new Array(TIMELINE_BUCKETS_PER_SESSION).fill(0);
    for (const t of times) {
      const bucket = Math.min(
        TIMELINE_BUCKETS_PER_SESSION - 1,
        Math.floor(((t - min) / span) * TIMELINE_BUCKETS_PER_SESSION),
      );
      counts[bucket]++;
    }
    const start = cursor;
    for (const count of counts) {
      points.push({ index: cursor, count, session_name: name });
      cursor++;
    }
    segments.push({ session_name: name, start, end: cursor - 1 });
  }
  return { points, segments };
}

function WeekendTimelineTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: { session_name: string; count: number } }[];
}) {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div
      style={{
        background: "#fff",
        border: "1px solid #ddd",
        borderRadius: 4,
        padding: "4px 8px",
        fontSize: 11,
      }}
    >
      <div style={{ fontWeight: 600, color: "#231f20" }}>{p.session_name}</div>
      <div style={{ color: "#6a6a6a" }}>{p.count} messages</div>
    </div>
  );
}

/** Message-volume evolution across an entire race weekend (Qualifying,
 * Sprint if present, and Race back to back), so bursts of radio chatter —
 * race starts, safety cars, pit windows — are visible against which session
 * they happened in, rather than only within one session at a time. */
function WeekendRadioTimeline({ circuit, year }: { circuit: string; year: number }) {
  const rowsQ = useSQLQuery(`
    select session_name, message_date
    from ${FCT_RADIO_MESSAGES}
    where circuit_short_name = '${circuit}' and year = ${year}
    order by message_date
  `);
  const rows = Array.isArray(rowsQ.data) ? rowsQ.data : [];
  const { points, segments } = useMemo(() => buildWeekendTimeline(rows), [rows]);

  return (
    <div className="mb-6">
      <h2 className="text-sm font-semibold mb-2" style={{ color: "#231f20" }}>
        Radio traffic over the weekend
      </h2>
      {rowsQ.isLoading ? (
        <div className="flex items-center gap-2" style={{ color: "#6a6a6a", height: 160 }}>
          <Loader2 className="animate-spin" size={16} /> Loading weekend timeline…
        </div>
      ) : rowsQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {rowsQ.error?.message}</p>
      ) : points.length === 0 ? (
        <p style={{ color: "#6a6a6a" }}>No radio messages found for this weekend.</p>
      ) : (
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={points}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis dataKey="index" tick={false} axisLine={false} tickLine={false} />
            <YAxis fontSize={11} width={30} allowDecimals={false} />
            <Tooltip content={<WeekendTimelineTooltip />} />
            {segments.map((seg, i) => (
              <ReferenceArea
                key={seg.session_name}
                x1={seg.start}
                x2={seg.end}
                strokeOpacity={0}
                fill={i % 2 === 0 ? "#0777b3" : "#bd4e35"}
                fillOpacity={0.06}
                label={{ value: seg.session_name, position: "insideTop", fontSize: 11, fill: "#6a6a6a" }}
              />
            ))}
            <Line type="monotone" dataKey="count" stroke="#0777b3" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

function RaceTopicsDetail({ season }: { season: Season }) {
  const sessionsQ = useSQLQuery(
    `
      select distinct session_key, year, circuit_short_name, session_name, session_date
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
  const effectiveSession = sessions.find((s) => N(s.session_key) === effectiveSessionKey);

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

      {effectiveSession != null && (
        <WeekendRadioTimeline
          circuit={String(effectiveSession.circuit_short_name)}
          year={N(effectiveSession.year)}
        />
      )}

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
