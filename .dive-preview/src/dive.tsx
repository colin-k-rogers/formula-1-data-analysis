// Kept byte-for-byte identical to dives/season_pace_dive.tsx (the deployed
// dive source) — mirror any change there too.
import { useEffect, useRef, useState } from "react";
import { useSQLQuery, useDiveState } from "@motherduck/react-sql-query";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { Loader2 } from "lucide-react";

const N = (v: unknown): number => (v != null ? Number(v) : 0);

function formatLapTime(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = (seconds - mins * 60).toFixed(3);
  return `${mins}:${secs.padStart(6, "0")}`;
}

const FCT_LAP_PACE = '"f1"."marts"."fct_lap_pace"';

const PALETTE = ["#0777b3", "#bd4e35", "#2d7a00", "#e18727", "#638CAD", "#adadad"];

export default function SeasonPaceDive() {
  const [view, setView] = useDiveState<"season" | "race">("view", "season");

  return (
    <div className="p-6" style={{ background: "#f8f8f8", maxWidth: 900, margin: "0 auto" }}>
      <h1 className="text-2xl font-semibold" style={{ color: "#231f20" }}>
        2025 Relative Lap Pace
      </h1>
      <p className="text-sm mb-6" style={{ color: "#6a6a6a" }}>
        Each driver's lap time compared with the fastest lap turned by the field on that
        same lap, across all 2025 Race sessions.
      </p>

      <div className="flex gap-2 mb-6">
        <button
          onClick={() => setView("season")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: view !== "race" ? "#0777b3" : "transparent",
            color: view !== "race" ? "#fff" : "#6a6a6a",
          }}
        >
          Season overview
        </button>
        <button
          onClick={() => setView("race")}
          className="text-sm px-3 py-1 rounded"
          style={{
            background: view === "race" ? "#0777b3" : "transparent",
            color: view === "race" ? "#fff" : "#6a6a6a",
          }}
        >
          Race detail
        </button>
      </div>

      {view === "race" ? <RaceDetail /> : <SeasonOverview />}
    </div>
  );
}

function SeasonOverview() {
  const q = useSQLQuery(`
    select
      driver_acronym,
      team_name,
      avg(delta_to_fastest) as avg_delta,
      count(distinct session_key) as races
    from ${FCT_LAP_PACE}
    group by 1, 2
    order by avg_delta asc
  `);

  const rows = Array.isArray(q.data) ? q.data : [];

  return (
    <div>
      {q.isLoading ? (
        <div className="animate-pulse space-y-2">
          <div className="h-4 bg-gray-200 rounded w-3/4" />
          <div className="h-4 bg-gray-200 rounded w-1/2" />
          <div className="h-4 bg-gray-200 rounded w-2/3" />
        </div>
      ) : q.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {q.error?.message}</p>
      ) : (
        <table className="w-full text-sm" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #ddd", color: "#6a6a6a", textAlign: "left" }}>
              <th className="py-2">#</th>
              <th className="py-2">Driver</th>
              <th className="py-2">Team</th>
              <th className="py-2 text-right">Avg gap to fastest lap</th>
              <th className="py-2 text-right">Races</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={String(r.driver_acronym)} style={{ borderBottom: "1px solid #eee" }}>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{i + 1}</td>
                <td className="py-1" style={{ color: "#231f20", fontWeight: 600 }}>
                  {String(r.driver_acronym)}
                </td>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{String(r.team_name)}</td>
                <td className="py-1 text-right" style={{ color: "#231f20" }}>
                  +{N(r.avg_delta).toFixed(3)}s
                </td>
                <td className="py-1 text-right" style={{ color: "#6a6a6a" }}>{N(r.races)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function RaceDetail() {
  const sessionsQ = useSQLQuery(`
    select distinct
      session_key,
      meeting_official_name,
      circuit_short_name,
      strftime(session_date AT TIME ZONE 'UTC', '%Y-%m-%d') as session_date
    from ${FCT_LAP_PACE}
    order by session_date
  `);
  const sessions = Array.isArray(sessionsQ.data) ? sessionsQ.data : [];

  const [sessionKey, setSessionKey] = useDiveState<number | null>("session", null);
  const effectiveSessionKey =
    sessionKey ?? (sessions.length ? N(sessions[sessions.length - 1].session_key) : null);

  const driversQ = useSQLQuery(
    `
      select
        driver_acronym,
        team_colour,
        avg(delta_to_fastest) as avg_delta
      from ${FCT_LAP_PACE}
      where session_key = ${effectiveSessionKey}
      group by 1, 2
      order by avg_delta asc
    `,
    { enabled: effectiveSessionKey != null },
  );
  const raceDrivers = Array.isArray(driversQ.data) ? driversQ.data : [];

  const [selectedDrivers, setSelectedDrivers] = useDiveState<string[]>("drivers", []);

  // driversQ.data keeps showing the *previous* session's rows for a render or
  // two after effectiveSessionKey changes (the query hasn't re-fetched yet),
  // so gate on the data array reference actually changing rather than on
  // effectiveSessionKey/length alone — otherwise this reseeds from stale data
  // and then never corrects itself once the real rows for the new session
  // arrive with the same driver count.
  const seededSessionKeyRef = useRef<number | null>(null);
  const prevDriversDataRef = useRef(driversQ.data);
  useEffect(() => {
    const dataChanged = driversQ.data !== prevDriversDataRef.current;
    prevDriversDataRef.current = driversQ.data;
    if (
      dataChanged &&
      driversQ.isSuccess &&
      raceDrivers.length > 0 &&
      seededSessionKeyRef.current !== effectiveSessionKey
    ) {
      seededSessionKeyRef.current = effectiveSessionKey;
      setSelectedDrivers(raceDrivers.slice(0, 5).map((d) => String(d.driver_acronym)));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [driversQ.data, driversQ.isSuccess, effectiveSessionKey]);

  const lapsQ = useSQLQuery(
    `
      select lap_number, driver_acronym, delta_to_fastest
      from ${FCT_LAP_PACE}
      where session_key = ${effectiveSessionKey}
      order by lap_number
    `,
    { enabled: effectiveSessionKey != null },
  );
  const lapRows = Array.isArray(lapsQ.data) ? lapsQ.data : [];

  const summaryQ = useSQLQuery(
    `
      select
        driver_acronym,
        team_name,
        avg(lap_duration) as avg_lap_time,
        sum(delta_to_fastest) as cumulative_gap
      from ${FCT_LAP_PACE}
      where session_key = ${effectiveSessionKey}
      group by 1, 2
      order by cumulative_gap asc
    `,
    { enabled: effectiveSessionKey != null },
  );
  const summaryRows = Array.isArray(summaryQ.data) ? summaryQ.data : [];

  const chartData: Record<string, number>[] = [];
  const chartByLap = new Map<number, Record<string, number>>();
  for (const r of lapRows) {
    const acronym = String(r.driver_acronym);
    if (!selectedDrivers.includes(acronym)) continue;
    const lap = N(r.lap_number);
    if (!chartByLap.has(lap)) {
      const row = { lap_number: lap };
      chartByLap.set(lap, row);
      chartData.push(row);
    }
    chartByLap.get(lap)![acronym] = N(r.delta_to_fastest);
  }
  chartData.sort((a, b) => a.lap_number - b.lap_number);

  function toggleDriver(acronym: string) {
    setSelectedDrivers(
      selectedDrivers.includes(acronym)
        ? selectedDrivers.filter((d) => d !== acronym)
        : [...selectedDrivers, acronym],
    );
  }

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
                {String(s.circuit_short_name)} — {String(s.session_date)}
              </option>
            ))}
          </select>
        )}
      </div>

      <div className="mb-4 flex flex-wrap gap-2">
        {raceDrivers.map((d) => {
          const acronym = String(d.driver_acronym);
          const active = selectedDrivers.includes(acronym);
          return (
            <button
              key={acronym}
              onClick={() => toggleDriver(acronym)}
              className="text-xs px-2 py-1 rounded"
              style={{
                background: active ? `#${String(d.team_colour)}` : "transparent",
                color: active ? "#fff" : "#6a6a6a",
                border: `1px solid ${active ? `#${String(d.team_colour)}` : "#ddd"}`,
              }}
            >
              {acronym}
            </button>
          );
        })}
      </div>

      {lapsQ.isLoading ? (
        <div className="flex items-center gap-2" style={{ color: "#6a6a6a", height: 280 }}>
          <Loader2 className="animate-spin" size={16} /> Loading laps…
        </div>
      ) : lapsQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {lapsQ.error?.message}</p>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis dataKey="lap_number" fontSize={11} label={{ value: "Lap", position: "insideBottom", offset: -2, fontSize: 11 }} />
            <YAxis
              fontSize={11}
              tickFormatter={(v) => `+${Number(v).toFixed(1)}s`}
              label={{ value: "Gap to fastest lap", angle: -90, position: "insideLeft", fontSize: 11 }}
            />
            <Tooltip formatter={(v: number) => `+${Number(v).toFixed(3)}s`} />
            {selectedDrivers.map((acronym, i) => (
              <Line
                key={acronym}
                type="linear"
                dataKey={acronym}
                stroke={PALETTE[i % PALETTE.length]}
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}

      <h2 className="text-sm font-semibold mt-6 mb-2" style={{ color: "#231f20" }}>
        Race pace summary
      </h2>
      {summaryQ.isLoading ? (
        <div className="animate-pulse space-y-2">
          <div className="h-4 bg-gray-200 rounded w-3/4" />
          <div className="h-4 bg-gray-200 rounded w-1/2" />
        </div>
      ) : summaryQ.isError ? (
        <p style={{ color: "#bc1200" }}>Failed to load: {summaryQ.error?.message}</p>
      ) : (
        <table className="w-full text-sm" style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #ddd", color: "#6a6a6a", textAlign: "left" }}>
              <th className="py-2">#</th>
              <th className="py-2">Driver</th>
              <th className="py-2">Team</th>
              <th className="py-2 text-right">Avg lap time</th>
              <th className="py-2 text-right">Cumulative gap to fastest</th>
            </tr>
          </thead>
          <tbody>
            {summaryRows.map((r, i) => (
              <tr key={String(r.driver_acronym)} style={{ borderBottom: "1px solid #eee" }}>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{i + 1}</td>
                <td className="py-1" style={{ color: "#231f20", fontWeight: 600 }}>
                  {String(r.driver_acronym)}
                </td>
                <td className="py-1" style={{ color: "#6a6a6a" }}>{String(r.team_name)}</td>
                <td className="py-1 text-right" style={{ color: "#231f20" }}>
                  {formatLapTime(N(r.avg_lap_time))}
                </td>
                <td className="py-1 text-right" style={{ color: "#231f20" }}>
                  +{N(r.cumulative_gap).toFixed(3)}s
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
