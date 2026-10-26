import React, { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Line, ComposedChart, Legend, PieChart, Pie, Cell } from "recharts";

const ACC = "#f0b13f", GREY = "#8fa3ad", OK = "#4fd1b0";
const PIE = ["#f0b13f", "#4fd1b0", "#8fa3ad", "#e07a5f", "#7b8cde"];
const get = (p) => fetch(p).then((r) => r.json());
const fmt = (n) => Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });

function useApi(path, deps = []) {
  const [d, setD] = useState(null);
  useEffect(() => { get(path).then(setD).catch(() => setD(null)); }, deps); // eslint-disable-line
  return d;
}
const Card = ({ title, hint, children }) => (
  <div className="card"><h3>{title}</h3><div className="hint">{hint}</div>{children}</div>
);
const Kpi = ({ l, v }) => (<div className="card kpi"><div className="v">{v}</div><div className="l">{l}</div></div>);
const Chart = ({ children }) => (<ResponsiveContainer width="100%" height={260}>{children}</ResponsiveContainer>);

function Overview({ live }) {
  const recent = useApi("/api/recent", [live?.total_bookings]);
  const hotels = useApi("/api/hotels", [live?.total_bookings > 0]);
  return (<>
    <div className="grid g4">
      <Kpi l="Bookings (Redis)" v={fmt(live?.total_bookings)} />
      <Kpi l="Cancellations" v={fmt(live?.cancellations)} />
      <Kpi l="Cancellation rate" v={`${live?.cancellation_rate ?? 0}%`} />
      <Kpi l="Revenue, net of cancellations" v={fmt(live?.revenue)} />
    </div>
    <div className="grid g2" style={{ marginTop: 14 }}>
      <Card title="Hotels" hint="Aggregated in MongoDB">
        <table><thead><tr><th>Hotel</th><th>Bookings</th><th>Cancel %</th><th>ADR</th><th>Nights</th></tr></thead>
          <tbody>{(hotels || []).map((h) => (<tr key={h.hotel}><td>{h.hotel}</td><td>{fmt(h.bookings)}</td><td>{h.cancel_rate}</td><td>{h.adr}</td><td>{h.nights}</td></tr>))}</tbody></table>
      </Card>
      <Card title="Latest bookings" hint="Recent-list in Redis">
        <table><thead><tr><th>Hotel</th><th>Country</th><th>Segment</th><th>ADR</th></tr></thead>
          <tbody>{(recent || []).slice(0, 8).map((b) => (<tr key={b.booking_id}><td>{b.hotel.split(" ")[0]}</td><td>{b.country}</td><td>{b.segment}</td><td>{b.adr}</td></tr>))}</tbody></table>
      </Card>
    </div>
  </>);
}

function Cancellations() {
  const lead = useApi("/api/cancellation/leadtime");
  const seg = useApi("/api/cancellation/segment");
  return (<div className="grid g2">
    <Card title="Cancellation rate by lead time" hint="MongoDB $bucket aggregation">
      <Chart><BarChart data={lead || []}><CartesianGrid stroke="#243640" /><XAxis dataKey="bucket" stroke={GREY} /><YAxis stroke={GREY} unit="%" /><Tooltip /><Bar dataKey="cancel_rate" fill={ACC} name="Cancel %" /></BarChart></Chart>
    </Card>
    <Card title="Cancellation rate by market segment" hint="MongoDB $group aggregation">
      <Chart><BarChart data={seg || []} layout="vertical"><CartesianGrid stroke="#243640" /><XAxis type="number" stroke={GREY} unit="%" /><YAxis type="category" dataKey="segment" stroke={GREY} width={95} /><Tooltip /><Bar dataKey="cancel_rate" fill={ACC} name="Cancel %" /></BarChart></Chart>
    </Card>
  </div>);
}

function Seasonality() {
  const m = useApi("/api/monthly");
  return (<Card title="Bookings and ADR by arrival month" hint="Cassandra: one partition per hotel and month">
    <ResponsiveContainer width="100%" height={340}>
      <ComposedChart data={m || []}><CartesianGrid stroke="#243640" /><XAxis dataKey="month" stroke={GREY} tick={{ fontSize: 10 }} /><YAxis yAxisId="l" stroke={GREY} /><YAxis yAxisId="r" orientation="right" stroke={GREY} /><Tooltip /><Legend />
        <Bar yAxisId="l" dataKey="city_bookings" name="City bookings" fill={GREY} /><Bar yAxisId="l" dataKey="resort_bookings" name="Resort bookings" fill={OK} />
        <Line yAxisId="r" dataKey="city_adr" name="City ADR" stroke={ACC} dot={false} /><Line yAxisId="r" dataKey="resort_adr" name="Resort ADR" stroke="#e07a5f" dot={false} /></ComposedChart>
    </ResponsiveContainer></Card>);
}

function Preferences() {
  const countries = useApi("/api/countries");
  const [c, setC] = useState("PRT");
  const pref = useApi(`/api/preferences?country=${c}`, [c]);
  const meals = useApi("/api/meals");
  return (<div className="grid g2">
    <Card title="What guests from a country choose" hint="Neo4j graph traversal">
      <select value={c} onChange={(e) => setC(e.target.value)}>{(countries || ["PRT"]).map((x) => <option key={x}>{x}</option>)}</select>
      <Chart><BarChart data={pref?.rooms || []}><CartesianGrid stroke="#243640" /><XAxis dataKey="room" stroke={GREY} /><YAxis stroke={GREY} /><Tooltip /><Bar dataKey="bookings" fill={ACC} name="Bookings by room type" /></BarChart></Chart>
    </Card>
    <Card title="Meal plan share" hint="MongoDB $group aggregation">
      <Chart><PieChart><Pie data={meals || []} dataKey="bookings" nameKey="meal" outerRadius={90} label>{(meals || []).map((_, i) => <Cell key={i} fill={PIE[i % PIE.length]} />)}</Pie><Tooltip /><Legend /></PieChart></Chart>
    </Card>
  </div>);
}

function System() {
  const h = useApi("/api/health");
  const desc = { mongodb: "Bookings", cassandra: "Event log", redis: "Live counters", neo4j: "Preference graph" };
  return (<div className="grid g4">{Object.keys(desc).map((k) => (
    <div className="card kpi" key={k}><div className="l">{k}</div><div className="v" style={{ color: h?.databases?.[k] === "up" ? OK : "#e07a5f" }}>{h?.databases?.[k] ?? "..."}</div><div className="l">{desc[k]}</div></div>))}</div>);
}

export default function App() {
  const [tab, setTab] = useState("Overview");
  const [live, setLive] = useState(null);
  const [on, setOn] = useState(false);
  useEffect(() => {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => setOn(true); ws.onclose = () => setOn(false);
    ws.onmessage = (e) => setLive(JSON.parse(e.data));
    return () => ws.close();
  }, []);
  const tabs = { Overview: <Overview live={live} />, Cancellations: <Cancellations />, Seasonality: <Seasonality />, Preferences: <Preferences />, System: <System /> };
  return (<div className="wrap">
    <div className="top"><div><h1>Hotel Booking Analytics</h1><div className="sub">Real-time analytics across four NoSQL databases</div></div>
      <span className={`pill ${on ? "on" : ""}`}>{on ? "Live updates on" : "Disconnected"}</span></div>
    <div className="tabs">{Object.keys(tabs).map((t) => <button key={t} className={t === tab ? "act" : ""} onClick={() => setTab(t)}>{t}</button>)}</div>
    {tabs[tab]}
  </div>);
}
