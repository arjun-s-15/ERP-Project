import { useEffect, useState } from "react";
import axios from "axios";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";
import "../styles/sales_analytics.css";

const SalesDashboard = () => {
  const [totalSales, setTotalSales] = useState(() => {
    const saved = localStorage.getItem("dashboard_totalSales");
    return saved ? JSON.parse(saved) : [];
  });
  const [monthlySales, setMonthlySales] = useState(() => {
    const saved = localStorage.getItem("dashboard_monthlySales");
    return saved ? JSON.parse(saved) : [];
  });
  const [dowSales, setDowSales] = useState(() => {
    const saved = localStorage.getItem("dashboard_dowSales");
    return saved ? JSON.parse(saved) : [];
  });
  const [storeForecast, setStoreForecast] = useState(() => {
    const saved = localStorage.getItem("dashboard_storeForecast");
    return saved ? JSON.parse(saved) : [];
  });
  const [totalForecast, setTotalForecast] = useState(() => {
    const saved = localStorage.getItem("dashboard_totalForecast");
    return saved ? JSON.parse(saved) : [];
  });
  const [dailyTotal, setDailyTotal] = useState(() => {
    const saved = localStorage.getItem("dashboard_dailyTotal");
    return saved ? JSON.parse(saved) : [];
  });
  const [storeDaily, setStoreDaily] = useState(() => {
    const saved = localStorage.getItem("dashboard_storeDaily");
    return saved ? JSON.parse(saved) : [];
  });
  const [loading, setLoading] = useState(() => {
    return !localStorage.getItem("dashboard_totalSales");
  });

  const fetchAll = async () => {
    try {
      const [
        totalRes,
        monthlyRes,
        dowRes,
        storeForecastRes,
        totalForecastRes,
        dailyTotalRes,
        storeDailyRes,
      ] = await Promise.all([
        axios.get("http://localhost:8001/analytics/total-store-sales"),
        axios.get("http://localhost:8001/analytics/monthly-store-sales"),
        axios.get("http://localhost:8001/analytics/day-of-week-sales"),
        axios.get("http://localhost:8001/predictions/sales-forecast/store"),
        axios.get("http://localhost:8001/predictions/sales-forecast/total"),
        axios.get(
          "http://localhost:8001/analytics/historical/total-daily-sales",
        ),
        axios.get(
          "http://localhost:8001/analytics/historical/store_daily_sales",
        ),
      ]);

      setTotalSales(totalRes.data);
      setMonthlySales(monthlyRes.data);
      setDowSales(dowRes.data);
      setDailyTotal(dailyTotalRes.data);
      setStoreDaily(storeDailyRes.data);
      setStoreForecast(storeForecastRes.data);
      setTotalForecast(totalForecastRes.data);

      localStorage.setItem(
        "dashboard_totalSales",
        JSON.stringify(totalRes.data),
      );
      localStorage.setItem(
        "dashboard_monthlySales",
        JSON.stringify(monthlyRes.data),
      );
      localStorage.setItem("dashboard_dowSales", JSON.stringify(dowRes.data));
      localStorage.setItem(
        "dashboard_dailyTotal",
        JSON.stringify(dailyTotalRes.data),
      );
      localStorage.setItem(
        "dashboard_storeDaily",
        JSON.stringify(storeDailyRes.data),
      );
      localStorage.setItem(
        "dashboard_storeForecast",
        JSON.stringify(storeForecastRes.data),
      );
      localStorage.setItem(
        "dashboard_totalForecast",
        JSON.stringify(totalForecastRes.data),
      );
    } catch (err) {
      console.error("Analytics error:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (localStorage.getItem("dashboard_totalSales")) {
      setLoading(false);
      return;
    }
    fetchAll();
  }, []);

  if (loading) return <p>Loading dashboard...</p>;

  const storeIdsDaily = [...new Set(storeDaily.map((d) => d.location_id))];
  const storeDailyFormatted = Object.values(
    storeDaily.reduce((acc, item) => {
      const date = item.datetime;
      if (!acc[date]) {
        acc[date] = { datetime: date }; // ← was { date }, must match XAxis dataKey
      }
      acc[date][`store_${item.location_id}`] = item.quantity;
      return acc;
    }, {}),
  );

  // Keep only the latest model_version per location_id
  const latestPerStore = Object.values(
    storeForecast.reduce((acc, row) => {
      const key = row.location_id;
      if (!acc[key] || row.model_version > acc[key].model_version) {
        acc[key] = row;
      }
      return acc;
    }, {}),
  ).sort((a, b) => a.location_id - b.location_id);

  const totalFc = totalForecast[0];

  // 🧠 Transform monthly data → grouped by month
  const monthlyFormatted = Object.values(
    monthlySales.reduce((acc, item) => {
      if (!acc[item.month_year]) {
        acc[item.month_year] = { month: item.month_year };
      }
      acc[item.month_year][`store_${item.location_id}`] = item.quantity;
      return acc;
    }, {}),
  );

  // 🧠 Transform DOW data
  const dowFormatted = Object.values(
    dowSales.reduce((acc, item) => {
      if (!acc[item.day_name]) {
        acc[item.day_name] = { day: item.day_name };
      }
      acc[item.day_name][`store_${item.location_id}`] = item.quantity;
      return acc;
    }, {}),
  );

  const storeIds = [...new Set(monthlySales.map((d) => d.location_id))];

  const handleRefresh = async () => {
    [
      "totalSales",
      "monthlySales",
      "dowSales",
      "dailyTotal",
      "storeDaily",
      "storeForecast",
      "totalForecast",
    ].forEach((key) => localStorage.removeItem(`dashboard_${key}`));
    setLoading(true);
    await fetchAll();
  };

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <button className="btn-refresh" onClick={handleRefresh}>
          Refresh Data
        </button>
      </div>

      {/* ── Section: Trends ── */}
      <div className="section">
        <h2 className="section-title">Sales Trends</h2>

        <div className="grid-2">
          {/* Daily Total */}
          <div className="card">
            <div className="card-title">Daily Total Sales</div>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dailyTotal}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="datetime"
                  label={{
                    value: "Date",
                    position: "insideBottom",
                    offset: -5,
                  }}
                />
                <YAxis
                  label={{ value: "Sales", angle: -90, position: "insideLeft" }}
                />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="total_sales"
                  stroke="#2563EB"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Store Trend */}
          <div className="card">
            <div className="card-title">Store-wise Daily Sales</div>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={storeDailyFormatted}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="datetime"
                  label={{
                    value: "Date",
                    position: "insideBottom",
                    offset: -5,
                  }}
                />
                <YAxis
                  label={{ value: "Sales", angle: -90, position: "insideLeft" }}
                />
                <Tooltip />
                <Legend />
                {storeIdsDaily.map((id, index) => (
                  <Line
                    key={id}
                    type="monotone"
                    dataKey={`store_${id}`}
                    name={`Store ${id}`}
                    stroke={
                      ["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]
                    }
                    dot={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* ── Section: Forecast ── */}
      <div className="section">
        <h2 className="section-title">Forecast</h2>

        <div className="stats-grid">
          {/* ── Total Forecast ── */}
          {totalFc && (
            <div className="stat-card blue">
              <div className="stat-label">Total (Next Day)</div>

              <div className="stat-value">
                {totalFc.predicted_sales.toFixed(0)}
              </div>

              <div className="card-subtitle">
                {totalFc.prediction_date.slice(0, 10)}
              </div>

              <div className="model-meta">
                {totalFc.model_name} · v{totalFc.model_version}
              </div>
            </div>
          )}

          {/* ── Store Forecasts ── */}
          {latestPerStore.map((row) => (
            <div key={row.location_id} className="stat-card">
              <div className="stat-label">Store {row.location_id}</div>

              <div className="stat-value">{row.predicted_sales.toFixed(0)}</div>

              <div className="card-subtitle">
                {row.prediction_date.slice(0, 10)}
              </div>

              <div className="model-meta">
                {row.model_name} · v{row.model_version}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Section: Breakdown ── */}
      <div className="section">
        <h2 className="section-title">Breakdowns</h2>

        <div className="grid-2">
          {/* Total per store */}
          <div className="card">
            <div className="card-title">Total Sales per Store</div>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={totalSales}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="location_id"
                  label={{
                    value: "Store",
                    position: "insideBottom",
                    offset: -5,
                  }}
                />
                <YAxis
                  label={{ value: "Sales", angle: -90, position: "insideLeft" }}
                />
                <Tooltip />
                <Bar dataKey="quantity" fill="#2563EB" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Day of week */}
          <div className="card">
            <div className="card-title">Sales by Day of Week</div>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={dowFormatted}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="day"
                  label={{ value: "Day", position: "insideBottom", offset: -5 }}
                />
                <YAxis
                  label={{ value: "Sales", angle: -90, position: "insideLeft" }}
                />
                <Tooltip />
                <Legend />
                {storeIds.map((id, index) => (
                  <Bar
                    key={id}
                    dataKey={`store_${id}`}
                    name={`Store ${id}`}
                    fill={
                      ["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]
                    }
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Monthly full width */}
        <div className="card">
          <div className="card-title">Monthly Sales Trend</div>
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={monthlyFormatted}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="month"
                label={{ value: "Month", position: "insideBottom", offset: -5 }}
              />
              <YAxis
                label={{ value: "Sales", angle: -90, position: "insideLeft" }}
              />
              <Tooltip />
              <Legend />
              {storeIds.map((id, index) => (
                <Line
                  key={id}
                  type="monotone"
                  dataKey={`store_${id}`}
                  name={`Store ${id}`}
                  stroke={
                    ["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]
                  }
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default SalesDashboard;
