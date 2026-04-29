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
  const [totalSales, setTotalSales] = useState([]);
  const [monthlySales, setMonthlySales] = useState([]);
  const [dowSales, setDowSales] = useState([]);
  const [storeForecast, setStoreForecast] = useState([]);
  const [totalForecast, setTotalForecast] = useState([]);
  const [dailyTotal, setDailyTotal] = useState([]);
  const [storeDaily, setStoreDaily] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
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
      } catch (err) {
        console.error("Analytics error:", err);
      } finally {
        setLoading(false);
      }
    };

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

  return (
    <div className="dashboard">
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
          {totalFc && (
            <div className="stat-card blue">
              <div className="stat-label">Total (Next Day)</div>
              <div className="stat-value">
                {totalFc.predicted_sales.toFixed(0)}
              </div>
              <div className="card-subtitle">
                {totalFc.prediction_date.slice(0, 10)}
              </div>
            </div>
          )}

          {latestPerStore.map((row) => (
            <div key={row.location_id} className="stat-card">
              <div className="stat-label">Store {row.location_id}</div>
              <div className="stat-value">{row.predicted_sales.toFixed(0)}</div>
              <div className="card-subtitle">v{row.model_version}</div>
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
