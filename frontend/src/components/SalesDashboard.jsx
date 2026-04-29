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
import "../styles/dashboard.css";

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
    <div>
      <div className="card">
        <div className="card-title">Daily Total Sales Trend</div>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={dailyTotal}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="datetime" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="total_sales" stroke="#2563EB" />
          </LineChart>
        </ResponsiveContainer>
      </div>
      {/* ── Total Sales Forecast ── */}
      {totalFc && (
        <div className="card">
          <div className="card-title">Total Sales Forecast</div>
          <p>Prediction Date: {totalFc.prediction_date.slice(0, 10)}</p>
          <p>Predicted Sales: {totalFc.predicted_sales.toFixed(2)}</p>
          <p>
            Model: {totalFc.model_name} (v{totalFc.model_version})
          </p>
        </div>
      )}

      <div className="card">
        <div className="card-title">Store-wise Daily Sales Trend</div>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={storeDailyFormatted}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="datetime" />
            <YAxis />
            <Tooltip />
            <Legend />
            {storeIdsDaily.map((id, index) => (
              <Line
                key={id}
                type="monotone"
                dataKey={`store_${id}`}
                stroke={["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ── Store Sales Forecast ── */}
      <div className="card">
        <div className="card-title">Store Sales Forecast</div>
        {latestPerStore.map((row) => (
          <div key={row.location_id} style={{ marginBottom: "12px" }}>
            <p>
              Store {row.location_id} — Predicted Sales:{" "}
              {row.predicted_sales.toFixed(2)}
            </p>
            <p>
              Prediction Date: {row.prediction_date.slice(0, 10)} · Model v
              {row.model_version}
            </p>
          </div>
        ))}
      </div>

      {/* ── Total Sales (Bar) ── */}
      <div className="card">
        <div className="card-title">Total Sales per Store</div>

        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={totalSales}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="location_id" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="quantity" fill="#2563EB" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* ── Monthly Sales (Line) ── */}
      <div className="card">
        <div className="card-title">Monthly Sales Trend</div>

        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={monthlyFormatted}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" />
            <YAxis />
            <Tooltip />
            <Legend />
            {storeIds.map((id, index) => (
              <Line
                key={id}
                type="monotone"
                dataKey={`store_${id}`}
                stroke={["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ── Day of Week Sales (Bar) ── */}
      <div className="card">
        <div className="card-title">Day-wise Sales</div>

        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={dowFormatted}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="day" />
            <YAxis />
            <Tooltip />
            <Legend />
            {storeIds.map((id, index) => (
              <Bar
                key={id}
                dataKey={`store_${id}`}
                name={`store_${id}`}
                fill={["#2563EB", "#F59E0B", "#22C55E", "#EF4444"][index % 4]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default SalesDashboard;
