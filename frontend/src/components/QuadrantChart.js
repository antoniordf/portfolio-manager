import React, { useEffect, useState } from "react";
import {
  ComposedChart,
  Scatter,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

function QuadrantChart() {
  const [data, setData] = useState([]);
  const [lastDataPoint, setLastDataPoint] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch("/dashboard/quadrant_data/");
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const data = await response.json();
        const sortedData = data
          .map((item) => ({
            date: new Date(item.date),
            gdp_growth: item.gdp_growth,
            inflation_growth: item.inflation_growth,
          }))
          .sort((a, b) => a.date - b.date);
        setData(sortedData.slice(0, -1)); // All data except the last one
        setLastDataPoint(sortedData[sortedData.length - 1]); // The last data point
      } catch (error) {
        console.error("Error fetching data: ", error);
      }
    };
    fetchData();
  }, []);

  return (
    <ResponsiveContainer width="100%" height={600}>
      <ComposedChart
        data={data}
        margin={{ top: 20, right: 40, bottom: 20, left: 40 }}
      >
        {/* Cartesian Grid */}
        <CartesianGrid vertical={false} horizontal={false} />

        {/* X-Axis Configuration */}
        <XAxis
          type="number"
          dataKey="inflation_growth"
          name="Inflation Rate of Change (%)"
          domain={[-3, 3]}
          ticks={[-3, -2, -1, 0, 1, 2, 3]}
          label={{
            value: "Inflation Rate of Change (%)",
            position: "insideBottom",
            offset: -10,
          }}
        />

        {/* Y-Axis Configuration */}
        <YAxis
          type="number"
          dataKey="gdp_growth"
          name="GDP Rate of Change (%)"
          domain={[-10, 10]}
          ticks={[
            -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6,
            7, 8, 9, 10, 11,
          ]}
          label={{
            value: "GDP Rate of Change (%)",
            angle: -90,
            position: "insideLeft",
            offset: -10,
          }}
        />

        {/* Tooltip with Fixed labelFormatter */}
        <Tooltip
          cursor={{ strokeDasharray: "3 3" }}
          formatter={(value) => `${value.toFixed(2)}%`}
          labelFormatter={(label) => {
            const date = new Date(label);
            return isNaN(date) ? label : date.toLocaleDateString();
          }}
        />

        {/* Legend */}
        <Legend verticalAlign="top" height={36} />

        {/* Reference Lines at X=0 and Y=0 */}
        <ReferenceLine x={0} stroke="red" />
        <ReferenceLine y={0} stroke="red" />

        {/* Scatter for Historical Data */}
        <Scatter
          name="Historical Data"
          data={data}
          fill="#8884d8"
          // line={{ stroke: "#8884d8", strokeWidth: 2 }}
        />

        {/* Line Connecting All Data Points */}
        <Line
          type="cardinal"
          dataKey="gdp_growth"
          stroke="#82ca9d"
          dot={false}
          name="GDP / Inflation Rate of Change"
        />

        {/* Scatter for the Latest Data Point */}
        {lastDataPoint && (
          <Scatter
            name="Latest Data"
            data={[lastDataPoint]}
            fill="red"
            shape="circle"
          />
        )}
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export default QuadrantChart;
