import React, { useEffect, useState } from "react";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";

function QuadrantChart() {
  const [data, setData] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch("/quadrant_data/");
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const data = await response.json();
        const processedData = data.map((item) => ({
          date: new Date(item.date),
          gdp_growth: item.gdp_growth,
          inflation_growth: item.inflation_growth,
        }));
        setData(processedData);
      } catch (error) {
        console.error("Error fetching data: ", error);
      }
    };
    fetchData();
  }, []);

  return (
    <ScatterChart
      width={800}
      height={600}
      margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
    >
      <CartesianGrid />
      <XAxis
        type="number"
        dataKey="inflation_growth"
        name="Inflation Growth (%)"
        label={{
          value: "Inflation Growth (%)",
          position: "insideBottom",
          offset: -10,
        }}
      />
      <YAxis
        type="number"
        dataKey="gdp_growth"
        name="GDP Growth (%)"
        label={{
          value: "GDP Growth (%)",
          angle: -90,
          position: "insideLeft",
          offset: -5,
        }}
      />
      <Tooltip cursor={{ strokeDasharray: "3 3" }} />
      <Legend />
      <ReferenceLine x={0} stroke="red" />
      <ReferenceLine y={0} stroke="red" />
      <Scatter name="Economic Data" data={data} fill="#8884d8" />
    </ScatterChart>
  );
}

export default QuadrantChart;
