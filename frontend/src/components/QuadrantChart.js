import React, { useEffect, useState, useMemo } from "react";
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
import { useQuery, gql } from "@apollo/client";
import CustomTooltip from "./CustomTooltip";

// Define the GraphQL query
const GET_QUADRANT_DATA = gql`
  query GetQuadrantData($dataPoints: Int!) {
    quadrantData(dataPoints: $dataPoints) {
      date
      gdpGrowth
      inflationGrowth
    }
  }
`;

function QuadrantChart() {
  const [data, setData] = useState([]);
  const [lastDataPoint, setLastDataPoint] = useState(null);

  const {
    loading,
    error,
    data: queryData,
  } = useQuery(GET_QUADRANT_DATA, {
    variables: { dataPoints: 15 },
    fetchPolicy: "cache-first",
  });

  useEffect(() => {
    if (queryData) {
      const sortedData = queryData.quadrantData
        .map((item) => ({
          date: new Date(item.date),
          gdp_growth: item.gdpGrowth,
          inflation_growth: item.inflationGrowth,
        }))
        .sort((a, b) => a.date - b.date);
      setData(sortedData);
      setLastDataPoint(sortedData[sortedData.length - 1]);
    }
  }, [queryData]);

  // Memoize axis configurations to optimize performance
  const axisConfig = useMemo(() => {
    if (data.length === 0) {
      return {
        xDomain: [0, 0],
        xTicks: [],
        yDomain: [0, 0],
        yTicks: [],
      };
    }

    // Extract values for GDP and Inflation
    const gdpValues = data.map((d) => d.gdp_growth);
    const inflationValues = data.map((d) => d.inflation_growth);

    // Calculate min and max with padding
    const gdpMin = Math.min(...gdpValues);
    const gdpMax = Math.max(...gdpValues);
    const inflationMin = Math.min(...inflationValues);
    const inflationMax = Math.max(...inflationValues);

    const gdpPadding = 1; // Adjust padding as needed
    const inflationPadding = 0.5;

    const xDomain = [
      Math.floor(inflationMin - inflationPadding),
      Math.ceil(inflationMax + inflationPadding),
    ];
    const yDomain = [
      Math.floor(gdpMin - gdpPadding),
      Math.ceil(gdpMax + gdpPadding),
    ];

    // Generate ticks based on domain with a step of 1
    const generateTicks = (min, max, step = 1) => {
      const ticks = [];
      for (let i = min; i <= max; i += step) {
        ticks.push(i);
      }
      return ticks;
    };

    const xTicks = generateTicks(xDomain[0], xDomain[1]);
    const yTicks = generateTicks(yDomain[0], yDomain[1]);

    return { xDomain, xTicks, yDomain, yTicks };
  }, [data]);

  if (loading) return <p>Loading...</p>;
  if (error) console.log(error); // return <p>Error fetching data.</p>;

  return (
    <ResponsiveContainer width="100%" height={600}>
      <ComposedChart
        data={data}
        margin={{ top: 20, right: 40, bottom: 20, left: 40 }}
      >
        {/* Cartesian Grid without vertical and horizontal lines */}
        <CartesianGrid vertical={false} horizontal={false} />

        {/* X-Axis Configuration */}
        <XAxis
          type="number"
          dataKey="inflation_growth"
          name="Inflation Rate of Change (%)"
          domain={axisConfig.xDomain}
          ticks={axisConfig.xTicks}
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
          domain={axisConfig.yDomain}
          ticks={axisConfig.yTicks}
          label={{
            value: "GDP Rate of Change (%)",
            angle: -90,
            position: "insideLeft",
            offset: -10,
          }}
        />

        {/* Tooltip with Custom Tooltip Component */}
        <Tooltip content={<CustomTooltip />} />

        {/* Legend */}
        <Legend verticalAlign="top" height={36} />

        {/* Reference Lines at X=0 and Y=0 */}
        <ReferenceLine x={0} stroke="red" />
        <ReferenceLine y={0} stroke="red" />

        {/* Scatter for Historical Data (excluding the last data point) */}
        <Scatter
          name="Historical Data"
          data={data.slice(0, -1)}
          fill="#8884d8"
          shape="circle"
          size={60}
        />

        {/* Line Connecting All Data Points */}
        <Line
          type="cardinal" // You can switch to "monotone" if desired
          dataKey="gdp_growth"
          stroke="#82ca9d"
          dot={false}
          name="GDP Rate of Change"
        />

        {/* Scatter for the Latest Data Point */}
        {lastDataPoint && (
          <Scatter
            name="Latest Data"
            data={[lastDataPoint]}
            fill="red"
            shape="circle"
            size={80}
          />
        )}
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export default QuadrantChart;
