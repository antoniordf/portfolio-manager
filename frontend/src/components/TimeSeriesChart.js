// import React, { useEffect, useState, useMemo } from "react";
// import {
//   ComposedChart,
//   Scatter,
//   Line,
//   XAxis,
//   YAxis,
//   CartesianGrid,
//   Tooltip,
//   Legend,
//   ReferenceLine,
//   ResponsiveContainer,
// } from "recharts";
// import { useQuery, gql } from "@apollo/client";
// import CustomTooltip from "./CustomTooltip";

// // Define the GraphQL query
// const GET_TIME_SERIES_DATA = gql`
//   query GetTimeSeriesData($dataPoints: Int!) {
//     timeSeriesData(dataPoints: $dataPoints) {
//       date
//       gdpGrowth
//       inflationGrowth
//     }
//   }
// `;
