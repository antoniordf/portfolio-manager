import React from "react";

const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const dataPoint = payload[0].payload;
    const date = new Date(dataPoint.date).toLocaleDateString();
    const gdpGrowth = dataPoint.gdp_growth.toFixed(2);
    const inflationGrowth = dataPoint.inflation_growth.toFixed(2);

    return (
      <div
        className="custom-tooltip"
        style={{
          backgroundColor: "#fff",
          padding: "10px",
          border: "1px solid #ccc",
          borderRadius: "5px",
        }}
      >
        <p>
          <strong>Date:</strong> {date}
        </p>
        <p>
          <strong>GDP Rate of Change:</strong> {gdpGrowth}%
        </p>
        <p>
          <strong>Inflation Rate of Change:</strong> {inflationGrowth}%
        </p>
      </div>
    );
  }

  return null;
};

export default CustomTooltip;
