import React from "react";

const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const dataPoint = payload[0].payload;
    const date = new Date(dataPoint.date).toLocaleDateString();
    const gdpGrowth = dataPoint.gdp_growth.toFixed(2);
    const inflationGrowth = dataPoint.inflation_growth.toFixed(2);

    return (
      <div className="custom-tooltip">
        <p>
          <strong>Date:</strong> {date}
        </p>
        <p>
          <strong>GDP Growth:</strong> {gdpGrowth}%
        </p>
        <p>
          <strong>Inflation Growth:</strong> {inflationGrowth}%
        </p>
      </div>
    );
  }

  return null;
};

export default CustomTooltip;
