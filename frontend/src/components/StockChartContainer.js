import React, { useState } from "react";
import StockChart from "./StockChart";
import VolumeChart from "./VolumeChart";
import { useQuery } from "@apollo/client";
import { GET_STOCK_TIME_SERIES } from "../queries/getStockTimeSeries";
import { formatDate } from "../utils/formatDate";

function StockChartContainer() {
  const today = new Date();
  const fiveYearsAgo = new Date(
    today.getFullYear() - 5,
    today.getMonth(),
    today.getDate()
  );

  const defaultEndDate = formatDate(today);
  const defaultStartDate = formatDate(fiveYearsAgo);

  const [ticker, setTicker] = useState("AAPL"); // Default ticker symbol
  const [inputValue, setInputValue] = useState("AAPL");
  const [startDate, setStartDate] = useState(defaultStartDate);
  const [endDate, setEndDate] = useState(defaultEndDate);
  const [chartType, setChartType] = useState("line");
  const [showVolume, setShowVolume] = useState(true);

  // Fetch stock data using useQuery
  const { loading, error, data } = useQuery(GET_STOCK_TIME_SERIES, {
    variables: { seriesId: ticker, startDate, endDate },
    fetchPolicy: "cache-first",
  });

  const handleInputChange = (event) => {
    setInputValue(event.target.value.toUpperCase());
  };

  const handleStartDateChange = (event) => {
    setStartDate(event.target.value);
  };

  const handleEndDateChange = (event) => {
    setEndDate(event.target.value);
  };

  const handleChartTypeChange = (event) => {
    setChartType(event.target.value);
  };

  const handleShowVolumeChange = (event) => {
    setShowVolume(event.target.checked);
  };

  const handleSubmit = (event) => {
    event.preventDefault();

    // Validate dates
    if (startDate > endDate) {
      alert("Start date cannot be after end date.");
      return;
    }

    setTicker(inputValue);
  };

  return (
    <div style={{ margin: "20px 0" }}>
      <form onSubmit={handleSubmit} style={{ marginBottom: "20px" }}>
        {/* Ticker Input */}
        <label htmlFor="ticker-input" style={{ marginRight: "10px" }}>
          Enter Stock Ticker:
        </label>
        <input
          id="ticker-input"
          type="text"
          value={inputValue}
          onChange={handleInputChange}
          maxLength={5}
          placeholder="e.g., AAPL"
          style={{ padding: "5px", fontSize: "16px" }}
        />

        {/* Start Date Input */}
        <label
          htmlFor="start-date"
          style={{ marginRight: "10px", minWidth: "100px" }}
        >
          Start Date:
        </label>
        <input
          id="start-date"
          type="date"
          value={startDate}
          onChange={handleStartDateChange}
          max={formatDate(today)} // Prevent selecting a future date
          style={{ padding: "5px", fontSize: "16px", marginRight: "20px" }}
          required
        />

        {/* End Date Input */}
        <label
          htmlFor="end-date"
          style={{ marginRight: "10px", minWidth: "100px" }}
        >
          End Date:
        </label>
        <input
          id="end-date"
          type="date"
          value={endDate}
          onChange={handleEndDateChange}
          max={formatDate(today)} // Prevent selecting a future date
          style={{ padding: "5px", fontSize: "16px", marginRight: "20px" }}
          required
        />

        {/* Chart Type Selection */}
        <fieldset style={{ border: "none", marginRight: "20px", padding: "0" }}>
          <legend style={{ marginRight: "10px" }}>Chart Type:</legend>
          <label>
            <input
              type="radio"
              value="line"
              checked={chartType === "line"}
              onChange={handleChartTypeChange}
            />
            Line
          </label>
          <label style={{ marginLeft: "10px" }}>
            <input
              type="radio"
              value="candlestick"
              checked={chartType === "candlestick"}
              onChange={handleChartTypeChange}
            />
            Candlestick
          </label>
        </fieldset>

        {/* Show Volume Toggle */}
        <label style={{ marginRight: "10px", minWidth: "100px" }}>
          <input
            type="checkbox"
            checked={showVolume}
            onChange={handleShowVolumeChange}
            style={{ marginRight: "5px" }}
          />
          Show Volume
        </label>

        {/* Submit Button */}
        <button
          type="submit"
          style={{
            marginLeft: "10px",
            padding: "5px 10px",
            fontSize: "16px",
            cursor: "pointer",
          }}
        >
          Update Chart
        </button>
      </form>

      {/* Render Charts Only When Data is Available */}
      {!loading && !error && data && data.stockTimeSeries.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
          {/* Price Chart */}
          <StockChart
            seriesId={ticker}
            chartType={chartType}
            data={data.stockTimeSeries}
          />

          {/* Volume Chart */}
          {showVolume && <VolumeChart data={data.stockTimeSeries} />}
        </div>
      )}

      {/* Handle Loading, Error, and No-Data States */}
      {loading && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: "50%",
            transform: "translate(-50%, -50%)",
            background: "rgba(255, 255, 255, 0.8)",
            padding: "10px",
            borderRadius: "5px",
            zIndex: 1,
          }}
        >
          <p>Loading stock data...</p>
        </div>
      )}

      {error && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: "50%",
            transform: "translate(-50%, -50%)",
            background: "rgba(255, 0, 0, 0.1)",
            padding: "10px",
            borderRadius: "5px",
            zIndex: 1,
          }}
        >
          <p>Error fetching stock data: {error.message}</p>
        </div>
      )}

      {data && data.stockTimeSeries.length === 0 && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: "50%",
            transform: "translate(-50%, -50%)",
            background: "rgba(255, 255, 255, 0.8)",
            padding: "10px",
            borderRadius: "5px",
            zIndex: 1,
          }}
        >
          <p>No data available for ticker symbol "{ticker}".</p>
        </div>
      )}
    </div>
  );
}

export default StockChartContainer;
