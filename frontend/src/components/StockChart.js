import React, { useLayoutEffect, useEffect, useRef } from "react";
import { createChart, CrosshairMode } from "lightweight-charts";
import PropTypes from "prop-types";

const StockChart = React.forwardRef(({ seriesId, chartType, data }, ref) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const priceSeriesRef = useRef();

  // Initialize the chart once using useLayoutEffect for synchronous execution
  useLayoutEffect(() => {
    if (!chartContainerRef.current) return;

    // Create the chart
    chartRef.current = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 400,
      layout: {
        backgroundColor: "#ffffff",
        textColor: "#000",
      },
      grid: {
        vertLines: {
          color: "#e0e0e0",
        },
        horzLines: {
          color: "#e0e0e0",
        },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
      },
      rightPriceScale: {
        borderColor: "#e0e0e0",
      },
      timeScale: {
        borderColor: "#e0e0e0",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    // Add the Price Series (Line or Candlestick)
    if (chartType === "line") {
      priceSeriesRef.current = chartRef.current.addLineSeries({
        color: "#2962FF",
        lineWidth: 2,
      });
    } else if (chartType === "candlestick") {
      priceSeriesRef.current = chartRef.current.addCandlestickSeries({
        upColor: "#4CAF50",
        downColor: "#F44336",
        borderDownColor: "#F44336",
        borderUpColor: "#4CAF50",
        wickDownColor: "#F44336",
        wickUpColor: "#4CAF50",
      });
    }

    // Handle window resize
    const handleResize = () => {
      chartRef.current.applyOptions({
        width: chartContainerRef.current.clientWidth,
      });
      setTimeout(() => {
        chartRef.current.timeScale().fitContent();
      }, 0);
    };
    window.addEventListener("resize", handleResize);

    // Attach the chart instance to the forwarded ref
    if (ref) {
      ref.current = chartRef.current;
    }

    // Clean up on unmount
    return () => {
      window.removeEventListener("resize", handleResize);
      chartRef.current.remove();
    };
  }, [chartType, ref]);

  // Update chart data when 'data' changes
  useEffect(() => {
    if (data && priceSeriesRef.current) {
      // Transform the price data into the format needed for the chart
      let formattedPriceData = [];
      if (chartType === "line") {
        formattedPriceData = data.map((point) => ({
          time: point.date, // Correct key
          value: point.close,
        }));
      } else if (chartType === "candlestick") {
        formattedPriceData = data.map((point) => ({
          time: point.date, // Correct key
          open: point.open,
          high: point.high,
          low: point.low,
          close: point.close,
        }));
      }

      // Set data on the price series
      try {
        priceSeriesRef.current.setData(formattedPriceData);
      } catch (error) {
        console.error(`Error setting price series data: ${error.message}`);
      }

      // Fit the chart to the new data
      try {
        chartRef.current.timeScale().fitContent();
      } catch (error) {
        console.error(`Error fitting chart to content: ${error.message}`);
      }
    } else if (priceSeriesRef.current) {
      // Clear the chart if there is no data
      priceSeriesRef.current.setData([]);
    }
  }, [data, chartType]);

  return (
    <div style={{ position: "relative" }}>
      {/* Chart Container */}
      <div ref={chartContainerRef} style={{ width: "100%", height: "400px" }} />
    </div>
  );
});

StockChart.propTypes = {
  seriesId: PropTypes.string.isRequired,
  chartType: PropTypes.oneOf(["line", "candlestick"]).isRequired,
  data: PropTypes.arrayOf(
    PropTypes.shape({
      date: PropTypes.string.isRequired,
      open: PropTypes.number,
      high: PropTypes.number,
      low: PropTypes.number,
      close: PropTypes.number,
      volume: PropTypes.number,
    })
  ).isRequired,
};

export default StockChart;
