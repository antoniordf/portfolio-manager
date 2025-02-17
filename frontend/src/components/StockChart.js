import React, { useLayoutEffect, useEffect, useRef } from "react";
import { createChart, CrosshairMode } from "lightweight-charts";
import PropTypes from "prop-types";

const StockChart = React.forwardRef(
  ({ chartType, data, onReady = null }, ref) => {
    const chartContainerRef = useRef();
    const chartRef = useRef();
    const priceSeriesRef = useRef();

    // Initialize the chart once
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

      // Attach the chart instance to the forwarded ref
      if (ref) {
        ref.current = chartRef.current;
      }

      // Handle window resize
      const handleResize = () => {
        if (chartRef.current) {
          chartRef.current.applyOptions({
            width: chartContainerRef.current.clientWidth,
          });
          chartRef.current.timeScale().fitContent();
        }
      };
      window.addEventListener("resize", handleResize);

      // Clean up on unmount
      return () => {
        window.removeEventListener("resize", handleResize);
        if (chartRef.current) {
          chartRef.current.remove();
          chartRef.current = null;
          priceSeriesRef.current = null;
        }
      };
    }, []); // Empty dependency array

    // Update or create the price series when 'chartType' or 'data' changes
    useEffect(() => {
      if (!chartRef.current || !data?.length) return;

      try {
        // Always remove existing series first if it exists
        if (priceSeriesRef.current) {
          chartRef.current.removeSeries(priceSeriesRef.current);
        }

        // Create new series
        if (chartType === "line") {
          priceSeriesRef.current = chartRef.current.addLineSeries({
            color: "#2962FF",
            lineWidth: 2,
          });
        } else {
          priceSeriesRef.current = chartRef.current.addCandlestickSeries({
            upColor: "#4CAF50",
            downColor: "#F44336",
            borderVisible: false,
            wickVisible: true,
          });
        }

        // Format and set data
        const formattedData = data.map((point) => ({
          time: point.date,
          ...(chartType === "line"
            ? { value: point.close }
            : {
                open: point.open,
                high: point.high,
                low: point.low,
                close: point.close,
              }),
        }));

        priceSeriesRef.current.setData(formattedData);
        chartRef.current.timeScale().fitContent();

        if (onReady) {
          setTimeout(onReady, 0);
        }
      } catch (error) {
        console.error("Error updating price chart:", error);
      }
    }, [data, chartType, onReady]);

    return (
      <div style={{ position: "relative" }}>
        {/* Chart Container */}
        <div
          ref={chartContainerRef}
          style={{ width: "100%", height: "400px" }}
        />
      </div>
    );
  }
);

StockChart.propTypes = {
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
  onReady: PropTypes.func,
};

export default React.memo(StockChart);
