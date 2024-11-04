import React, { useLayoutEffect, useEffect, useRef } from "react";
import { createChart, CrosshairMode } from "lightweight-charts";
import PropTypes from "prop-types";

const VolumeChart = React.forwardRef(({ data, onReady = null }, ref) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const volumeSeriesRef = useRef();

  // Initialize the chart once using useLayoutEffect for synchronous execution
  useLayoutEffect(() => {
    if (!chartContainerRef.current) return;

    // Create the chart
    chartRef.current = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 150,
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
        scaleMargins: {
          top: 0.8, // Keep the top margin to prevent overlap
          bottom: 0,
        },
      },
      timeScale: {
        borderColor: "#e0e0e0",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    // Add the Volume Series as a Histogram
    volumeSeriesRef.current = chartRef.current.addHistogramSeries({
      color: "#26a69a",
      priceFormat: {
        type: "volume",
      },
      priceScaleId: "volume", // Use a separate price scale
      scaleMargins: {
        top: 0.8, // Position the volume at the bottom 20% of the chart
        bottom: 0,
        left: 0,
        right: 0,
      },
    });

    // Attach the chart instance to the forwarded ref
    if (ref) {
      ref.current = chartRef.current;
    }

    // Handle window resize
    const handleResize = () => {
      if (chartRef.current && chartContainerRef.current) {
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
        volumeSeriesRef.current = null;
      }
    };
  }, []);

  // Update chart data when 'data' changes
  useEffect(() => {
    if (!chartRef.current || !volumeSeriesRef.current || !data?.length) return;

    try {
      const formattedData = data.map((point) => ({
        time: point.date,
        value: point.volume,
        color: point.close >= point.open ? "#26a69a" : "#ef5350",
      }));

      volumeSeriesRef.current.setData(formattedData);

      // Ensure chart is properly scaled
      chartRef.current.timeScale().fitContent();

      // Delay the ready callback to ensure chart is fully rendered
      if (onReady) {
        requestAnimationFrame(() => {
          onReady();
        });
      }
    } catch (error) {
      console.error("Error updating volume chart:", error);
    }
  }, [data, onReady]);

  return (
    <div
      style={{
        position: "relative",
        width: "100%",
        height: "150px",
        marginTop: "1px", // Add small gap between charts
        visibility: data?.length ? "visible" : "hidden",
      }}
    >
      {/* Chart Container */}
      <div
        ref={chartContainerRef}
        style={{
          width: "100%",
          height: "100%",
        }}
      />
    </div>
  );
});

VolumeChart.propTypes = {
  data: PropTypes.arrayOf(
    PropTypes.shape({
      date: PropTypes.string.isRequired,
      volume: PropTypes.number.isRequired,
    })
  ).isRequired,
  onReady: PropTypes.func,
};

export default VolumeChart;
