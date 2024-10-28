import React, { useLayoutEffect, useEffect, useRef } from "react";
import { createChart, CrosshairMode } from "lightweight-charts";
import PropTypes from "prop-types";

const VolumeChart = React.forwardRef(({ data }, ref) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const volumeSeriesRef = useRef();

  // Initialize the chart once using useLayoutEffect for synchronous execution
  useLayoutEffect(() => {
    if (!chartContainerRef.current) return;

    // Create the chart
    chartRef.current = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 150, // Adjust height as needed
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
  }, [ref]);

  // Update chart data when 'data' changes
  useEffect(() => {
    if (data && volumeSeriesRef.current) {
      // Transform the volume data into the format needed for the chart
      const formattedVolumeData = data.map((point) => ({
        time: point.date, // Correct key
        value: point.volume,
      }));

      // Set data on the volume series
      try {
        volumeSeriesRef.current.setData(formattedVolumeData);
      } catch (error) {
        console.error(`Error setting volume series data: ${error.message}`);
      }

      // Fit the chart to the new data
      try {
        chartRef.current.timeScale().fitContent();
      } catch (error) {
        console.error(
          `Error fitting volume chart to content: ${error.message}`
        );
      }
    } else if (volumeSeriesRef.current) {
      // Clear the chart if there is no data
      volumeSeriesRef.current.setData([]);
    }
  }, [data]);

  return (
    <div style={{ position: "relative" }}>
      {/* Chart Container */}
      <div ref={chartContainerRef} style={{ width: "100%", height: "150px" }} />
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
};

export default VolumeChart;
