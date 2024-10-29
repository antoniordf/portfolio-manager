import { useEffect } from "react";

function useChartSynchronization(
  stockChartRef,
  volumeChartRef,
  synchronize = true
) {
  useEffect(() => {
    if (!synchronize) return; // Exit early if synchronization is disabled

    if (
      stockChartRef.current &&
      volumeChartRef.current &&
      stockChartRef.current.timeScale &&
      volumeChartRef.current.timeScale
    ) {
      const stockTimeScale = stockChartRef.current.timeScale();
      const volumeTimeScale = volumeChartRef.current.timeScale();

      let isSyncing = false;

      const handleStockScaleChange = () => {
        if (isSyncing) return;
        isSyncing = true;
        const stockRange = stockTimeScale.getVisibleLogicalRange();
        if (stockRange) {
          console.log(
            "Synchronizing volume Chart to Stock Chart range:",
            stockRange
          );
          volumeTimeScale.setVisibleLogicalRange(stockRange);
        }
        isSyncing = false;
      };

      const handleVolumeScaleChange = () => {
        if (isSyncing) return;
        isSyncing = true;
        const volumeRange = volumeTimeScale.getVisibleLogicalRange();
        if (volumeRange) {
          console.log(
            "Synchronizing Stock Chart to Volume Chart range:",
            volumeRange
          );
          stockTimeScale.setVisibleLogicalRange(volumeRange);
        }
        isSyncing = false;
      };

      stockTimeScale.subscribeVisibleLogicalRangeChange(handleStockScaleChange);
      volumeTimeScale.subscribeVisibleLogicalRangeChange(
        handleVolumeScaleChange
      );

      console.log("Synchronization setup complete.");

      return () => {
        stockTimeScale.unsubscribeVisibleLogicalRangeChange(
          handleStockScaleChange
        );
        volumeTimeScale.unsubscribeVisibleLogicalRangeChange(
          handleVolumeScaleChange
        );
        console.log("Synchronization cleanup complete.");
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [synchronize]);
}

export default useChartSynchronization;
