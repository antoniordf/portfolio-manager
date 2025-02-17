import "./App.css";
import QuadrantChart from "./components/QuadrantChart";
import StockChartContainer from "./components/StockChartContainer";

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>Quadrant Chart</h1>
      </header>
      <main>
        <QuadrantChart />
        <StockChartContainer />
      </main>
    </div>
  );
}

export default App;
