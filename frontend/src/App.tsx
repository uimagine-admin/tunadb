import React, { useEffect, useState } from 'react';
import { Doughnut } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(ArcElement, Tooltip, Legend);

// We know the ring is a 64-bit space, max token range is 2^64
// We'll treat total ring size as a big number:
const MAX_UINT64 = BigInt('18446744073709551616'); // 2^64

interface RingResponse {
  nodes: {
    id: string;
    name: string;
    ip_address: string;
    port: number;
    status: string;
  }[];
  token_ranges: {
    [range: string]: string[];
  };
}

function App() {
  const [ringData, setRingData] = useState<RingResponse | null>(null);
  const [chartData, setChartData] = useState<any>(null);

  // Fetch ring data
  const fetchRingData = async () => {
    try {
      const res = await fetch('http://localhost:50058/ring');
      const data: RingResponse = await res.json();
      setRingData(data);
    } catch (err) {
      console.error('Error fetching ring data:', err);
    }
  };

  useEffect(() => {
    fetchRingData();
    const interval = setInterval(fetchRingData, 5000); // poll every 5 seconds
    return () => clearInterval(interval);
  }, []);

  // Prepare chart data whenever ringData changes
  useEffect(() => {
    if (!ringData) return;

    const tokenRanges = ringData.token_ranges;
    // Convert token ranges into chart segments
    // Each token_range key: "start:end"
    // We'll compute arc size = (end - start) mod 2^64, then scale to a percentage.
    const rangeEntries = Object.entries(tokenRanges);

    // We'll sort the ranges by their starting token to visualize in order
    const sortedRanges = rangeEntries.sort((a, b) => {
      const startA = BigInt(a[0].split(':')[0]);
      const startB = BigInt(b[0].split(':')[0]);
      return startA < startB ? -1 : startA > startB ? 1 : 0;
    });

    const labels: string[] = [];
    const dataValues: number[] = [];
    const backgroundColors: string[] = [];
    const hoverInfos: { label: string; nodes: string[] }[] = [];

    // We will map each node ID to a color consistently
    // But here we can just generate random colors for simplicity or use a fixed palette.
    // Let's just generate random colors for each segment.
    // In a production scenario, you might want a stable color scheme.
    const getRandomColor = () => {
      const r = Math.floor(Math.random() * 200);
      const g = Math.floor(Math.random() * 200);
      const b = Math.floor(Math.random() * 200);
      return `rgb(${r},${g},${b})`;
    };

    for (const [rangeStr, nodes] of sortedRanges) {
      const [startStr, endStr] = rangeStr.split(':');
      const start = BigInt(startStr);
      const end = BigInt(endStr);

      let arcLength: bigint;
      if (end > start) {
        arcLength = end - start;
      } else {
        // wrap around
        arcLength = MAX_UINT64 - start + end;
      }

      // Convert arc length to a fraction of 360 degrees or just use relative percentages
      // Chart.js data are just relative, we can just supply arc lengths as-is.
      // But arcLength is huge. We'll scale so total = 100 (percentage).
      // Actually, the entire ring sums up to 2^64, so arcLength/MAX_UINT64 = fraction of full ring.
      const fraction = Number(arcLength) / Number(MAX_UINT64);
      // fraction of full ring * 100
      const value = fraction * 100;

      labels.push(rangeStr);
      dataValues.push(value);
      backgroundColors.push(getRandomColor());
      hoverInfos.push({ label: rangeStr, nodes });
    }

    const data = {
      labels,
      datasets: [
        {
          label: 'Token Ranges',
          data: dataValues,
          backgroundColor: backgroundColors,
          borderColor: '#fff',
          borderWidth: 1,
          hoverOffset: 10,
        },
      ],
    };

    const options = {
      responsive: true,
      plugins: {
        tooltip: {
          callbacks: {
            label: (tooltipItem: any) => {
              const idx = tooltipItem.dataIndex;
              const info = hoverInfos[idx];
              return [
                `Range: ${info.label}`,
                `Nodes: ${info.nodes.join(', ')}`,
              ];
            },
          },
        },
        legend: {
          display: false,
        },
      },
      cutout: '50%', // make it doughnut instead of pie
    };

    setChartData({ data, options });
  }, [ringData]);

  return (
    <div className="bg-gray-100 min-h-screen min-w-screen p-20">
      <h1 className="text-3xl font-bold text-center mb-6">
        Consistent Hashing Ring Visualization
      </h1>
      <p className="text-center mb-6 text-gray-700">
        This visualization represents the ring of the consistent hashing setup.
        Each arc represents a token range segment handled by one or more nodes.
      </p>
      <div className="p-4 flex flex-row items-center justify-center space-x-10">
        <div className="flex flex-1 max-w-[600px]">
          {chartData ? (
            <div className="w-full">
              <Doughnut data={chartData.data} options={chartData.options} />
            </div>
          ) : (
            <p className="text-center text-gray-500">Loading ring data...</p>
          )}
        </div>
        {ringData && (
          <div className="mt-8 bg-white p-4 rounded shadow">
            <h2 className="text-xl font-semibold mb-4">Nodes</h2>
            <ul className="space-y-2">
              {ringData.nodes.map((node) => (
                <li key={node.id} className="p-2 bg-gray-50 rounded">
                  <div className="font-semibold">
                    {node.name} ({node.id})
                  </div>
                  <div className="text-sm text-gray-600">
                    IP: {node.ip_address} | Port: {node.port} | Status:{' '}
                    {node.status}
                  </div>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
