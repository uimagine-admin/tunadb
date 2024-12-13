import React, { useEffect, useRef, useState } from 'react';
import { Doughnut } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(ArcElement, Tooltip, Legend);

const MAX_UINT64 = BigInt('18446744073709551616'); // 2^64

interface NodeInfo {
  id: string;
  name: string;
  ip_address: string;
  port: number;
  status: string;
}

interface RingResponse {
  nodes: NodeInfo[];
  token_ranges: {
    [range: string]: string[];
  };
}

interface LogEntry {
  timestamp: string;
  message: string;
}

function App() {
  const [ringData, setRingData] = useState<RingResponse | null>(null);
  const [chartData, setChartData] = useState<any>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  const prevRingDataRef = useRef<RingResponse | null>(null);

  // This ref holds a stable mapping from token range to color
  const rangeColorsRef = useRef<{ [range: string]: string }>({});

  // Color functions
  const getNodeStatusColor = (status: string) => {
    switch (status) {
      case 'ALIVE':
        return 'text-green-600';
      case 'SUSPECT':
        return 'text-yellow-600';
      default:
        return 'text-red-600';
    }
  };

  const getRandomColor = () => {
    const r = Math.floor(Math.random() * 200);
    const g = Math.floor(Math.random() * 200);
    const b = Math.floor(Math.random() * 200);
    return `rgb(${r},${g},${b})`;
  };

  const fetchRingData = async () => {
    try {
      const res = await fetch('http://localhost:50058/ring');
      const data: RingResponse = await res.json();
      handleDataUpdate(data);
    } catch (err) {
      console.error('Error fetching ring data:', err);
    }
  };

  // Handle data updates: detect changes and log them
  const handleDataUpdate = (newData: RingResponse) => {
    const oldData = prevRingDataRef.current;
    let logMsg = '';

    if (!oldData) {
      // First load
      logMsg = 'Initial data loaded.';
    } else {
      const changes = detectChanges(oldData, newData);
      if (changes.length === 0) {
        logMsg = 'No change detected.';
      } else {
        logMsg = `Changes detected: ${changes.join(', ')}`;
      }
    }

    const timestamp = new Date().toLocaleString();
    setLogs((prev) => [{ timestamp, message: logMsg }, ...prev]);

    prevRingDataRef.current = newData;
    setRingData(newData);
  };

  // Detect changes between old and new ring data
  const detectChanges = (
    oldData: RingResponse,
    newData: RingResponse
  ): string[] => {
    const changes: string[] = [];

    // Compare nodes
    const oldNodesMap: { [id: string]: NodeInfo } = {};
    oldData.nodes.forEach((n) => (oldNodesMap[n.id] = n));

    const newNodesMap: { [id: string]: NodeInfo } = {};
    newData.nodes.forEach((n) => (newNodesMap[n.id] = n));

    // Check for added nodes
    for (const newNode of newData.nodes) {
      if (!oldNodesMap[newNode.id]) {
        changes.push(`Node added: ${newNode.name} (${newNode.id})`);
      } else {
        // Check status change
        const oldNode = oldNodesMap[newNode.id];
        if (oldNode.status !== newNode.status) {
          changes.push(
            `Node status changed: ${newNode.id} from ${oldNode.status} to ${newNode.status}`
          );
        }
      }
    }

    // Check for removed nodes
    for (const oldNode of oldData.nodes) {
      if (!newNodesMap[oldNode.id]) {
        changes.push(`Node removed: ${oldNode.name} (${oldNode.id})`);
      }
    }

    // Compare token ranges
    const oldRanges = Object.keys(oldData.token_ranges).sort();
    const newRanges = Object.keys(newData.token_ranges).sort();

    // Check for added ranges
    for (const nr of newRanges) {
      if (!oldData.token_ranges[nr]) {
        changes.push(`Token range added: ${nr}`);
      }
    }

    // Check for removed ranges
    for (const orr of oldRanges) {
      if (!newData.token_ranges[orr]) {
        changes.push(`Token range removed: ${orr}`);
      }
    }

    return changes;
  };

  useEffect(() => {
    fetchRingData();
    const interval = setInterval(fetchRingData, 2000); // poll every 5 seconds
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (!ringData) return;

    const tokenRanges = ringData.token_ranges;
    const rangeEntries = Object.entries(tokenRanges);

    // Sort by starting token
    const sortedRanges = rangeEntries.sort((a, b) => {
      const startA = BigInt(a[0].split(':')[0]);
      const startB = BigInt(b[0].split(':')[0]);
      return startA < startB ? -1 : startA > startB ? 1 : 0;
    });

    // Maintain stable colors
    const existingColors = rangeColorsRef.current;
    const newColorMap: { [range: string]: string } = {};

    // Keep old colors for unchanged ranges, assign new colors for new ranges
    for (const [rangeStr] of sortedRanges) {
      if (existingColors[rangeStr]) {
        newColorMap[rangeStr] = existingColors[rangeStr];
      } else {
        // Assign a new color
        newColorMap[rangeStr] = getRandomColor();
      }
    }

    // Update the ref with the new mapping
    rangeColorsRef.current = newColorMap;

    const labels: string[] = [];
    const dataValues: number[] = [];
    const backgroundColors: string[] = [];
    const hoverInfos: { label: string; nodes: string[] }[] = [];

    for (const [rangeStr, nodes] of sortedRanges) {
      const [startStr, endStr] = rangeStr.split(':');
      const start = BigInt(startStr);
      const end = BigInt(endStr);

      let arcLength: bigint;
      if (end > start) {
        arcLength = end - start;
      } else {
        arcLength = MAX_UINT64 - start + end;
      }

      const fraction = Number(arcLength) / Number(MAX_UINT64);
      const value = fraction * 100;

      labels.push(rangeStr);
      dataValues.push(value);
      backgroundColors.push(rangeColorsRef.current[rangeStr]);
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
      cutout: '50%', // doughnut style
    };

    setChartData({ data, options });
  }, [ringData]);

  return (
    <div className="bg-gray-100 min-h-screen w-full p-8">
      <h1 className="text-3xl font-bold text-center mb-6">
        Consistent Hashing Ring Visualization
      </h1>
      <p className="text-center mb-6 text-gray-700">
        This visualization represents the ring of the consistent hashing setup.
        Each arc represents a token range segment handled by one or more nodes.
      </p>

      <div
        className="flex flex-row items-center justify-center space-x-10 p-10"
        style={{ height: 'calc(100vh - 160px)' }}
      >
        {/* Left side: Ring */}
        <div className="flex-1 max-w-[600px]">
          {chartData ? (
            <Doughnut data={chartData.data} options={chartData.options} />
          ) : (
            <p className="text-center text-gray-500">Loading ring data...</p>
          )}
        </div>

        {/* Right side: Nodes and Logs */}
        <div className="flex-1 flex flex-col space-y-8 h-full">
          {ringData && (
            <div className="bg-white p-4 rounded shadow">
              <h2 className="text-xl font-semibold mb-4">Nodes</h2>
              <ul className="space-y-2">
                {ringData.nodes.map((node) => {
                  const statusColor = getNodeStatusColor(node.status);
                  return (
                    <li key={node.id} className="p-2 bg-gray-50 rounded">
                      <div className="font-semibold flex items-center space-x-2">
                        <span>
                          {node.name} ({node.id})
                        </span>
                        <span className={`${statusColor} font-bold text-sm`}>
                          ●
                        </span>
                      </div>
                      <div className="text-sm text-gray-600">
                        IP: {node.ip_address} | Port: {node.port} | Status:{' '}
                        <span className={`${statusColor}`}>{node.status}</span>
                      </div>
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
          <div className="bg-white p-4 rounded shadow overflow-auto max-h-full h-full">
            <h2 className="text-xl font-semibold mb-4">Logs</h2>
            <ul className="space-y-2">
              {logs.map((log, index) => (
                <li key={index} className="text-sm text-gray-700">
                  <span className="font-semibold">{log.timestamp}:</span>{' '}
                  {log.message}
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
