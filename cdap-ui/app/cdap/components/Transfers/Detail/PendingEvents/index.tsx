/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as React from 'react';
import Chart from 'chart.js';

const chartId = 'pending-events-chart';
const data = [];
let lastData = 17;

const endTime = Date.now();
for (let i = 1799; i >= 0; i--) {
  const newData = generateRandomData(lastData);
  data.push({
    x: endTime - 1000 * i,
    y: newData,
  });
  lastData = newData;
}

function generateRandomData(seed) {
  const adjust = Math.ceil(Math.random() * 5 + 1) - 4;
  let newVal = seed + adjust;
  newVal = Math.max(newVal, 0);

  return newVal;
}

const PendingEvents: React.FC = () => {
  let chart;
  function renderChart() {
    const ctx = (document.getElementById(chartId) as HTMLCanvasElement).getContext('2d');
    chart = new Chart(ctx, {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'Latency',
            data,
            pointRadius: 0,
            borderColor: '#3cc801',
            backgroundColor: '#8af302',
          },
        ],
      },
      options: {
        maintainAspectRatio: false,
        legend: {
          display: false,
        },
        scales: {
          xAxes: [
            {
              type: 'time',
              position: 'bottom',
              gridLines: {
                drawOnChartArea: false,
              },
              time: {
                unit: 'minute',
              },
            },
          ],
          yAxes: [
            {
              scaleLabel: {
                display: true,
                labelString: 'events (thousands)',
              },
            },
          ],
        },
      },
    });

    makeRealtime();
  }

  React.useEffect(renderChart, []);

  function makeRealtime() {
    setTimeout(() => {
      chart.data.datasets[0].data.shift();
      let latestData = chart.data.datasets[0].data;
      latestData = latestData[latestData.length - 1].y;
      chart.data.datasets[0].data.push({
        x: Date.now(),
        y: generateRandomData(latestData),
      });
      chart.update();
      makeRealtime();
    }, 1000);
  }

  return (
    <React.Fragment>
      <h3 className="pl-5 pt-3">Pending Events</h3>
      <div className="pl-3 pr-3">
        <canvas id={chartId} width="300" height="300" />
      </div>
    </React.Fragment>
  );
};

export default PendingEvents;
