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

const data = [];

const endTime = Date.now();
for (let i = 59; i >= 0; i -= 2) {
  data.push({
    x: endTime - 2000 * i,
    y: generateRandomData(),
  });
}

function generateRandomData() {
  return (Math.random() * 4 + 1).toFixed(1);
}

const chartId = 'events-chart';

const Events: React.FC = () => {
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
            borderColor: '#0076dc',
            backgroundColor: '#cae7ef',
          },
        ],
      },
      options: {
        maintainAspectRatio: false,
        legend: {
          display: false,
        },
        title: {
          display: true,
          text: 'Events',
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
                unit: 'second',
              },
            },
          ],
          yAxes: [
            {
              ticks: {
                min: 0,
                max: 6,
              },
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
      chart.data.datasets[0].data.push({
        x: Date.now(),
        y: generateRandomData(),
      });
      chart.update();
      makeRealtime();
    }, 2000);
  }

  return (
    <div>
      <canvas id={chartId} width="300" height="300" />
    </div>
  );
};

export default Events;
