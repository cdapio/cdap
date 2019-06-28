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
    y: Math.ceil(Math.random() * 20 + 4),
  });
}

const Latency: React.FC = () => {
  let chart;
  function renderChart() {
    const ctx = (document.getElementById('latency-chart') as HTMLCanvasElement).getContext('2d');
    chart = new Chart(ctx, {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'Latency',
            data,
            pointRadius: 0,
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
          text: 'Latency',
        },
        scales: {
          xAxes: [
            {
              type: 'time',
              position: 'bottom',
              gridLines: {
                drawOnChartArea: false,
              },
              displayFormats: 'hh:mm:ss',
              ticks: {
                maxTicksLimit: 5,
              },
            },
          ],
          yAxes: [
            {
              ticks: {
                min: 0,
                max: 30,
              },
              scaleLabel: {
                display: true,
                labelString: 'seconds',
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
        y: Math.ceil(Math.random() * 20),
      });
      chart.update();
      makeRealtime();
    }, 2000);
  }

  return (
    <div>
      <canvas id="latency-chart" width="300" height="300" />
    </div>
  );
};

export default Latency;
