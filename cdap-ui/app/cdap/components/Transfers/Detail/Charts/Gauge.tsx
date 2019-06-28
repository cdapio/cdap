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
import ReactSpeedometer from 'react-d3-speedometer';

const Gauge: React.FC = () => {
  const [level, setLevel] = React.useState(73);

  React.useEffect(() => {
    generateRandom();
  }, []);

  function generateRandom() {
    const timeoutTime = Math.ceil(Math.random() * 7000);

    setTimeout(() => {
      setLevel(Math.ceil(Math.random() * 100));
      generateRandom();
    }, timeoutTime);
  }

  return (
    <div>
      <ReactSpeedometer
        value={level}
        minValue={0}
        maxValue={100}
        maxSegmentLabels={0}
        segments={500}
        needleHeightRatio={0.6}
        currentValueText={`${level} events/sec`}
        fluidWidth={true}
      />
    </div>
  );
};

export default Gauge;
