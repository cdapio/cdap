/*
 * Copyright © 2018 Cask Data, Inc.
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
import TimePicker from 'components/FieldLevelLineage/TimePicker';
import './TopPanel.scss';

const TopPanel: React.SFC = () => {
  return (
    <div className="top-panel">
      <div className="title-row">
        <div className="title">Field level lineage</div>

        <TimePicker />
      </div>

      <div className="subtitle">Select a field to explore root cause and impact</div>
    </div>
  );
};

export default TopPanel;
