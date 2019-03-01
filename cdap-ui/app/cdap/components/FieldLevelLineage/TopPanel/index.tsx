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
import Typography from '@material-ui/core/Typography';
import T from 'i18n-react';
import './TopPanel.scss';

const PREFIX = 'features.FieldLevelLineage.TopPanel';

const TopPanel: React.SFC = () => {
  return (
    <div className="top-panel row">
      <div className="col-4">
        <h4>{T.translate(`${PREFIX}.title`)}</h4>
        <div>{T.translate(`${PREFIX}.subtitle`)}</div>
      </div>

      <div className="col-4">
        <Typography variant="caption" className="time-picker-caption">
          {T.translate(`${PREFIX}.timePickerCaption`)}
        </Typography>
        <TimePicker />
      </div>
    </div>
  );
};

export default TopPanel;
