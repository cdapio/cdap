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

import React, { useContext, useState } from 'react';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import T from 'i18n-react';
import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import ExpandableTimeRange from 'components/TimeRangePicker/ExpandableTimeRange';
import { IContextState, FllContext } from 'components/FieldLevelLineage/v2/Context/FllContext';

const PREFIX = 'features.FieldLevelLineage.v2.TimeRangeOptions';

const styles = () => {
  return {
    button: {
      width: '150px',
    },
  };
};

export default function TimeRangePicker() {
  const { start, end, selection, setTimeRange } = useContext<IContextState>(FllContext);

  const onSelect = (e: React.ChangeEvent<{ value: string }>) => {
    // update selection
    const range = e.target.value;
    setTimeRange(range);

    // render date range picker if selection is custom
    // if (range === TIME_OPTIONS[0]) {
    //   renderCustomTimeRange();
    // }
  };

  const renderCustomTimeRange = () => {
    if (this.props.selections !== TIME_OPTIONS[0]) {
      return null;
    }

    return (
      <div className="custom-time-range-container">
        <ExpandableTimeRange onDone={this.onDone} inSeconds={true} start={start} end={end} />
      </div>
    );
  };

  return (
    <div>
      <span>View</span>
      <Select value={selection} onChange={onSelect}>
        {TIME_OPTIONS.map((option) => {
          return (
            <MenuItem value={option} key={option}>
              {T.translate(`${PREFIX}.${option}`)}
            </MenuItem>
          );
        })}
      </Select>
    </div>
  );
}
