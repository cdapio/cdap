/*
 * Copyright © 2020 Cask Data, Inc.
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

import React, { useState } from 'react';
import { WithStyles } from '@material-ui/core/styles/withStyles';
import { IWidgetProps } from 'components/AbstractWidget';
import ExpandableTimeRange from 'components/TimeRangePicker/ExpandableTimeRange';
import { DatePickerStyles } from 'components/AbstractWidget/DateTimeWidget';

interface IDateRangeWidgetProps extends IWidgetProps<null>, WithStyles<typeof DatePickerStyles> {}

const DateRangeWidget: React.FC<IDateRangeWidgetProps> = ({
  value,
  onChange,
  disabled,
  classes,
}) => {
  const delimiter = ',';
  const initRange = typeof value === 'string' ? value.split(delimiter) : ['0', '0'];
  const [startTime, updateStart] = useState(parseInt(initRange[0], 10));
  const [endTime, updateEnd] = useState(parseInt(initRange[1], 10));

  const onChangeHandler = ({ start, end }) => {
    const newStart: number = start || 0;
    const newEnd: number = end || 0;

    updateStart(newStart);
    updateEnd(newEnd);

    const updatedValue = [newStart, newEnd].join(delimiter);
    onChange(updatedValue);
  };

  return (
    <div className={classes.root}>
      <ExpandableTimeRange
        showRange={true}
        start={startTime}
        end={endTime}
        onChange={onChangeHandler}
        disabled={disabled}
      />
    </div>
  );
};

export default DateRangeWidget;
