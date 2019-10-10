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

import React, { useState } from 'react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { IWidgetProps } from 'components/AbstractWidget';
import ExpandableTimeRange from 'components/TimeRangePicker/ExpandableTimeRange';

export const DatePickerStyles = () => {
  return {
    root: {
      height: '44px',
    },
  };
};

interface IDateTimeWidgetProps extends IWidgetProps<null>, WithStyles<typeof DatePickerStyles> {}

const DateTimeWidget: React.FC<IDateTimeWidgetProps> = ({ value, onChange, disabled, classes }) => {
  const initStart = typeof value === 'string' ? parseInt(value, 10) : 0;
  const [startTime, updateStart] = useState(initStart);

  const onChangeHandler = ({ start }) => {
    const newStart = parseInt(start, 10) || 0;
    updateStart(newStart);
    onChange(newStart.toString());
  };

  return (
    <div className={classes.root}>
      <ExpandableTimeRange
        showRange={false}
        start={startTime}
        onChange={onChangeHandler}
        disabled={disabled}
      />
    </div>
  );
};

const StyledDateTimeWidget = withStyles(DatePickerStyles)(DateTimeWidget);

export default StyledDateTimeWidget;
