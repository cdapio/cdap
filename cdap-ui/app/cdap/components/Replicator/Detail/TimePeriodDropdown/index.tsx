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

import React, { useContext } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { DetailContext } from 'components/Replicator/Detail';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import InputBase from '@material-ui/core/InputBase';

const styles = (): StyleRules => {
  return {
    select: {
      marginLeft: '5px',
    },
    selectRoot: {
      textDecoration: 'underline',
      marginTop: '2px',
    },
    menu: {
      // This is needed because there is no way to overwrite the parent portal z-index for the menu dropdown.
      // The parent container has a z-index set as an inline style.
      // The property name in here is in string, because typescript complaints for zIndex with !important.
      'z-index': '1301 !important',
    },
  };
};

interface ITimeRange {
  value: string;
  label: string;
}

const timeRangeOptions: ITimeRange[] = [
  {
    value: '1h',
    label: 'last 1 hour',
  },
  {
    value: '12h',
    label: 'last 12 hours',
  },
  {
    value: '24h',
    label: 'last 24 hours',
  },
  {
    value: '7d',
    label: 'last 7 days',
  },
];

const CustomizedInput = withStyles((theme) => {
  return {
    root: {
      color: theme.palette.grey[100],
      fontSize: '1rem',
    },
    input: {
      '&:focus': {
        backgroundColor: 'transparent',
      },
    },
  };
})(InputBase);

const TimePeriodDropdownView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { timeRange, setTimeRange } = useContext(DetailContext);

  function handleTimeRangeChange(e) {
    setTimeRange(e.target.value);
  }

  return (
    <Select
      className={classes.select}
      value={timeRange}
      onChange={handleTimeRangeChange}
      input={<CustomizedInput />}
      MenuProps={{
        className: classes.menu,
      }}
      classes={{
        root: classes.selectRoot,
      }}
    >
      {timeRangeOptions.map((option) => {
        return (
          <MenuItem key={option.value} value={option.value} className={classes.option}>
            {option.label}
          </MenuItem>
        );
      })}
    </Select>
  );
};

const TimePeriodDropdown = withStyles(styles)(TimePeriodDropdownView);
export default TimePeriodDropdown;
