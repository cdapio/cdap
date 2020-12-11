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

import React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import IconSVG from 'components/IconSVG';
import { DURATION_SECONDS } from 'components/Operations';
import classnames from 'classnames';
import moment from 'moment';

const styles = (theme): StyleRules => {
  return {
    root: {
      marginTop: '5px',
      border: `3px solid ${theme.palette.grey[300]}`,
      width: 'max-content',
      borderRadius: '4px',
      display: 'grid',
      gridTemplateColumns: '20px 1fr 20px',
      alignItems: 'stretch',
    },
    arrow: {
      margin: '-1px', // there's an odd gap because of the size of the border of the container
      display: 'grid',
      alignItems: 'center',
      justifyItems: 'center',
      cursor: 'pointer',
      '&:hover': {
        backgroundColor: theme.palette.grey[500],
        color: theme.palette.blue[100],
      },
    },
    disabled: {
      cursor: 'not-allowed',
      '&:hover': {
        backgroundColor: 'initial',
        color: 'inherit',
      },
    },
    flipArrow: {
      transform: 'scaleX(-1)',
    },
    icon: {
      marginTop: '-2px',
    },
    timeText: {
      fontWeight: 'bold',
      textAlign: 'center',
      padding: '3px 10px',
    },
  };
};

interface ITimePickerProps extends WithStyles<typeof styles> {
  next: () => void;
  prev: () => void;
  startTime: number;
  isNextDisabled: boolean;
}

function displayTime(time) {
  return moment(time * 1000).format('M/D h a');
}

const TimePickerView: React.FC<ITimePickerProps> = ({
  classes,
  next,
  prev,
  startTime,
  isNextDisabled,
}) => {
  return (
    <div className={classes.root}>
      <div onClick={prev} className={`${classes.arrow} ${classes.left}`}>
        <IconSVG name="icon-angle-double-left" className={classes.icon} />
      </div>
      <div className={classes.timeText}>
        {displayTime(startTime)} to {displayTime(startTime + DURATION_SECONDS)}
      </div>
      <div
        onClick={next}
        className={classnames(classes.arrow, classes.right, { [classes.disabled]: isNextDisabled })}
      >
        <IconSVG name="icon-angle-double-left" className={`${classes.icon} ${classes.flipArrow}`} />
      </div>
    </div>
  );
};

const TimePicker = withStyles(styles)(TimePickerView);
export default TimePicker;
