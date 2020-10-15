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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { ILogResponse, LogLevel } from 'components/LogViewer/types';
import moment from 'moment';
import { logsTableGridStyle } from 'components/LogViewer';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  const tableStyle = logsTableGridStyle(theme);
  return {
    root: {
      ...tableStyle.row,

      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },
    },
    cell: tableStyle.cell,
    warn: {
      color: theme.palette.yellow[50],
    },
    error: {
      color: theme.palette.red[100],
    },
  };
};

interface ILogRowProps extends WithStyles<typeof styles> {
  logObj: ILogResponse;
}

const TIMESTAMP_FORMAT = 'L H:mm:ss';

const LogRowView: React.FC<ILogRowProps> = ({ classes, logObj }) => {
  const timeDate = new Date(logObj.log.timestamp);
  const displayTime = moment(timeDate).format(TIMESTAMP_FORMAT);

  const logLevel = logObj.log.logLevel;

  return (
    <div className={classes.root} data-cy="log-viewer-row">
      <div className={classes.cell}>{displayTime}</div>
      <div
        className={classnames(classes.cell, {
          [classes.warn]: logLevel === LogLevel.WARN,
          [classes.error]: logLevel === LogLevel.ERROR,
        })}
      >
        {logLevel}
      </div>
      <div className={classes.cell} data-cy="log-message">
        {logObj.log.message}
      </div>
    </div>
  );
};

const StyledLogRow = withStyles(styles)(LogRowView);
const LogRow = React.memo(StyledLogRow);
export default LogRow;
