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

import React, { useContext, useEffect, useState } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { DetailContext } from 'components/Replicator/Detail';
import { humanReadableDate, humanReadableDuration } from 'services/helpers';
import { PROGRAM_STATUSES } from 'services/global-constants';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      padding: '0 15px',
      fontSize: '16px',
    },
    title: {
      fontWeight: 500,
      color: theme.palette.grey[200],
    },
  };
};

const RUNNING_STATUSES = [
  PROGRAM_STATUSES.RUNNING,
  PROGRAM_STATUSES.STOPPING,
  PROGRAM_STATUSES.STARTING,
  PROGRAM_STATUSES.PENDING,
];

const RunInfoView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { startTime, endTime, status } = useContext(DetailContext);
  const [duration, setDuration] = useState<number>(getDuration());

  function getDuration() {
    const currentEndTime = endTime ? endTime : Math.floor(Date.now() / 1000);
    return currentEndTime - startTime;
  }

  useEffect(() => {
    let interval;
    if (RUNNING_STATUSES.indexOf(status) !== -1) {
      interval = setInterval(() => {
        setDuration(getDuration());
      }, 1000);
    } else {
      setDuration(getDuration());
    }

    return () => {
      clearTimeout(interval);
    };
  }, [status, startTime, endTime]);

  return (
    <div className={classes.root}>
      <div>
        <div className={classes.title}>Start time</div>
        <div>{humanReadableDate(startTime)}</div>
      </div>

      <div>
        <div className={classes.title}>Duration</div>
        <div>{humanReadableDuration(duration) || '--'}</div>
      </div>
    </div>
  );
};

const RunInfo = withStyles(styles)(RunInfoView);
export default RunInfo;
