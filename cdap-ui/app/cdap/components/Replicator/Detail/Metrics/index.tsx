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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { MyReplicatorApi } from 'api/replicator';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import InputBase from '@material-ui/core/InputBase';
import { humanReadableNumber } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    root: {
      maxWidth: '750px',
      margin: '0 auto',
      paddingBottom: '15px',
    },
    metricsContainer: {
      display: 'flex',
      justifyContent: 'space-between',
    },
    metricCard: {
      textAlign: 'center',
      padding: '0 15px',
    },
    metricsLabel: {
      color: theme.palette.grey[200],
    },
    metricsCount: {
      fontSize: '32px',
    },
    option: {
      minHeight: '32px',
    },
    timeSelectorContainer: {
      position: 'relative',
      height: '40px',

      '& > *': {
        position: 'absolute',
      },
    },
    timeSelector: {
      color: theme.palette.grey[100],
      transform: 'translate(-50%, -50%)',
      top: '50%',
      left: '50%',
      backgroundColor: theme.palette.white[50],
      padding: '5px 25px',

      '& > span': {
        marginRight: '5px',
      },
    },
    line: {
      borderWidth: '3px',
      borderColor: theme.palette.grey[500],
      width: '100%',
      margin: 0,
      top: '50%',
    },
    menu: {
      // This is needed because there is no way to overwrite the parent portal z-index for the menu dropdown.
      // The parent container has a z-index set as an inline style.
      // The property name in here is in string, because typescript complaints for zIndex with !important.
      'z-index': '1301 !important',
    },
  };
};

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

function aggregateValue(seriesData) {
  return seriesData.reduce((acc, curr) => acc + curr.value, 0);
}

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

const MetricsView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  targetPluginInfo,
  name,
}) => {
  const [timeRange, setTimeRange] = React.useState(timeRangeOptions[2]); // default to last 24 hours
  const [inserts, setInserts] = React.useState(0);
  const [updates, setUpdates] = React.useState(0);
  const [deletes, setDeletes] = React.useState(0);
  const [currentPoll, setCurrentPoll] = React.useState(null);

  const updateMap = {
    insert: setInserts,
    update: setUpdates,
    delete: setDeletes,
  };

  function pollMetrics() {
    if (!targetPluginInfo) {
      return;
    }

    unsubscribePoll();

    const tags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
    };

    const tagsParams = MetricsQueryHelper.tagsToParams(tags);

    const targetStageName = targetPluginInfo.name;
    const metrics = ['insert', 'update', 'delete']
      .map((metric) => {
        return `metric=user.${targetStageName}.dml.${metric}`;
      })
      .join('&');

    const start = `start=now-${timeRange.value}`;
    const end = 'end=now';
    const aggregate = 'aggregate=false';
    const resolution = 'resolution=auto';

    const queryParams = [start, end, aggregate, resolution, tagsParams, metrics].join('&');

    // TODO: optimize polling
    // Don't poll when status is not running - only do a single request
    const poll = MyReplicatorApi.pollMetrics({ queryParams }).subscribe(
      (res) => {
        res.series.forEach((metric) => {
          const value = aggregateValue(metric.data);
          const metricName = metric.metricName.split('.');
          const metricType = metricName[metricName.length - 1];

          updateMap[metricType](value);
        });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
      }
    );

    setCurrentPoll(poll);
  }

  function unsubscribePoll() {
    if (currentPoll && currentPoll.unsubscribe) {
      currentPoll.unsubscribe();
    }
  }

  function handleTimeRangeChange(e) {
    const selected = timeRangeOptions.find((option) => option.value === e.target.value);

    if (!selected) {
      return;
    }

    setTimeRange(selected);
  }

  React.useEffect(() => {
    pollMetrics();

    return () => {
      unsubscribePoll();
    };
  }, [targetPluginInfo, timeRange.value]);

  return (
    <div className={classes.root}>
      <div className={classes.timeSelectorContainer}>
        <hr className={classes.line} />
        <div className={classes.timeSelector}>
          <span>Displaying</span>
          <Select
            value={timeRange.value}
            onChange={handleTimeRangeChange}
            input={<CustomizedInput />}
            MenuProps={{
              className: classes.menu,
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
        </div>
      </div>

      <div className={classes.metricsContainer}>
        <div className={classes.metricCard}>
          <strong className={classes.metricsLabel}>No. of inserts</strong>
          <div className={classes.metricsCount}>{humanReadableNumber(inserts)}</div>
        </div>

        <div className={classes.metricCard}>
          <strong className={classes.metricsLabel}>No. of deletes</strong>
          <div className={classes.metricsCount}>{humanReadableNumber(deletes)}</div>
        </div>

        <div className={classes.metricCard}>
          <strong className={classes.metricsLabel}>No. of updates</strong>
          <div className={classes.metricsCount}>{humanReadableNumber(updates)}</div>
        </div>
      </div>
    </div>
  );
};

const StyledMetrics = withStyles(styles)(MetricsView);
const Metrics = detailContextConnect(StyledMetrics);
export default Metrics;
