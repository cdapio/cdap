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

import React, { useState, useEffect } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Heading, { HeadingTypes } from 'components/Heading';
import T from 'i18n-react';
import Helmet from 'react-helmet';
import { Theme } from 'services/ThemeHelper';
import { MyOperationsApi } from 'api/operations';
import moment from 'moment';
import { parseOperationsResponse } from 'components/Operations/parser';
import JobsTable, { IJobsData } from 'components/Operations/JobsTable';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { NamespacesPickerBase as NamespacesPicker } from 'components/NamespacesPicker';
import TimePicker from 'components/Operations/TimePicker';
import { humanReadableDate } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '15px 50px',
      height: '100%',
    },
    heading: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
    },
    namespacesPicker: {
      '&.namespace-picker': {
        marginTop: 0,
      },
    },
    jobsInfo: {
      marginTop: '15px',
      color: theme.palette.grey[100],
      display: 'grid',
      gridTemplateColumns: '50% 50%',

      '& > div:last-child': {
        textAlign: 'right',
      },
    },
    date: {
      marginRight: '5px',
      marginLeft: '5px',
    },
  };
};

const PREFIX = 'features.Operations';

const DURATION_HOUR = 6;
export const DURATION_SECONDS = DURATION_HOUR * 60 * 60; // 6 hours in seconds

function getInitialTime() {
  const startTime = moment()
    // .startOf('hour')
    .subtract(DURATION_HOUR, 'h')
    .format('x'); // epoch start time
  const start = Math.floor(parseInt(startTime, 10) / 1000);
  return start;
}

const OperationsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const featureName = Theme.featureNames.dashboard;
  const [data, setData] = useState<IJobsData>({
    lastUpdated: Date.now(),
    jobs: [],
  });
  const [monitorNamespaces, setMonitorNamespaces] = useState([]);
  const [startTime, setStartTime] = useState(getInitialTime());
  const [initialStart] = useState(startTime);

  useEffect(() => {
    const params = {
      start: startTime,
      duration: DURATION_SECONDS, // 6 hours in seconds
      namespace: monitorNamespaces.concat([getCurrentNamespace()]),
    };

    MyOperationsApi.getDashboard(params).subscribe((res) => {
      setData({
        lastUpdated: Date.now(),
        jobs: parseOperationsResponse(res),
      });
    });
  }, [monitorNamespaces, startTime]);

  function prev() {
    const newStartTime = startTime - DURATION_SECONDS;
    setStartTime(newStartTime);
  }

  function isNextDisabled() {
    const newStartTime = startTime + DURATION_SECONDS;
    const ONE_HOUR_SECONDS = 60 * 60;
    return newStartTime >= initialStart + ONE_HOUR_SECONDS / 2;
  }

  function next() {
    if (isNextDisabled()) {
      return;
    }
    const newStartTime = startTime + DURATION_SECONDS;
    setStartTime(newStartTime);
  }

  return (
    <div className={classes.root}>
      <Helmet
        title={T.translate(`${PREFIX}.pageTitle`, {
          productName: Theme.productName,
          featureName,
        })}
      />
      <div className={classes.heading}>
        <Heading type={HeadingTypes.h1} label="Jobs" />

        <div>
          <NamespacesPicker
            namespacesPick={monitorNamespaces}
            setNamespacesPick={setMonitorNamespaces}
            className={classes.namespacesPicker}
          />
        </div>
      </div>
      <div>
        <div>Select a 6 hrs time range</div>
        <TimePicker
          next={next}
          prev={prev}
          startTime={startTime}
          isNextDisabled={isNextDisabled()}
        />
      </div>
      <div className={classes.jobsInfo}>
        <div>
          {data.jobs.length} jobs ran between
          <span className={classes.date}>{humanReadableDate(startTime)}</span>
          and
          <span className={classes.date}>
            {humanReadableDate(startTime + DURATION_SECONDS)}
          </span>- <strong>Click one row to view details</strong>
        </div>
        <div>Last updated {humanReadableDate(data.lastUpdated, true)}</div>
      </div>
      <JobsTable data={data} />
    </div>
  );
};

const Operations = withStyles(styles)(OperationsView);
export default Operations;
