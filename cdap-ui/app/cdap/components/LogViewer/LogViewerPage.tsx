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
import Heading, { HeadingTypes } from 'components/Heading';
import { match as RouterMatch } from 'react-router';
import { MyProgramApi } from 'api/program';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { humanReadableDate } from 'services/helpers';
import ProgramDataFetcher from 'components/LogViewer/DataFetcher/ProgramDataFetcher';
import LogViewer from 'components/LogViewer';
import If from 'components/If';
import { PROGRAM_STATUSES } from 'services/global-constants';

const HEADER_HEIGHT = '60px';

const styles = (): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    header: {
      height: HEADER_HEIGHT,
      padding: '5px 15px',
    },
    programInfo: {
      display: 'flex',
    },
    programInfoItem: {
      marginRight: '10px',
    },
    infoLabel: {
      marginRight: '5px',
    },
    logViewerContainer: {
      height: `calc(100% - ${HEADER_HEIGHT})`,
      overflow: 'hidden',
    },
  };
};

interface IMatchParams {
  appId: string;
  programType: string;
  programId: string;
  runId: string;
}

interface IStatus {
  starting?: number;
  end?: number;
  status?: string;
}

interface ILogViewerPageProps extends WithStyles<typeof styles> {
  match: RouterMatch<IMatchParams>;
}

export function getLogViewerPageUrl(namespace, appId, programType, programId, runId) {
  return `/cdap/ns/${namespace}/logs/program/${appId}/${programType}/${programId}/${runId}`;
}

const LogViewerPageView: React.FC<ILogViewerPageProps> = ({ classes, match = {} }) => {
  const { appId, programType, programId, runId } = match.params;

  const [run, setRun] = React.useState<IStatus>({});
  const [shouldStopPoll, setShouldStopPoll] = React.useState<boolean>(false);
  const [dataFetcher, setDataFetcher] = React.useState<ProgramDataFetcher>();

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      appId,
      programType,
      programId,
      runId,
    };

    const runStatusPoll$ = MyProgramApi.pollRunStatus(params).subscribe((res) => {
      setRun(res);

      const stopPollCondition =
        [
          PROGRAM_STATUSES.RUNNING,
          PROGRAM_STATUSES.STARTING,
          PROGRAM_STATUSES.STOPPING,
          PROGRAM_STATUSES.PENDING,
        ].indexOf(res.status) === -1;

      if (stopPollCondition) {
        setShouldStopPoll(true);
        runStatusPoll$.unsubscribe();
      }
    });

    setDataFetcher(
      new ProgramDataFetcher({
        namespace: getCurrentNamespace(),
        application: appId,
        programType,
        programName: programId,
        runId,
      })
    );
  }, []);

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <Heading type={HeadingTypes.h4} label="Logs" />
        <div className={classes.programInfo}>
          <div className={classes.programInfoItem}>
            <strong className={classes.infoLabel}>App Name:</strong>
            <span>{appId}</span>
          </div>
          <div className={classes.programInfoItem}>
            <strong className={classes.infoLabel}>Program Name:</strong>
            <span>{programId}</span>
          </div>
          <div className={classes.programInfoItem}>
            <strong className={classes.infoLabel}>Status:</strong>
            <span>{run.status}</span>
          </div>
          <div className={classes.programInfoItem}>
            <strong className={classes.infoLabel}>Start Time:</strong>
            <span>{humanReadableDate(run.starting)}</span>
          </div>
          <div className={classes.programInfoItem}>
            <strong className={classes.infoLabel}>End Time:</strong>
            <span>{humanReadableDate(run.end)}</span>
          </div>
        </div>
      </div>

      <If condition={!!dataFetcher}>
        <div className={classes.logViewerContainer}>
          <LogViewer dataFetcher={dataFetcher} stopPoll={shouldStopPoll} />
        </div>
      </If>
    </div>
  );
};

const LogViewerPage = withStyles(styles)(LogViewerPageView);
export default LogViewerPage;
