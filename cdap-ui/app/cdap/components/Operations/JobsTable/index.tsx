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
import { IParsedOperationsData } from 'components/Operations/parser';
import Table from 'components/Table';
import TableHeader from 'components/Table/TableHeader';
import TableRow from 'components/Table/TableRow';
import TableCell from 'components/Table/TableCell';
import TableBody from 'components/Table/TableBody';
import IconSVG from 'components/IconSVG';
import StatusIndicator from 'components/Status/StatusIndicator';
import { humanReadableDate, humanReadableDuration } from 'services/helpers';
import { PIPELINE_TYPE } from 'components/Operations/parser';

const styles = (): StyleRules => {
  return {
    root: {
      height: 'calc(100% - 150px)',
    },
    grid: {
      height: '100%',
      marginTop: '15px',
    },
  };
};

export interface IJobsData {
  lastUpdated: number;
  jobs: IParsedOperationsData[];
}

interface IJobsTableProps extends WithStyles<typeof styles> {
  data: IJobsData;
}

const JobsTitleView: React.FC<IJobsTableProps> = ({ classes, data }) => {
  return (
    <div className={classes.root}>
      <Table columnTemplate="40px 3fr 1fr 1fr 2fr 2fr 1fr" classes={{ grid: classes.grid }}>
        <TableHeader>
          <TableRow>
            <TableCell>
              <IconSVG name="icon-circle" />
            </TableCell>
            <TableCell>Name</TableCell>
            <TableCell>Namespace</TableCell>
            <TableCell>Pipeline type</TableCell>
            <TableCell>Start time</TableCell>
            <TableCell>Duration</TableCell>
            <TableCell>Start method</TableCell>
          </TableRow>
        </TableHeader>

        <TableBody>
          {data.jobs.map((row) => {
            const endTime = row.end ? row.end : Math.floor(Date.now() / 1000);
            const duration = endTime - row.start;
            const namespace = row.namespace;
            const appName = row.application.name;

            let link = window.getHydratorUrl({
              stateName: 'hydrator.detail',
              stateParams: {
                namespace,
                pipelineId: appName,
                runid: row.run,
              },
            });
            let nativeLink = true;
            if (row.pipelineType === PIPELINE_TYPE.replication) {
              link = `/ns/${namespace}/replication/detail/${appName}/monitor`;
              nativeLink = false;
            }

            return (
              <TableRow key={row.run} to={link} nativeLink={nativeLink}>
                <TableCell>
                  <StatusIndicator status={row.status} />
                </TableCell>
                <TableCell>{row.application.name}</TableCell>
                <TableCell>{row.namespace}</TableCell>
                <TableCell>{row.pipelineType}</TableCell>
                <TableCell>{humanReadableDate(row.start)}</TableCell>
                <TableCell>{humanReadableDuration(duration)}</TableCell>
                <TableCell>{row.startMethod.toLowerCase()}</TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
};

const JobsTitle = withStyles(styles)(JobsTitleView);
export default JobsTitle;
