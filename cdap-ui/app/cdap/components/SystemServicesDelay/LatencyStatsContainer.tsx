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
the License.
 */

import React from 'react';
import { ILatencyChildProps } from 'components/SystemServicesDelay/LatencyTypes';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import TableHead from '@material-ui/core/TableHead';
import TableCell from '@material-ui/core/TableCell';
import { StyleRules, withStyles, WithStyles } from '@material-ui/core/styles';
const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: 10,
    fontSize: 14,
  },
  body: {
    padding: 10,
    fontSize: 14,
  },
}))(TableCell);
const style = (theme): StyleRules => {
  return {
    row: {
      height: 40,
      '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.grey['600'],
      },
    },
  };
};

interface ILatencyStatsContainerProps extends ILatencyChildProps, WithStyles<typeof style> {}
function LatencyStatsContainerBase({ requestDelayMap, classes }: ILatencyStatsContainerProps) {
  let totalRequestTimeFromClient: number = 0;
  let totalBackendRequestTimeDuration: number = 0;
  const totalRequests = Object.entries(requestDelayMap).length;
  for (const [id, value] of Object.entries(requestDelayMap)) {
    totalRequestTimeFromClient += value.requestTimeFromClient || 0;
    totalBackendRequestTimeDuration += value.backendRequestTimeDuration || 0;
  }
  const avgNetworkDelay = totalRequestTimeFromClient / totalRequests;
  const avgBackendDelay = totalBackendRequestTimeDuration / totalRequests;
  return (
    <Table>
      <TableHead>
        <TableRow>
          <CustomTableCell>Stat</CustomTableCell>
          <CustomTableCell>Value</CustomTableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        <TableRow className={classes.row}>
          <CustomTableCell>Avg Network delay</CustomTableCell>
          <CustomTableCell>
            {avgNetworkDelay < 1000.0
              ? `${avgNetworkDelay.toFixed(1)}ms`
              : `${(avgNetworkDelay / 1000).toFixed(1)}s`}
          </CustomTableCell>
        </TableRow>
        <TableRow className={classes.row}>
          <CustomTableCell>Avg Backend response delay</CustomTableCell>
          <CustomTableCell>
            {avgBackendDelay < 1000.0
              ? `${avgBackendDelay.toFixed(1)}ms`
              : `${(avgBackendDelay / 1000).toFixed(1)}s`}
          </CustomTableCell>
        </TableRow>
      </TableBody>
    </Table>
  );
}

export const LatencyStatsContainer = withStyles(style)(LatencyStatsContainerBase);
