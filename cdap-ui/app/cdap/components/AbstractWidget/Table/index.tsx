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

import React from 'react';
import createStyles from '@material-ui/core/styles/createStyles';
import makeStyles from '@material-ui/core/styles/makeStyles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

/**
 * Objective of this wrapper table component are two things,
 *
 * 1. Ease of use of table component (simple api)
 * 2. Customization we add on top.
 *    - Sparse/Dense table
 *    - Pagination support
 *    - Rows per page
 *    - Styling changes
 *    - Sub header/Sub row?
 * Ideally we shouldn't need to have this loginc scattered across all the tables we use.
 * The styling and the behavior should be consistent.
 */

// @ts-ignore
const useStyles = makeStyles(() => {
  return createStyles({
    root: {
      width: '100%',
      overflowX: 'auto',
    },
  });
});

interface ICustomTableBaseEntities {
  id: string;
}

interface ICustomTableEntitiesProps<p extends ICustomTableBaseEntities> {
  entities: p;
}

interface ICustomTableProps<e extends ICustomTableBaseEntities> {
  entities: ICustomTableEntitiesProps<e>;
}

function CustomTable<P extends ICustomTableBaseEntities>(props: ICustomTableProps<P>) {
  const classes = useStyles();
  return (
    <Paper className={classes.root}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell align="center">Column 1</TableCell>
            <TableCell align="center">Column 2</TableCell>
            <TableCell align="center">Column 3</TableCell>
            <TableCell align="center">Column 4</TableCell>
            <TableCell align="center">Column 5</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell align="center">Row1 Cell 1</TableCell>
            <TableCell align="center">Row1 Cell 2</TableCell>
            <TableCell align="center">Row1 Cell 3</TableCell>
            <TableCell align="center">Row1 Cell 4</TableCell>
            <TableCell align="center">Row1 Cell 5</TableCell>
          </TableRow>
          <TableRow>
            <TableCell align="center">Row2 Cell 1</TableCell>
            <TableCell align="center">Row2 Cell 2</TableCell>
            <TableCell align="center">Row2 Cell 3</TableCell>
            <TableCell align="center">Row2 Cell 4</TableCell>
            <TableCell align="center">Row2 Cell 5</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </Paper>
  );
}

export default CustomTable;
