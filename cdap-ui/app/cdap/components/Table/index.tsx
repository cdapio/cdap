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
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Paper from '@material-ui/core/Paper';

const styles = (theme) => {
  return {
    root: {
      display: 'block',
    },
    paper: {
      marginTop: theme.spacing(2),
      width: '100%',
      overflowX: 'auto' as 'auto',
      marginBottom: theme.spacing(2),
    },
  };
};

class CustomTable extends React.Component<WithStyles<typeof styles>> {
  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <Paper className={classes.paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell> Column 1 </TableCell>
                <TableCell> Column 2 </TableCell>
                <TableCell> Column 3 </TableCell>
                <TableCell> Column 4 </TableCell>
                <TableCell> Column 5 </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <TableRow>
                <TableCell> Row 1 Value 1</TableCell>
                <TableCell> Row 1 Value 2</TableCell>
                <TableCell> Row 1 Value 3</TableCell>
                <TableCell> Row 1 Value 4</TableCell>
                <TableCell> Row 1 Value 5</TableCell>
              </TableRow>
              <TableRow>
                <TableCell> Row 2 Value 1</TableCell>
                <TableCell> Row 2 Value 2</TableCell>
                <TableCell> Row 2 Value 3</TableCell>
                <TableCell> Row 2 Value 4</TableCell>
                <TableCell> Row 2 Value 5</TableCell>
              </TableRow>
              <TableRow>
                <TableCell> Row 3 Value 1</TableCell>
                <TableCell> Row 3 Value 2</TableCell>
                <TableCell> Row 3 Value 3</TableCell>
                <TableCell> Row 3 Value 4</TableCell>
                <TableCell> Row 3 Value 5</TableCell>
              </TableRow>
              <TableRow>
                <TableCell> Row 4 Value 1</TableCell>
                <TableCell> Row 4 Value 2</TableCell>
                <TableCell> Row 4 Value 3</TableCell>
                <TableCell> Row 4 Value 4</TableCell>
                <TableCell> Row 4 Value 5</TableCell>
              </TableRow>
              <TableRow>
                <TableCell> Row 5 Value 1</TableCell>
                <TableCell> Row 5 Value 2</TableCell>
                <TableCell> Row 5 Value 3</TableCell>
                <TableCell> Row 5 Value 4</TableCell>
                <TableCell> Row 5 Value 5</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </Paper>
      </div>
    );
  }
}

export default withStyles(styles)(CustomTable);
