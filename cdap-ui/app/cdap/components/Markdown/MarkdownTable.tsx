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
import Paper from '@material-ui/core/Paper';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

/**
 * This is crazy but we get
 *
 * <thead>
 *  <tr>
 *    <th>heading1</th>
 *    <th>heading2</th>
 *    <th>heading4</th>
 *  </tr>
 * </thead>
 * <tbody>
 *  <tr>
 *    <td>value1</td>
 *    <td>value2</td>
 *    <td>value3</td>
 *  </tr>
 * </tbody
 *
 * but as jsx. What we are doing here is iterating over children and extracting values from th and td
 * and wrapping them around material-ui elements.
 */

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

const styles = (theme) => ({
  root: {
    minWidth: 400,
    display: 'inline-block',
    height: 'auto',
    marginTop: theme.Spacing(3),
  },
  table: {
    minWidth: 700,
  },
  row: {
    height: 40,
    '&:nth-of-type(odd)': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
});
interface IMarkdownWithoutStylesProps extends WithStyles<typeof styles> {
  children: React.ReactNode;
}

const MarkdownWithoutStyles: React.SFC<IMarkdownWithoutStylesProps> = ({ children, classes }) => {
  const flatFilterMap = (child, nodeType = null) =>
    child
      .flat()
      .filter((c) => {
        if (!c) {
          return false;
        }
        if (!nodeType) {
          return c;
        }
        if (nodeType && c.type === nodeType) {
          return c;
        }
      })
      .map((nodeTypeNodes) =>
        typeof nodeTypeNodes === 'string' ? nodeTypeNodes : nodeTypeNodes.props.children
      );
  return (
    <div>
      <Paper className={classes.root}>
        <Table>
          <TableHead>
            <TableRow className={classes.row}>
              {flatFilterMap(flatFilterMap(flatFilterMap(children, 'thead'), 'tr'))
                .flat()
                .map((head, i) => (
                  <CustomTableCell key={`table-head-cell-${i}`}>{head} </CustomTableCell>
                ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {flatFilterMap(flatFilterMap(children, 'tbody'), 'tr')
              .map((tr) => tr.filter((t) => t))
              .map((row, i) => {
                return (
                  <TableRow className={classes.row} key={`tr-${i}`}>
                    {row.map((r, j) => {
                      let cellValue;
                      if (r.props.children && r.props.children.length > 1) {
                        // This usually means the cell value has more than just text (code or someother markdown element)
                        cellValue = flatFilterMap(r.props.children)
                          .flat()
                          .join('');
                      } else {
                        cellValue = r.props.children ? r.props.children.join() : null;
                      }
                      return <CustomTableCell key={`table-cell-${j}`}>{cellValue}</CustomTableCell>;
                    })}
                  </TableRow>
                );
              })}
          </TableBody>
        </Table>
      </Paper>
    </div>
  );
};

const MarkdownTable = withStyles(styles)(MarkdownWithoutStyles);
export { MarkdownTable };
