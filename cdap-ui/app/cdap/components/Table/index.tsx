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

import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

const tableCellStyles = (theme) => ({
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
});

const paperStyles = (theme) => ({
  root: {
    minWidth: 400,
    display: 'inline-block',
    height: 'auto',
    marginTop: theme.Spacing(3),
  },
});

const tableRowStyles = (theme) => ({
  root: {
    height: 40,
    '&:nth-of-type(odd)': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
});
const CustomTableWrapper = withStyles(paperStyles)(Paper);
const CustomTableRow = withStyles(tableRowStyles)(TableRow);
const CustomTableCell = withStyles(tableCellStyles)(TableCell);

export {
  Table,
  TableHead,
  CustomTableWrapper as TableWrapper,
  TableBody,
  CustomTableCell as TableCell,
  CustomTableRow as TableRow,
};
