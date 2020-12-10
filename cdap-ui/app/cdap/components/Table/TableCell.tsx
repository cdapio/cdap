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
import { TextAlignProperty } from 'csstype';

const styles = (): StyleRules => {
  return {
    gridCell: {
      borderLeft: 0,
      borderBottom: 0,
      padding: '5px 7px',
      maxWidth: '100%',
      overfllow: 'hidden',
      wordBreak: 'inherit',
      textOverflow: 'ellipsis',
    },
  };
};

interface ITableCellProps extends WithStyles<typeof styles> {
  textAlign?: TextAlignProperty;
}

const TableCellView: React.FC<React.PropsWithChildren<ITableCellProps>> = ({
  classes,
  children,
  textAlign = 'left',
}) => {
  const style: React.CSSProperties = {
    textAlign,
  };
  return (
    <div className={classes.gridCell} style={style}>
      {children}
    </div>
  );
};

const TableCell = withStyles(styles)(TableCellView);
export default TableCell;
