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
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    gridRow: {
      display: 'grid',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
      alignContent: 'center',
    },
    hover: {
      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },
    },
  };
};

interface ITableRowProps extends WithStyles<typeof styles> {
  columnTemplate?: string;
  hover?: boolean;
  alignItems?: string;
}

const TableRowView: React.FC<React.PropsWithChildren<ITableRowProps>> = ({
  classes,
  children,
  columnTemplate,
  alignItems = 'center',
  hover = true,
}) => {
  const style = {
    gridTemplateColumns: columnTemplate,
    alignItems,
  };
  return (
    <div className={classnames(classes.gridRow, { [classes.hover]: hover })} style={style}>
      {children}
    </div>
  );
};

const TableRow = withStyles(styles)(TableRowView);
export default TableRow;
