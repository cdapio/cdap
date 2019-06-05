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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { createStyles } from '@material-ui/core';

const styles = (theme) => {
  return createStyles({
    root: {
      width: '170px',
      border: `1px solid ${theme.palette.grey[300]}`,
      margin: '10px',
    },
    row: {
      borderBottom: `0.5px solid ${theme.palette.grey[500]}`,
      height: '20px',
      paddingLeft: '10px',
      marginRight: '0px',
      marginLeft: '0px',
    },
    tableHeader: {
      borderBottom: `2px solid ${theme.palette.grey[300]}`,
      height: '40px',
      paddingLeft: '10px',
      fontWeight: 'bold',
    },
    tableSubheader: {
      fontWeight: 'normal',
      color: theme.palette.grey[500],
      paddingBottom: 5,
      paddingTop: 3,
    },
  });
};

interface INode {
  id: string;
  name: string;
  group: number;
}

interface ITableProps extends WithStyles<typeof styles> {
  nodes: INode[];
  tableName: string;
}

// TO DO: We can probably replace this component with the SortableStickyGrid - will investigate!

function Table({ nodes, tableName, classes }: ITableProps) {
  return (
    <div className={classes.root} id={`table-${tableName}`}>
      <div className={classes.tableHeader}>
        {tableName}
        <div className={classes.tableSubheader}>{`${nodes.length} fields`}</div>
      </div>
      {nodes.map((node) => {
        return (
          <div id={node.id} key={node.id} className={classes.row}>
            {node.name}
          </div>
        );
      })}
    </div>
  );
}

const StyledTable = withStyles(styles)(Table);
export default StyledTable;
