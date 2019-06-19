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
import SortableStickyGrid from 'components/SortableStickyGrid/index.js';
import withStyles from '@material-ui/core/styles/withStyles';
import createStyles from '@material-ui/core/styles/createStyles';
import T from 'i18n-react';

const styles = (theme) => {
  return createStyles({
    table: {
      width: '170px',
      border: `1px solid ${theme.palette.grey[300]}`,
      margin: '10px',
      '& .grid-row': {
        height: '20px',
      },
      // TO DO: Fix overflow-y and maxHeight of grid-container to get rid of vertical scroll
    },
    tableHeader: {
      borderBottom: `2px solid ${theme.palette.grey[300]}`,
      height: '40px',
      paddingLeft: '10px',
      fontWeight: 'bold',
    },
    tableSubheader: {
      fontWeight: 'normal',
      color: theme.palette.grey[200],
      paddingBottom: 5,
      paddingTop: 3,
    },
  });
};

function renderGridHeader(nodes, tableName, classes) {
  const count: number = nodes.length;
  return (
    <div className={classes.tableHeader}>
      {tableName}
      <div className={classes.tableSubheader}>
        {T.translate('features.FieldLevelLineage.v2.TableSubheader', { count })}
      </div>
    </div>
  );
}

function FllTable({ tableName, fields, classes }) {
  const GRID_HEADERS = [{ property: 'name', label: tableName }];
  return (
    <SortableStickyGrid
      key={`cause ${tableName}`}
      entities={fields}
      gridHeaders={GRID_HEADERS}
      className={classes.table}
      renderGridHeader={renderGridHeader.bind(null, fields, tableName, classes)}
    />
  );
}

const StyledFllTable = withStyles(styles)(FllTable);

export default StyledFllTable;
