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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import createStyles from '@material-ui/core/styles/createStyles';
import T from 'i18n-react';
import classnames from 'classnames';
import { INode } from 'components/FieldLevelLineage/v2/Context/FllContext';

const styles = (theme) => {
  return createStyles({
    table: {
      width: '170px',
      border: `1px solid ${theme.palette.grey[300]}`,
      marginBottom: '10px',
      '& .grid.grid-container': {
        maxHeight: 'none',
      },
    },
    targetTable: {
      border: `2px solid #3cc801`,
    },
    // had to add this in to fix styling after adding custom renderGridBody method...
    gridBody: {
      '& .grid-row': {
        paddingLeft: '10px',
        paddingRight: '10px',
        borderBottom: `1px solid ${theme.palette.grey[500]}`,
      },
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

interface ITableProps extends WithStyles<typeof styles> {
  tableName: string;
  fields: INode[];
  isTarget?: boolean;
}

function renderGridHeader(fields, tableName, classes) {
  const count: number = fields.length;
  return (
    <div className={classes.tableHeader}>
      {tableName}
      <div className={classes.tableSubheader}>
        {T.translate('features.FieldLevelLineage.v2.TableSubheader', { count })}
      </div>
    </div>
  );
}

function renderGridBody(fields, tableName, classes) {
  return (
    <div className={classes.gridBody} id={`${tableName}`}>
      {fields.map((field) => {
        return (
          <div
            className={classnames('grid-row', {
              'grid-link': true,
            })}
            key={field.id}
            id={field.id}
          >
            {field.name}
          </div>
        );
      })}
    </div>
  );
}

function FllTable({ tableName, fields, classes, isTarget = false }: ITableProps) {
  const GRID_HEADERS = [{ property: 'name', label: tableName }];
  return (
    <SortableStickyGrid
      key={`cause ${tableName}`}
      entities={fields}
      gridHeaders={GRID_HEADERS}
      className={classnames(classes.table, { [classes.targetTable]: isTarget })}
      renderGridHeader={renderGridHeader.bind(null, fields, tableName, classes)}
      renderGridBody={renderGridBody.bind(this, fields, tableName, classes)}
    />
  );
}

const StyledFllTable = withStyles(styles)(FllTable);

export default StyledFllTable;
