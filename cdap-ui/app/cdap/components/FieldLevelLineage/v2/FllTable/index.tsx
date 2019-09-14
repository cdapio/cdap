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

import React, { useContext } from 'react';
import SortableStickyGrid from 'components/SortableStickyGrid/index.js';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import createStyles from '@material-ui/core/styles/createStyles';
import T from 'i18n-react';
import classnames from 'classnames';
import { IField } from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import FllField from 'components/FieldLevelLineage/v2/FllTable/FllField';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

// TO DO: Consolidate different fontsizes in ThemeWrapper
const styles = (theme) => {
  return createStyles({
    table: {
      width: '225px',
      border: `1px solid ${theme.palette.grey[300]}`,
      fontSize: '0.92rem',
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
        borderTop: `1px solid ${theme.palette.grey[500]}`,
      },
      ' & .grid-row:hover': {
        backgroundColor: theme.palette.grey[700],
      },
      ' & .grid-row.selected': {
        backgroundColor: theme.palette.yellow[200],
        color: theme.palette.orange[50],
        fontWeight: 'bold',
      },
    },
    tableHeader: {
      borderBottom: `2px solid ${theme.palette.grey[300]}`,
      height: '40px',
      paddingLeft: '10px',
      fontWeight: 'bold',
      fontSize: '1rem',
    },
    tableSubheader: {
      fontWeight: 'normal',
      color: theme.palette.grey[200],
      fontSize: '0.92rem',
    },
    viewLineage: {
      visibility: 'hidden',
      color: theme.palette.blue[300],
    },
  });
};

interface ITableProps extends WithStyles<typeof styles> {
  tableId?: string;
  fields?: IField[];
  type?: string;
}

function renderGridHeader(fields: IField[], isTarget: boolean, classes) {
  const count: number = fields.length;
  const tableName = fields[0].dataset;
  return (
    <div className={classes.tableHeader}>
      <div>{tableName}</div>
      <div className={classes.tableSubheader}>
        {isTarget
          ? T.translate('features.FieldLevelLineage.v2.FllTable.fieldsCount', {
              context: count,
            })
          : T.translate('features.FieldLevelLineage.v2.FllTable.relatedFieldsCount', {
              context: count,
            })}
      </div>
    </div>
  );
}

function renderGridBody(fields: IField[], tableName: string, classes) {
  return (
    <div
      className={classes.gridBody}
      id={tableName}
      data-tablename={fields[0].dataset}
      data-namespace={fields[0].namespace}
    >
      {fields.map((field) => {
        return <FllField key={field.id} field={field} />;
      })}
    </div>
  );
}

function FllTable({ tableId, fields, type, classes }: ITableProps) {
  const GRID_HEADERS = [{ property: 'name', label: tableId }];
  const { target } = useContext<IContextState>(FllContext);
  const isTarget = type === 'target';

  if (!fields || fields.length === 0) {
    return (
      <div>
        {T.translate('features.FieldLevelLineage.v2.FllTable.noRelatedTables', { type, target })}
      </div>
    );
  }

  return (
    <SortableStickyGrid
      key={`cause ${tableId}`}
      entities={fields}
      gridHeaders={GRID_HEADERS}
      className={classnames(classes.table, { [classes.targetTable]: isTarget })}
      renderGridHeader={renderGridHeader.bind(null, fields, isTarget, classes)}
      renderGridBody={renderGridBody.bind(this, fields, tableId, classes)}
    />
  );
}

const StyledFllTable = withStyles(styles)(FllTable);

export default StyledFllTable;
