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
        background: theme.palette.white[50],
      },
    },
    targetTable: {
      border: `2px solid ${theme.palette.green[100]}`,
    },
    activeTable: {
      border: `2px solid ${theme.palette.grey[100]}`,
    },
    // had to add this in to fix styling after adding custom renderGridBody method...
    gridBody: {
      '& .grid-row': {
        paddingLeft: '10px',
        paddingRight: '10px',
        borderTop: `1px solid ${theme.palette.grey[500]}`,
        height: '21px',
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
      overflow: 'hidden',
      ' & .table-name': {
        overflow: 'hidden',
        whiteSpace: 'nowrap',
        textOverflow: 'ellipsis',
      },
    },
    tableSubheader: {
      color: theme.palette.grey[100],
      fontWeight: 'normal',
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
  isActive?: boolean;
}

function renderGridHeader(fields: IField[], isTarget: boolean, classes) {
  const count: number = fields.length;
  const tableName = fields[0].dataset;
  return (
    <div className={classes.tableHeader}>
      <div className="table-name">{tableName}</div>
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

function renderGridBody(fields: IField[], tableName: string, activeFields = new Set(), classes) {
  return (
    <div
      className={classes.gridBody}
      id={tableName}
      data-tablename={fields[0].dataset}
      data-namespace={fields[0].namespace}
    >
      {fields.map((field) => {
        const isActiveField = activeFields.has(field.id);
        return <FllField key={field.id} field={field} isActive={isActiveField} />;
      })}
    </div>
  );
}

function FllTable({ tableId, fields, type, isActive, classes }: ITableProps) {
  const GRID_HEADERS = [{ property: 'name', label: tableId }];
  const { target, activeCauseSets, activeImpactSets } = useContext<IContextState>(FllContext);
  const isTarget = type === 'target';

  // get fields that are a direct cause or impact to selected field
  let activeFields = [];
  if (isActive && !isTarget) {
    if (type === 'cause') {
      activeFields = activeCauseSets[tableId];
    } else {
      activeFields = activeImpactSets[tableId];
    }
  }

  const activeFieldIds = new Set(
    activeFields.map((field) => {
      return field.id;
    })
  );

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
      className={classnames(
        classes.table,
        { [classes.targetTable]: isTarget },
        { [classes.activeTable]: isActive }
      )}
      renderGridHeader={renderGridHeader.bind(null, fields, isTarget, classes)}
      renderGridBody={renderGridBody.bind(this, fields, tableId, activeFieldIds, classes)}
    />
  );
}

const StyledFllTable = withStyles(styles)(FllTable);

export default StyledFllTable;
