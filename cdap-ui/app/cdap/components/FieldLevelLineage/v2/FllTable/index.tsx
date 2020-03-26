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
import { IField, ITableInfo } from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import FllField from 'components/FieldLevelLineage/v2/FllTable/FllField';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';
import ExpandableField from 'components/FieldLevelLineage/v2/FllTable/FllExpandableField';
import If from 'components/If';
import FllTableHeader from 'components/FieldLevelLineage/v2/FllTable/FllTableHeader';

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
        alignItems: 'center',
        padding: '0',
        gridTemplateColumns: '1fr auto !important', // To override styles for .grid-row from common.less
      },
      ' & .grid-row:hover': {
        backgroundColor: theme.palette.grey[700],
      },
      ' & .grid-row.selected': {
        backgroundColor: theme.palette.yellow[200],
        color: theme.palette.orange[50],
        fontWeight: 'bold',
        cursor: 'unset',
      },
    },
    viewLineage: {
      visibility: 'hidden',
      color: theme.palette.blue[300],
    },
  });
};

interface ITableProps extends WithStyles<typeof styles> {
  tableId?: string;
  tableInfo?: ITableInfo;
  type?: string;
  isActive?: boolean;
}

function renderGridHeader(
  tableInfo: ITableInfo,
  fields: IField[],
  isTarget: boolean,
  isExpanded: boolean = false
) {
  return (
    <FllTableHeader
      tableInfo={tableInfo}
      fields={fields}
      isTarget={isTarget}
      isExpanded={isExpanded}
    />
  );
}

function renderGridBody(
  fields: IField[],
  tableId: string,
  activeFields = new Set(),
  hasUnrelatedFields: boolean = false,
  isExpanded: boolean = false,
  handleClick: () => void,
  classes
) {
  const namespace = fields[0].namespace;
  return (
    <div
      className={classes.gridBody}
      id={tableId}
      data-tablename={tableId}
      data-namespace={namespace}
    >
      {fields.map((field) => {
        const isActiveField = activeFields.has(field.id);
        return <FllField key={field.id} field={field} isActive={isActiveField} />;
      })}
      <If condition={hasUnrelatedFields}>
        <ExpandableField
          isExpanded={isExpanded}
          handleClick={handleClick}
          tablename={fields[0].dataset}
        />
      </If>
    </div>
  );
}

function FllTable({ tableId, tableInfo, type, isActive, classes }: ITableProps) {
  const GRID_HEADERS = [{ property: 'name', label: tableId }];
  const { target, activeCauseSets, activeImpactSets, handleExpandFields } = useContext<
    IContextState
  >(FllContext);
  let fields = tableInfo ? tableInfo.fields : [];

  if (!fields || fields.length === 0) {
    return (
      <div>
        {T.translate('features.FieldLevelLineage.v2.FllTable.noRelatedTables', { type, target })}
      </div>
    );
  }

  const unrelatedFields = tableInfo.unrelatedFields;
  const fieldCount = tableInfo.fieldCount;
  const isExpanded = tableInfo.isExpanded || false;
  const isTarget = type === 'target';
  const hasUnrelatedFields = fields.length < fieldCount;

  // If there are unrelated fields AND the user has expanded to see all fields
  // render the unrelated fields
  if (unrelatedFields && isExpanded) {
    fields = fields.concat(unrelatedFields);
  }

  // get fields that are a direct cause or impact to selected field
  const getActiveFields = () => {
    let activeFields = [];
    if (isActive && !isTarget) {
      if (type === 'cause' && Object.keys(activeCauseSets).length > 0) {
        activeFields = activeCauseSets[tableId].fields;
      } else if (type === 'impact' && Object.keys(activeImpactSets).length > 0) {
        activeFields = activeImpactSets[tableId].fields;
      }
    }
    return activeFields;
  };

  const activeFieldIds = new Set(
    getActiveFields().map((field) => {
      return field.id;
    })
  );

  const handleClick = () => {
    const namespace = fields[0].namespace;
    const tablename = fields[0].dataset;

    handleExpandFields(namespace, tablename, type);
  };

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
      renderGridHeader={renderGridHeader.bind(
        null,
        tableInfo,
        fields,
        isTarget,
        isExpanded,
        classes
      )}
      renderGridBody={renderGridBody.bind(
        this,
        fields,
        tableId,
        activeFieldIds,
        hasUnrelatedFields,
        isExpanded,
        handleClick,
        classes
      )}
    />
  );
}

const StyledFllTable = withStyles(styles)(FllTable);

export default StyledFllTable;
