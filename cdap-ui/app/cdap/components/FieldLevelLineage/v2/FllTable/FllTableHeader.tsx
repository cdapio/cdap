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

import React, { useState, useContext } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import {
  IField,
  getTimeQueryParams,
  ITableInfo,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import { Link } from 'react-router-dom';
import T from 'i18n-react';
import If from 'components/If';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

const styles = (theme): StyleRules => {
  return {
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
      ' &.hovering': {
        backgroundColor: theme.palette.grey[700],
      },
    },
    tableSubheader: {
      color: theme.palette.grey[100],
      fontWeight: 'normal',
    },
  };
};

interface ITableHeaderProps extends WithStyles<typeof styles> {
  tableInfo: ITableInfo;
  fields: IField[];
  isTarget: boolean;
  isExpanded: boolean;
}

function FllTableHeader({
  tableInfo,
  fields,
  isTarget,
  isExpanded = false,
  classes,
}: ITableHeaderProps) {
  const [isHovering, setHoverState] = useState<boolean>(false);
  const { selection, start, end } = useContext<IContextState>(FllContext);

  const timeParams = getTimeQueryParams(selection, start, end);
  const toggleHoverState = (nextState) => {
    setHoverState(nextState);
  };
  const count: number = fields.length;
  const tableName = fields[0].dataset;
  const i18nOptions = { context: count };
  const linkPath = `/ns/${tableInfo.namespace}/datasets/${tableInfo.dataset}/fields${timeParams}`;
  return (
    <div
      className={classnames(classes.tableHeader, { hovering: isHovering && !isTarget })}
      onMouseEnter={toggleHoverState.bind(this, true)}
      onMouseLeave={toggleHoverState.bind(this, false)}
    >
      <div className="table-name">{tableName}</div>

      <If condition={!isHovering || isTarget}>
        <div className={classes.tableSubheader}>
          {isTarget || isExpanded
            ? T.translate('features.FieldLevelLineage.v2.FllTable.fieldsCount', i18nOptions)
            : T.translate('features.FieldLevelLineage.v2.FllTable.relatedFieldsCount', i18nOptions)}
        </div>
      </If>
      <If condition={isHovering}>
        <span data-cy="view-lineage" className={classes.tableSubheader}>
          <Link to={linkPath} className={classes.hoverText} title={tableName}>
            {T.translate('features.FieldLevelLineage.v2.FllTable.FllTableHeader.viewLineage')}
          </Link>
        </span>
      </If>
    </div>
  );
}

const StyledTableHeader = withStyles(styles)(FllTableHeader);

export default StyledTableHeader;
