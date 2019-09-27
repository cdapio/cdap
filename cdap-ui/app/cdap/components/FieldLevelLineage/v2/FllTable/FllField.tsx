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

import React, { useState, useContext } from 'react';
import {
  IField,
  getTimeQueryParams,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import T from 'i18n-react';
import If from 'components/If';
import { Link } from 'react-router-dom';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';
import FllMenu from 'components/FieldLevelLineage/v2/FllTable/FllMenu';

const styles = (theme): StyleRules => {
  return {
    root: {
      paddingLeft: '10px',
      paddingRight: '10px',
      borderTop: `1px solid ${theme.palette.grey[500]}`,
      ' & .grid-row:hover': {
        backgroundColor: theme.palette.grey[700],
      },
      ' & .grid-row:selected': {
        backgroundColor: theme.palette.yellow[200],
        color: theme.palette.orange[50],
        fontWeight: 'bold',
      },
    },
    hoverText: {
      color: theme.palette.blue[200],
      paddingLeft: '30px',
    },
    targetView: {
      paddingLeft: '55px',
      color: theme.palette.blue[200],
    },
  };
};

interface IFieldProps extends WithStyles<typeof styles> {
  field: IField;
}

function FllField({ field, classes }: IFieldProps) {
  const [isHovering, setHoverState] = useState<boolean>(false);
  const {
    activeField,
    showingOneField,
    handleFieldClick,
    handleReset,
    selection,
    start,
    end,
  } = useContext<IContextState>(FllContext);

  const timeParams = getTimeQueryParams(selection, start, end);

  // TO DO: Update linkPath once v1 and v2 FLL UI's are switched

  const linkPath = `/ns/${field.namespace}/datasets/${
    field.dataset
  }/fll-experiment${timeParams}&field=${field.name}`;

  const toggleHoverState = (nextState) => {
    setHoverState(nextState);
  };
  const isTarget = field.type === 'target';
  return (
    <div
      onClick={isTarget && !showingOneField ? handleFieldClick : undefined}
      onMouseEnter={toggleHoverState.bind(this, true)}
      onMouseLeave={toggleHoverState.bind(this, false)}
      className={classnames('grid-row', 'grid-link', classes.root)}
      id={field.id}
      data-fieldname={field.name}
      data-hovering={isHovering}
      data-target={isTarget}
    >
      {field.name}
      <If condition={isHovering && !isTarget}>
        <span data-cy="view-lineage">
          <Link to={linkPath} className={classes.hoverText} title={field.name}>
            {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.viewLineage')}
          </Link>
        </span>
      </If>
      <If condition={activeField.id && field.id === activeField.id && isTarget && !showingOneField}>
        <FllMenu />
      </If>
      <If condition={activeField.id && field.id === activeField.id && isTarget && showingOneField}>
        <span className={classes.targetView} onClick={handleReset} data-cy="reset-lineage">
          {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.resetLineage')}
        </span>
      </If>
    </div>
  );
}

const StyledFllField = withStyles(styles)(FllField);

export default StyledFllField;
