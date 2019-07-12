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
import { IField } from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import T from 'i18n-react';
import If from 'components/If';
import IconButton from '@material-ui/core/IconButton';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

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
    },
    targetView: {
      color: theme.palette.blue[200],
      textAlign: 'right',
    },
    viewDropdown: {
      padding: 0,
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
    handleViewCauseImpact,
    handleReset,
  } = useContext<IContextState>(FllContext);

  const toggleHoverState = () => {
    setHoverState(!isHovering);
  };
  const isTarget = field.type === 'target';
  return (
    <div
      onClick={isTarget && !showingOneField ? handleFieldClick : undefined}
      onMouseEnter={toggleHoverState}
      onMouseLeave={toggleHoverState}
      className={classnames('grid-row', 'grid-link', classes.root)}
      id={field.id}
    >
      {field.name}
      <If condition={isHovering && !isTarget}>
        <span className={classes.hoverText}>
          {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.viewLineage')}
        </span>
      </If>
      <If condition={field.id === activeField && isTarget && !showingOneField}>
        <span className={classes.targetView} onClick={handleViewCauseImpact}>
          {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.viewDropdown')}
          <IconButton className={classes.viewDropdown}>
            <KeyboardArrowDownIcon />
          </IconButton>
        </span>
      </If>
      <If condition={field.id === activeField && isTarget && showingOneField}>
        <span className={classes.targetView} onClick={handleReset}>
          {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.resetLineage')}
        </span>
      </If>
    </div>
  );
}

const StyledFllField = withStyles(styles)(FllField);

export default StyledFllField;
