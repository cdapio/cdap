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

import React, { useState } from 'react';
import { INode } from 'components/FieldLevelLineage/v2/Context/FllContext';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import T from 'i18n-react';

const styles = (theme) => {
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
        fontWeight: 'bold' as 'bold',
      },
    },
    hoverText: {
      color: theme.palette.blue[200],
    },
    targetView: {
      color: theme.palette.blue[200],
      textAlign: 'right' as 'right',
    },
    viewDropdown: {
      paddingLeft: '3px',
    },
  };
};

interface IFieldProps extends WithStyles<typeof styles> {
  field: INode;
  isTarget: boolean;
  activeField: string;
  clickFieldHandler: (event: React.MouseEvent<HTMLDivElement>) => void;
  viewCauseImpactHandler?: (event: React.MouseEvent<HTMLDivElement>) => void;
  showingOneField?: boolean;
  resetHandler?: (event: React.MouseEvent<HTMLDivElement>) => void;
}

function FllField({
  field,
  isTarget = false,
  clickFieldHandler,
  activeField,
  viewCauseImpactHandler,
  showingOneField,
  resetHandler,
  classes,
}: IFieldProps) {
  const [isHovering, setHoverState] = useState<boolean>(false);
  const toggleHoverState = () => {
    setHoverState(!isHovering);
  };

  return (
    <div
      onClick={isTarget ? clickFieldHandler : undefined}
      onMouseEnter={toggleHoverState}
      onMouseLeave={toggleHoverState}
      className={classnames('grid-row', 'grid-link', classes.root)}
      id={field.id}
    >
      {field.name}
      {isHovering &&
        !isTarget && (
          <span className={classes.hoverText}>
            {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.viewLineage')}
          </span>
        )}
      {field.id === activeField &&
        isTarget &&
        !showingOneField && (
          <span className={classes.targetView} onClick={viewCauseImpactHandler}>
            {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.viewDropdown')}
            <span className={classnames('fa', 'fa-chevron-down', classes.viewDropdown)} />
          </span>
        )}
      {field.id === activeField &&
        isTarget &&
        showingOneField && (
          <span className={classes.targetView} onClick={resetHandler}>
            {T.translate('features.FieldLevelLineage.v2.FllTable.FllField.resetLineage')}
          </span>
        )}
    </div>
  );
}

const StyledFllField = withStyles(styles)(FllField);

export default StyledFllField;
