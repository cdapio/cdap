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
import { createStyles } from '@material-ui/styles';

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
  };
};

interface IFieldProps extends WithStyles<typeof styles> {
  field: INode;
  isTarget: boolean;
  activeField: string;
  clickFieldHandler: (event: React.MouseEvent<HTMLInputElement>) => void;
}

function FllField({ field, isTarget = false, clickFieldHandler, classes }: IFieldProps) {
  const [isHovering, setHoverState] = useState<boolean>(false);
  const toggleHoverState = () => {
    setHoverState(!isHovering);
  };

  return (
    <div
      onClick={isTarget ? clickFieldHandler : undefined}
      onMouseOver={toggleHoverState}
      onMouseLeave={toggleHoverState}
      className={classnames('grid-row', 'grid-link', classes.root)}
      key={field.id}
      id={field.id}
    >
      {field.name}
      {isHovering && <span>{isTarget ? 'View' : 'View lineage'}</span>}
    </div>
  );
}

const StyledFllField = withStyles(styles)(FllField);

export default StyledFllField;
