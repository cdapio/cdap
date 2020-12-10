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

import React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';

const styles = (theme): StyleRules => {
  return {
    columnGroup: {
      position: 'relative',
      height: '30px',

      '& > *': {
        position: 'absolute',
        transform: 'translate(-50%, -50%)',
        left: '50%',
        top: '50%',
      },
    },
    line: {
      margin: '2px 0 0 0',
      width: '100%',
      borderWidth: '2px',
      borderColor: theme.palette.grey[400],
    },
    groupLabel: {
      backgroundColor: theme.palette.white[50],
      paddingLeft: '15px',
      paddingRight: '15px',
      width: 'max-content',
    },
  };
};

interface IColumnGroupProps extends WithStyles<typeof styles> {
  gridColumn: string;
}

const ColumnGroupView: React.FC<React.PropsWithChildren<IColumnGroupProps>> = ({
  classes,
  gridColumn,
  children,
}) => {
  const style = {
    gridColumn,
  };
  return (
    <div className={classes.columnGroup} style={style}>
      <hr className={classes.line} />
      <div className={classes.groupLabel}>{children}</div>
    </div>
  );
};

const ColumnGroup = withStyles(styles)(ColumnGroupView);
export default ColumnGroup;
