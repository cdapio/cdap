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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@material-ui/icons/KeyboardArrowUp';
import T from 'i18n-react';

export const styles = (theme) => {
  return {
    root: {
      color: theme.palette.blue[200],
      fontSize: '0.92em',
      cursor: 'pointer',
    },
  };
};

interface IExpandableFieldProps extends WithStyles<typeof styles> {
  isExpanded: boolean;
  handleClick: () => void;
  tablename: string;
}

const I18N_PREFIX = 'features.FieldLevelLineage.v2.FllTable.FllExpandableField';

function ExpandableField({ isExpanded, handleClick, tablename, classes }: IExpandableFieldProps) {
  const message = isExpanded
    ? T.translate(`${I18N_PREFIX}.hideFields`)
    : T.translate(`${I18N_PREFIX}.showFields`);
  const arrowIcon = isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />;
  return (
    <div
      className={classnames('grid-row', 'grid-link', classes.root)}
      data-cy={`${isExpanded ? 'hide' : 'show'}-fields-panel-${tablename}`}
      onClick={handleClick}
    >
      {message}
      {arrowIcon}
    </div>
  );
}

const StyledExpandableField = withStyles(styles)(ExpandableField);
export default StyledExpandableField;
