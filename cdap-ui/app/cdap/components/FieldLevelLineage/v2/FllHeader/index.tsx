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
import T from 'i18n-react';
import classnames from 'classnames';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

interface IHeaderProps extends WithStyles<typeof styles> {
  type: string;
  total: number;
}

const styles = (theme): StyleRules => {
  return {
    root: {
      color: `${theme.palette.grey[50]}`,
      height: 70, // this is closer to 75 in the design
      '& .target': {
        fontSize: '14px',
        fontWeight: 'bold',
      },
    },
    header: {
      overflow: 'hidden',
      textOverflow: 'ellipsis',
      whiteSpace: 'nowrap',
    },
    subHeader: {
      borderBottom: `2px solid ${theme.palette.grey[200]}`,
    },
  };
};

function FllHeader({ type, total, classes }: IHeaderProps) {
  const { firstCause, firstImpact, firstField, target, numTables } = useContext<IContextState>(
    FllContext
  );
  let last;
  let first;
  let header;

  const isTarget = type === 'target';

  if (type === 'impact') {
    first = firstImpact;
    header = T.translate('features.FieldLevelLineage.v2.FllHeader.OutputHeader', { target });
  } else {
    first = firstCause;
    header = isTarget
      ? T.translate('features.FieldLevelLineage.v2.FllHeader.TargetHeader')
      : T.translate('features.FieldLevelLineage.v2.FllHeader.InputHeader', { target });
  }

  last = first + numTables - 1 <= total ? first + numTables - 1 : total;

  if (isTarget) {
    last = total;
  }

  const options = { first, last, total, context: total };
  let subHeader;

  if (total === 0) {
    subHeader = T.translate('features.FieldLevelLineage.v2.FllHeader.NoRelatedSubheader');
  } else {
    subHeader =
      isTarget && total > 0
        ? T.translate('features.FieldLevelLineage.v2.FllHeader.TargetSubheader', options)
        : T.translate('features.FieldLevelLineage.v2.FllHeader.RelatedSubheader', options);
  }

  return (
    <div className={classes.root}>
      <div className={classnames(classes.header, { target: isTarget })}>{header}</div>
      <div className={classes.subHeader}>{subHeader}</div>
    </div>
  );
}
const StyledFllHeader = withStyles(styles)(FllHeader);

export default StyledFllHeader;
