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

import React from 'react';
import T from 'i18n-react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { Consumer } from '../Context/FllContext';

interface IHeaderProps extends WithStyles<typeof styles> {
  type: string;
  first: number;
  total: number;
}

const styles = (theme) => {
  return {
    root: {
      color: `${theme.palette.grey[200]}`,
      height: 70, // this is closer to 75 in the design
      '& .target': {
        fontSize: '1.25rem',
      },
    },
    subHeader: {
      borderBottom: `2px solid ${theme.palette.grey[200]}`,
    },
  };
};

function FllHeader({ type, first, total, classes }: IHeaderProps) {
  return (
    <Consumer>
      {({ numTables, target }) => {
        let last;
        if (type === ('impact' || 'cause')) {
          last = first + numTables - 1 <= total ? first + numTables - 1 : total;
        } else {
          last = total;
        }

        const header =
          type === 'target'
            ? T.translate('features.FieldLevelLineage.v2.FllHeader.TargetHeader')
            : T.translate('features.FieldLevelLineage.v2.FllHeader.RelatedHeader', {
                type,
                target,
              });
        const options = { first, last, total };
        const subHeader =
          type === 'target'
            ? T.translate('features.FieldLevelLineage.v2.FllHeader.TargetSubheader', options)
            : T.translate('features.FieldLevelLineage.v2.FllHeader.RelatedSubheader', options);

        return (
          <div className={classes.root}>
            <div className={type}>{header}</div>
            <div className={classes.subHeader}>{subHeader}</div>
          </div>
        );
      }}
    </Consumer>
  );
}
const StyledFllHeader = withStyles(styles)(FllHeader);

export default StyledFllHeader;
