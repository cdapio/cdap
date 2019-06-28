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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { humanReadableNumber } from 'services/helpers';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    error: {
      '& > h3, & > h2': {
        color: theme.palette.red[100],
      },
    },
  };
};

interface IStatProps extends WithStyles<typeof styles> {
  type: string;
  stat: number;
}

const StatView: React.FC<IStatProps> = ({ classes, type, stat }) => {
  return (
    <div className={classnames({ [classes.error]: type === 'ERROR' })}>
      <h3 className="text-center">{type}</h3>
      <h2 className="text-center">{humanReadableNumber(stat, 'DECIMAL')}</h2>
    </div>
  );
};

const Stat = withStyles(styles)(StatView);
export default Stat;
