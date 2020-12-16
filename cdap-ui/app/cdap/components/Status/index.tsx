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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import StatusMapper from 'services/StatusMapper';
import StatusIndicator from 'components/Status/StatusIndicator';

const styles = (): StyleRules => {
  return {
    indicator: {
      marginRight: '5px',
    },
    statusText: {
      verticalAlign: 'middle',
    },
  };
};

interface IStatusProps extends WithStyles<typeof styles> {
  status?: string;
}

const StatusView: React.FC<IStatusProps> = ({ classes, status }) => {
  if (!status) {
    return <span className={classes.root}>--</span>;
  }

  const displayName = StatusMapper.lookupDisplayStatus(status);

  return (
    <span className={classes.root}>
      <StatusIndicator status={status} className={classes.indicator} />
      <span className={classes.statusText}>{displayName}</span>
    </span>
  );
};

const Status = withStyles(styles)(StatusView);
export default Status;
