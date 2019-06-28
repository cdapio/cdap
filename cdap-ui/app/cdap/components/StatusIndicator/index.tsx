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
import StatusMapper from 'services/StatusMapper';
import IconSVG from 'components/IconSVG';

const styles = (theme): StyleRules => {
  return {
    'status-blue': {
      color: theme.palette.blue[100],
    },
    'status-light-green': {
      color: theme.palette.green[50],
    },
    'status-light-red': {
      color: theme.palette.red[100],
    },
    'status-light-grey': {
      color: theme.palette.grey[200],
    },
    label: {
      marginLeft: '5px',
    },
  };
};

interface IStatusIndicatorProps extends WithStyles<typeof styles> {
  status: string;
}

const StatusIndicatorView: React.SFC<IStatusIndicatorProps> = ({ status, classes }) => {
  const statusLabel = StatusMapper.lookupDisplayStatus(status);
  const statusCSSClass = StatusMapper.getStatusIndicatorClass(statusLabel);
  const statusIcon = StatusMapper.getStatusIndicatorIcon(statusLabel);

  return (
    <span>
      <span className={classes[statusCSSClass]}>
        <IconSVG name={statusIcon} />
      </span>
      <span className={classes.label}>{statusLabel}</span>
    </span>
  );
};

const StatusIndicator = withStyles(styles)(StatusIndicatorView);
export default StatusIndicator;
