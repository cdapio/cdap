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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import Status from 'components/Status';

const styles = (theme): StyleRules => {
  return {
    root: {
      width: '150px',
    },
    title: {
      fontWeight: 600,
      fontSize: '15px',
      color: theme.palette.grey[200],
    },
  };
};

const StatusIndicatorView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  status,
}) => {
  return (
    <div className={classes.root}>
      <div className={classes.title}>Status</div>
      <div>
        <Status status={status} />
      </div>
    </div>
  );
};

const StyledStatusIndicator = withStyles(styles)(StatusIndicatorView);
const StatusIndicator = detailContextConnect(StyledStatusIndicator);
export default StatusIndicator;
