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
import ActionButton from 'components/Replicator/Detail/TopPanel/ActionButtons/ActionButton';
import IconSVG from 'components/IconSVG';
import VerticalDivider from 'components/Replicator/Detail/TopPanel/VerticalDivider';
import LogsButton from 'components/Replicator/Detail/TopPanel/ActionButtons/LogsButton';
import StatusIndicator from 'components/Replicator/Detail/TopPanel/ActionButtons/StatusIndicator';
import { PROGRAM_STATUSES } from 'services/global-constants';
import Actions from 'components/Replicator/Detail/TopPanel/ActionButtons/Actions';
import Configure from 'components/Replicator/Detail/TopPanel/ActionButtons/Configure';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      justifyContent: 'flex-end',
    },
    startBtn: {
      color: theme.palette.green[100],
    },
    stopBtn: {
      color: theme.palette.red[100],
    },
  };
};

const ActionButtonsView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  start,
  stop,
  status,
}) => {
  const startDisabled =
    [PROGRAM_STATUSES.RUNNING, PROGRAM_STATUSES.STOPPING, PROGRAM_STATUSES.STARTING].indexOf(
      status
    ) !== -1;

  return (
    <div className={classes.root}>
      <Configure />

      <ActionButton
        icon={<IconSVG name="icon-stop" className={classes.stopBtn} />}
        text="Stop"
        onClick={stop}
        disabled={status !== PROGRAM_STATUSES.RUNNING || status === PROGRAM_STATUSES.STOPPING}
      />

      <ActionButton
        icon={<IconSVG name="icon-play" className={classes.startBtn} />}
        text="Start"
        onClick={start}
        disabled={startDisabled}
      />

      <VerticalDivider />
      <LogsButton />
      <VerticalDivider />
      <StatusIndicator />
      <Actions />
    </div>
  );
};

const StyledActionButtons = withStyles(styles)(ActionButtonsView);
const ActionButtons = detailContextConnect(StyledActionButtons);
export default ActionButtons;
