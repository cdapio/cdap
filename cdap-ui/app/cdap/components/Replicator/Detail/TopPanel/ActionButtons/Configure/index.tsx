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
import { PROGRAM_STATUSES } from 'services/global-constants';
import IconSVG from 'components/IconSVG';
import ConfigureModeless from 'components/Replicator/Detail/TopPanel/ActionButtons/Configure/ConfigureModeless';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    icon: {
      transform: 'rotate(90deg)',
    },
    modelessContainer: {
      position: 'relative',
    },
    actionButtonRootOverride: {
      width: '70px',
    },
  };
};

const ConfigureView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  status,
}) => {
  const [showConfigure, setShowConfigure] = React.useState(false);

  function toggleConfigure() {
    setShowConfigure(!showConfigure);
  }

  return (
    <div className={classes.root}>
      <ActionButton
        icon={<IconSVG name="icon-sliders" className={classes.icon} />}
        text="Configure"
        onClick={toggleConfigure}
        disabled={status === PROGRAM_STATUSES.RUNNING}
        classes={{
          root: classes.actionButtonRootOverride,
        }}
      />

      <If condition={showConfigure}>
        <div className={classes.modelessContainer}>
          <ConfigureModeless onToggle={toggleConfigure} />
        </div>
      </If>
    </div>
  );
};

const StyledConfigure = withStyles(styles)(ConfigureView);
const Configure = detailContextConnect(StyledConfigure);
export default Configure;
