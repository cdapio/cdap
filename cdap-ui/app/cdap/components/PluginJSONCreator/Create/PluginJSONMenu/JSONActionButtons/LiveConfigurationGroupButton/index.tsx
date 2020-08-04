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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';

import IconButton from '@material-ui/core/IconButton';
import ImportContactsIcon from '@material-ui/icons/ImportContacts';
import { LiveViewMode } from 'components/PluginJSONCreator/Create/PluginJSONMenu';
import Tooltip from '@material-ui/core/Tooltip';
import classnames from 'classnames';
import { fade } from '@material-ui/core/styles/colorManipulator';

const styles = (theme): StyleRules => {
  return {
    buttonTooltip: {
      fontSize: '13px',
      backgroundColor: theme.palette.grey[50],
    },
    liveConfigurationGroupButton: {
      '&:focus': {
        outline: 'none',
      },
    },
    clickedButton: {
      backgroundColor: fade(theme.palette.primary.main, 0.2),
    },
  };
};

const LiveConfigurationGroupButtonView = ({
  classes,
  liveViewMode,
  switchToConfigurationGroupsView,
}) => {
  return (
    <Tooltip
      title={'Configuration Groups View'}
      classes={{
        tooltip: classes.buttonTooltip,
      }}
    >
      <div>
        <IconButton
          className={classnames(classes.liveConfigurationGroupButton, {
            [classes.clickedButton]: liveViewMode === LiveViewMode.ConfigurationGroupsView,
          })}
          color="primary"
          onClick={switchToConfigurationGroupsView}
          disableRipple
          disableFocusRipple
        >
          <ImportContactsIcon />
        </IconButton>
      </div>
    </Tooltip>
  );
};

const LiveConfigurationGroupButton = withStyles(styles)(LiveConfigurationGroupButtonView);
export default LiveConfigurationGroupButton;
