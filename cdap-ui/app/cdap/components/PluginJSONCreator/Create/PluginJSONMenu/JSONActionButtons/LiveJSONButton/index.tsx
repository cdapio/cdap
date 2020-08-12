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

import CodeIcon from '@material-ui/icons/Code';
import IconButton from '@material-ui/core/IconButton';
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
    liveJSONButton: {
      '&:focus': {
        outline: 'none',
      },
    },
    clickedButton: {
      backgroundColor: fade(theme.palette.primary.main, 0.2),
    },
  };
};

export const LiveJSONButtonView = ({ classes, liveViewMode, switchToJSONView }) => {
  return (
    <Tooltip
      title="JSON View"
      classes={{
        tooltip: classes.buttonTooltip,
      }}
    >
      <IconButton
        className={classnames(classes.liveJSONButton, {
          [classes.clickedButton]: liveViewMode === LiveViewMode.JSONView,
        })}
        color="primary"
        onClick={switchToJSONView}
        disableRipple
        disableFocusRipple
        data-cy="open-live-view-btn"
      >
        <CodeIcon />
      </IconButton>
    </Tooltip>
  );
};

const LiveJSONButton = withStyles(styles)(LiveJSONButtonView);
export default LiveJSONButton;
