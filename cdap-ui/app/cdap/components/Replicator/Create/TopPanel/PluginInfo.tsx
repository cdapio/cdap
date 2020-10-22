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
import { objectQuery } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    root: {
      lineHeight: 1.2,
    },
    type: {
      color: theme.palette.grey[200],
    },
    name: {
      fontSize: '14px',
    },
  };
};

// TODO: refactor props when types are extracted to separate file
interface IPluginInfoProps extends WithStyles<typeof styles> {
  type: string;
  pluginInfo: any;
  pluginWidget: any;
}

const PluginInfoView: React.FC<IPluginInfoProps> = ({
  classes,
  type,
  pluginInfo,
  pluginWidget,
}) => {
  if (!pluginInfo) {
    return null;
  }

  const displayName = objectQuery(pluginWidget, 'display-name') || objectQuery(pluginInfo, 'name');

  return (
    <div className={classes.root}>
      <div className={classes.type}>{type}</div>
      <div className={classes.name}>{displayName}</div>
    </div>
  );
};

const PluginInfo = withStyles(styles)(PluginInfoView);
export default PluginInfo;
