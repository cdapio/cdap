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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import Heading, { HeadingTypes } from 'components/Heading';
import IconButton from '@material-ui/core/IconButton';
import Close from '@material-ui/icons/Close';
import ChevronRight from '@material-ui/icons/ChevronRight';
import PluginInfo from 'components/Replicator/Create/TopPanel/PluginInfo';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '50px',
      backgroundColor: theme.palette.grey[600],
      display: 'flex',
      alignItems: 'center',
    },
    contentContainer: {
      display: 'flex',
      height: '100%',
      alignItems: 'center',
      paddingLeft: '25px',
      width: 'calc(100% - 75px)',
    },
    closeButtonContainer: {
      float: 'right',
      marginRight: '30px',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'flex-end',
    },
    heading: {
      marginBottom: 0,
      maxWidth: '60%',
      overflow: 'hidden',
      whiteSpace: 'nowrap',
      textOverflow: 'ellipsis',
      marginRight: '50px',
      minWidth: 0,
    },
    divider: {
      marginRight: '30px',
      marginLeft: '30px',
      fontSize: '35px',
      color: theme.palette.grey[200],
    },
  };
};

const TopPanelView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  name,
  sourcePluginInfo,
  sourcePluginWidget,
  targetPluginInfo,
  targetPluginWidget,
}) => {
  return (
    <div className={classes.root}>
      <div className={classes.contentContainer}>
        <Heading
          type={HeadingTypes.h5}
          label={name ? name : 'Create new Replicator'}
          className={classes.heading}
        />

        <If condition={name && sourcePluginInfo}>
          <PluginInfo
            type="Source"
            pluginInfo={sourcePluginInfo}
            pluginWidget={sourcePluginWidget}
          />
        </If>

        <If condition={targetPluginInfo}>
          <React.Fragment>
            <ChevronRight className={classes.divider} />
            <PluginInfo
              type="Target"
              pluginInfo={targetPluginInfo}
              pluginWidget={targetPluginWidget}
            />
          </React.Fragment>
        </If>
      </div>

      <div className={classes.closeButtonContainer}>
        <IconButton size="small" onClick={() => history.back()}>
          <Close />
        </IconButton>
      </div>
    </div>
  );
};

const StyledTopPanel = withStyles(styles)(TopPanelView);
const TopPanel = createContextConnect(StyledTopPanel);
export default TopPanel;
