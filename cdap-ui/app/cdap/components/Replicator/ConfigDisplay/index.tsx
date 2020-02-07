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
import PluginConfigDisplay from 'components/Replicator/ConfigDisplay/PluginConfigDisplay';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    gridContainer: {
      display: 'grid',
      gridTemplateColumns: '45% 45%',
      gridColumnGap: '10%',
    },
    container: {
      padding: '15px 0',
      maxHeight: '300px',
      overflow: 'hidden',
    },
    expanded: {
      maxHeight: 'initial',
    },
    sectionTitle: {
      marginBottom: '5px',
    },
    viewMoreContainer: {
      padding: '5px',
      backgroundColor: theme.palette.white[50],
      '& > div > span': {
        color: theme.palette.blue[100],
        cursor: 'pointer',
        '&:hover': {
          textDecoration: 'underline',
        },
      },
    },
  };
};

interface IConfigDisplayProps extends WithStyles<typeof styles> {
  sourcePluginInfo: any;
  targetPluginInfo: any;
  sourcePluginWidget: any;
  targetPluginWidget: any;
  sourceConfig: Record<string, string>;
  targetConfig: Record<string, string>;
}

const ConfigDisplayView: React.FC<IConfigDisplayProps> = ({
  classes,
  sourcePluginInfo,
  targetPluginInfo,
  sourcePluginWidget,
  targetPluginWidget,
  sourceConfig,
  targetConfig,
}) => {
  const [viewMore, setViewMore] = React.useState(false);

  function toggleViewMore() {
    setViewMore(!viewMore);
  }

  return (
    <div className={classes.root}>
      <div
        className={classnames(classes.container, classes.gridContainer, {
          [classes.expanded]: viewMore,
        })}
      >
        <div>
          <h5 className={classes.sectionTitle}>SOURCE</h5>
          <PluginConfigDisplay
            pluginInfo={sourcePluginInfo}
            pluginWidget={sourcePluginWidget}
            pluginConfig={sourceConfig}
          />
        </div>

        <div>
          <h5 className={classes.sectionTitle}>TARGET</h5>
          <PluginConfigDisplay
            pluginInfo={targetPluginInfo}
            pluginWidget={targetPluginWidget}
            pluginConfig={targetConfig}
          />
        </div>
      </div>
      <div className={`${classes.gridContainer} ${classes.viewMoreContainer}`}>
        <div>
          <span onClick={toggleViewMore}>View {!viewMore ? 'more' : 'less'} configuration</span>
        </div>
        <div>
          <span onClick={toggleViewMore}>View {!viewMore ? 'more' : 'less'} configuration</span>
        </div>
      </div>
    </div>
  );
};

const ConfigDisplay = withStyles(styles)(ConfigDisplayView);
export default ConfigDisplay;
