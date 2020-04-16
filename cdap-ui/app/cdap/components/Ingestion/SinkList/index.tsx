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
import Card from '@material-ui/core/Card';
import { objectQuery } from 'services/helpers';
import { getIcon } from 'components/Ingestion/helpers';
import If from 'components/If';
import HorizontalCarousel from 'components/HorizontalCarousel';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      backgroundColor: theme.palette.grey[700],
      padding: '20px',
      width: '95vw',
    },
    pluginsRow: { width: '100%' },
    title: { display: 'flex', alignItems: 'center' },
    pluginCard: {
      display: 'flex',
      flexDirection: 'column',
      flexShrink: 0,
      margin: '10px',
      alignItems: 'center',
      width: '175px',
      height: '120px',
      cursor: 'pointer',
      justifyContent: 'space-around',
    },
    pluginImageContainer: {
      display: 'flex',
      alignItems: 'center',
      marginTop: '10px',
    },
    pluginIcon: {
      width: '50px',
      height: 'auto',
    },
    pluginFAIcon: {
      fontSize: '32px',
    },
    cardTitle: {
      padding: '15px',
    },
    cardButtonsContainer: {
      display: 'flex',
      width: '100%',
      justifyContent: 'center',
    },
    targetsButton: {},
  };
};
interface IPlugin {
  name: string;
  artifact: { version: string };
  widgetJson: any;
}
interface ISinkListProps extends WithStyles<typeof styles> {
  plugins: IPlugin[];
  onPluginSelect: (plugin: IPlugin) => void;
}

const SinkListView: React.FC<ISinkListProps> = ({ classes, plugins, onPluginSelect }) => {
  return (
    <div className={classes.root}>
      <h6> Select a target</h6>
      <div className={classes.pluginsRow}>
        <HorizontalCarousel scrollAmount={300}>
          {plugins.map((plugin) => {
            const displayName = objectQuery(plugin, 'widgetJson', 'display-name') || plugin.name;
            const iconData = objectQuery(plugin, 'widgetJson', 'icon', 'arguments', 'data');
            return (
              <Card
                key={`${plugin.name} - ${plugin.artifact.version}`}
                className={classes.pluginCard}
                onClick={() => {
                  onPluginSelect(plugin);
                }}
              >
                <div className={classes.pluginImageContainer}>
                  <If condition={iconData}>
                    <img className={classes.pluginIcon} src={iconData} />
                  </If>
                  <If condition={!iconData}>
                    <div
                      className={`${classes.pluginFAIcon} fa ${getIcon(plugin.name.toLowerCase())}`}
                    ></div>
                  </If>
                </div>
                <h5 className={classes.cardTitle}>{displayName}</h5>
              </Card>
            );
          })}
        </HorizontalCarousel>
      </div>
    </div>
  );
};

const StyledPluginList = withStyles(styles)(SinkListView);

export default StyledPluginList;
