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
import ThemeWrapper from 'components/ThemeWrapper';
import Button from '@material-ui/core/Button';
import Card from '@material-ui/core/Card';
import { objectQuery } from 'services/helpers';
import Popover from '@material-ui/core/Popover';
import If from 'components/If';
import { getIcon } from 'components/Ingestion/helpers';
import HorizontalCarousel from 'components/HorizontalCarousel';
import SinkList from 'components/Ingestion/SinkList';
import { getPluginDisplayName } from 'components/Ingestion/helpers';

const styles = (theme): StyleRules => {
  return {
    root: { width: '100%' },
    pluginsRow: { display: 'flex', flexDirection: 'row', flexWrap: 'wrap' },
    title: { display: 'flex', alignItems: 'center' },
    pluginCard: {
      display: 'flex',
      flexDirection: 'column',
      margin: '10px',
      alignItems: 'center',
      width: '250px',
      height: '255px',
      flexShrink: 0,
      justifyContent: 'space-around',
    },
    pluginImageContainer: {
      display: 'flex',
      alignItems: 'center',
    },
    pluginImageBackground: {
      display: 'flex',
      width: '100%',
      minHeight: '128px',
      justifyContent: 'center',
      backgroundColor: theme.palette.grey[700],
    },
    sourceListTable: { width: '900px' },
    tablePluginIcon: {
      width: '32px',
      height: 'auto',
    },
    tablePluginFAIcon: {
      fontSize: '32px',
    },
    ingestionHeader: {
      backgroundColor: theme.palette.grey[700],
      color: theme.palette.grey[100],
    },
    targetName: {
      cursor: 'pointer',
      marginRight: '5px',
    },
    targetsCell: {},
    pluginIcon: {
      width: '100px',
      height: 'auto',
    },
    pluginFAIcon: {
      fontSize: '64px',
    },
    cardTitle: {
      padding: '15px',
    },
    cardButtonsContainer: {
      display: 'flex',
      width: '100%',
      justifyContent: 'center',
    },
  };
};
interface IPlugin {
  name: string;
  artifact: { version: string };
  widgetJson: any;
}
interface IPluginListProps extends WithStyles<typeof styles> {
  plugins: IPlugin[];
  title: string;
  onPluginSelect: (plugin: IPlugin) => void;
  sinks: any;
  onSourceSinkSelect: any;
}

const PluginListView: React.FC<IPluginListProps> = ({
  classes,
  plugins,
  sinks,
  onPluginSelect,
}) => {
  const [anchorEl, setAnchorEl] = React.useState(null);

  const open = Boolean(anchorEl);

  function handleClick(event, plugin) {
    setAnchorEl(event.currentTarget);
    onPluginSelect(plugin);
  }
  function handleClose() {
    setAnchorEl(null);
  }
  function renderCarousel() {
    return (
      <HorizontalCarousel scrollAmount={650}>
        {plugins.map((plugin) => {
          const displayName = getPluginDisplayName(plugin);
          const iconData = objectQuery(plugin, 'widgetJson', 'icon', 'arguments', 'data');
          return (
            <Card
              key={`${plugin.name} - ${plugin.artifact.version}`}
              className={classes.pluginCard}
            >
              <div className={classes.pluginImageBackground}>
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
              </div>
              <div className={classes.cardTitle}>
                <h3>{displayName}</h3>
              </div>
              <div className={classes.cardButtonsContainer}>
                <Button
                  className={classes.targetsButton}
                  color="primary"
                  onClick={(e) => handleClick(e, plugin)}
                >
                  Show Targets
                </Button>
              </div>
            </Card>
          );
        })}
      </HorizontalCarousel>
    );
  }

  const targetList = <SinkList title="Sinks" plugins={sinks} onPluginSelect={onPluginSelect} />;

  return (
    <div className={classes.root}>
      {renderCarousel()}
      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <If condition={sinks.length > 0}>{targetList}</If>
      </Popover>
    </div>
  );
};

const StyledPluginList = withStyles(styles)(PluginListView);

export default StyledPluginList;
