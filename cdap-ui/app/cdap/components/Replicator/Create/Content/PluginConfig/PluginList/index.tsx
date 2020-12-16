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
import HorizontalCarousel from 'components/HorizontalCarousel';
import { fetchPluginsAndWidgets } from 'components/Replicator/utilities';
import { objectQuery } from 'services/helpers';
import TextField from '@material-ui/core/TextField';
import InputAdornment from '@material-ui/core/InputAdornment';
import Search from '@material-ui/icons/Search';
import PluginCard, {
  PluginCardWidth,
  PluginCardHeight,
} from 'components/Replicator/Create/Content/PluginConfig/PluginCard';
import classnames from 'classnames';
import Heading, { HeadingTypes } from 'components/Heading';
import { PluginType } from 'components/Replicator/constants';

const styles = (theme): StyleRules => {
  return {
    header: {
      display: 'grid',
      gridTemplateColumns: '1fr 400px',
    },
    searchSection: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'flex-end',
    },
    targetItem: {
      marginRight: '25px',
      cursor: 'pointer',
    },
    selected: {
      borderColor: theme.palette.blue[200],
      borderWidth: '3px',
    },
    search: {
      width: '200px',
      marginLeft: '20px',

      '& input': {
        paddingTop: '10px',
        paddingBottom: '10px',
      },
    },
    listContainer: {
      marginTop: '15px',
      marginBottom: '15px',
      height: `${PluginCardHeight}px`,
    },
    arrow: {
      top: `${Math.floor(PluginCardHeight / 2)}px`,
    },
  };
};

interface IPluginListProps extends ICreateContext, WithStyles<typeof styles> {
  onSelect: (plugin) => void;
  currentSelection: any;
  pluginType: PluginType;
}

const PluginListView: React.FC<IPluginListProps> = ({
  classes,
  onSelect,
  currentSelection,
  parentArtifact,
  pluginType,
}) => {
  const [plugins, setPlugins] = React.useState([]);
  const [widgetMap, setWidgetMap] = React.useState({});
  const [search, setSearch] = React.useState('');

  React.useEffect(() => {
    fetchPluginsAndWidgets(parentArtifact, pluginType).subscribe((res) => {
      if (res.plugins.length > 0 && !currentSelection) {
        onSelect(res.plugins[0]);
      }

      setPlugins(res.plugins);
      setWidgetMap(res.widgetMap);
    });
  }, []);

  function handleSearch(e) {
    const value = e.target.value;
    setSearch(value);
  }

  let filteredPlugins = plugins;
  if (search.length > 0) {
    filteredPlugins = plugins.filter((plugin) => {
      const pluginKey = `${plugin.name}-${plugin.type}`;
      const displayName = objectQuery(widgetMap, pluginKey, 'display-name');

      const normalizedSearch = search.toLowerCase();

      if (plugin.name.toLowerCase().indexOf(normalizedSearch) !== -1) {
        return true;
      }

      return displayName && displayName.toLowerCase().indexOf(normalizedSearch) !== -1;
    });
  }

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <div>
          <Heading type={HeadingTypes.h4} label="Select plugin" />
        </div>
        <div className={classes.searchSection}>
          <div>
            {plugins.length} {plugins.length === 1 ? 'plugin' : 'plugins'} available
          </div>
          <TextField
            className={classes.search}
            value={search}
            onChange={handleSearch}
            variant="outlined"
            placeholder="Search plugins by name"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search />
                </InputAdornment>
              ),
            }}
          />
        </div>
      </div>

      <div className={classes.listContainer}>
        <HorizontalCarousel scrollAmount={PluginCardWidth} classes={{ arrow: classes.arrow }}>
          {filteredPlugins.map((plugin) => {
            const pluginKey = `${plugin.name}-${plugin.type}`;
            const widgetInfo = widgetMap[pluginKey];
            const targetName = widgetInfo ? widgetInfo['display-name'] : plugin.name;
            const icon = objectQuery(widgetInfo, 'icon', 'arguments', 'data');

            const currentSelectionName = objectQuery(currentSelection, 'name');
            const currentSelectionArtifact = objectQuery(currentSelection, 'artifact', 'name');

            return (
              <div
                key={plugin.name}
                className={classes.targetItem}
                onClick={onSelect.bind(null, plugin)}
              >
                <PluginCard
                  name={targetName}
                  icon={icon}
                  classes={{
                    root: classnames({
                      [classes.selected]:
                        plugin.name === currentSelectionName &&
                        plugin.artifact.name === currentSelectionArtifact,
                    }),
                  }}
                />
              </div>
            );
          })}
        </HorizontalCarousel>
      </div>
    </div>
  );
};

const StyledPluginList = withStyles(styles)(PluginListView);
const PluginList = createContextConnect(StyledPluginList);
export default PluginList;
