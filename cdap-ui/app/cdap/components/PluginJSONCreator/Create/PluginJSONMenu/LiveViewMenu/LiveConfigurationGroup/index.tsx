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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import ConfigurationGroup from 'components/ConfigurationGroup';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import debounce from 'lodash/debounce';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    loadingBox: {
      width: '100%',
      height: '100%',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
  };
};

interface ILiveConfigurationGroup extends WithStyles<typeof styles> {
  JSONOutput: any;
}

const LiveConfigurationGroupView: React.FC<ILiveConfigurationGroup> = ({ classes, JSONOutput }) => {
  // Values needed for Configuration Groups live view
  const [pluginProperties, setPluginProperties] = React.useState(null);
  const [values, onChange] = React.useState<Record<string, string>>({});

  const [loading, setLoading] = React.useState(false);

  // When JSON config changes, show loading view for 500ms
  // This is in order to force rerender ConfigurationGroup component
  const debouncedUpdate = debounce(() => {
    const newPluginProperties = getNewPluginProperties(JSONOutput);
    setPluginProperties(newPluginProperties);
    setLoading(false);
  }, 500);

  React.useEffect(() => {
    // after a setTimeout for 500ms, set the loading state back to false
    setLoading(true);
    debouncedUpdate();
  }, [JSONOutput]);

  /*
   * Get pluginProperties, which is required for ConfigurationGroups component.
   */
  function getNewPluginProperties(config) {
    if (!config['configuration-groups']) {
      return;
    }
    const newPluginProperties = {};
    config['configuration-groups'].forEach((group) => {
      return group.get('properties').forEach((widget) => {
        const widgetName = widget.get('name');
        newPluginProperties[widgetName] = { name: widgetName };
      });
    });
    return newPluginProperties;
  }

  /*
   * Convert immutable data to non-immutable.
   * ConfigurationGroup component takes non-immutable data as input.
   */
  function processForConfigurationGroup(config) {
    const processed = {
      ...config,
      ...(config['configuration-groups'] && {
        'configuration-groups': config['configuration-groups'].toJS(),
      }),
      ...(config.filters && {
        filters: config.filters.toJS(),
      }),
    };
    return processed;
  }

  return (
    <div>
      <If condition={loading}>
        <div className={classes.loadingBox}>
          <LoadingSVGCentered />
        </div>
      </If>
      <If condition={!loading}>
        <ConfigurationGroup
          pluginProperties={pluginProperties}
          widgetJson={processForConfigurationGroup(JSONOutput)}
          values={values}
        />
      </If>
    </div>
  );
};

const LiveConfigurationGroup = withStyles(styles)(LiveConfigurationGroupView);
export default LiveConfigurationGroup;
