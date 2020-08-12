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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import CollapseLiveViewButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/CollapseLiveViewButton';
import If from 'components/If';
import LiveConfigurationGroup from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveView/LiveConfigurationGroup';
import LiveJSON from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveView/LiveJSON';
import { LiveViewMode } from 'components/PluginJSONCreator/Create/PluginJSONMenu';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import debounce from 'lodash/debounce';

const styles = (theme): StyleRules => {
  return {
    liveViewtopPanel: {
      padding: '0px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      borderBottom: `1px solid ${theme.palette.divider}`,
      height: '40px',
    },
    collapseLiveViewButton: {
      marginLeft: 'auto',
      display: 'flex',
      flexDirection: 'row',
    },
    buttonTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
    currentFilename: {
      position: 'relative',
      margin: '0 auto',
      left: '25px',
      fontFamily: 'Courier New',
    },
  };
};

interface ILiveViewProps extends WithStyles<typeof styles> {
  liveViewMode: LiveViewMode;
  JSONOutput: any;
  JSONFilename: string;
  collapseLiveView: () => void;
}

const LiveViewView: React.FC<ILiveViewProps> = ({
  classes,
  liveViewMode,
  JSONOutput,
  JSONFilename,
  collapseLiveView,
}) => {
  const [loading, setLoading] = React.useState(false);

  // When JSONOutput changes, show loading view for 500ms
  // This is in order to force rerender LiveConfigurationGroup component
  const debouncedUpdate = debounce(() => {
    setLoading(true);

    // after a setTimeout for 500ms, set the loading state back to false
    setTimeout(() => {
      setLoading(false);
    }, 500);
  }, 500);

  React.useEffect(debouncedUpdate, [JSONOutput]);

  return (
    <div>
      <div className={classes.liveViewtopPanel}>
        <div className={classes.currentFilename} data-cy="plugin-json-filename">
          {JSONFilename}
        </div>
        <div className={classes.collapseLiveViewButton}>
          <CollapseLiveViewButton collapseLiveView={collapseLiveView} />
        </div>
      </div>
      <If condition={loading && liveViewMode === LiveViewMode.ConfigurationGroupsView}>
        <LoadingSVGCentered />
      </If>
      <If condition={!loading && liveViewMode === LiveViewMode.ConfigurationGroupsView}>
        <LiveConfigurationGroup JSONOutput={JSONOutput} />
      </If>
      <If condition={liveViewMode === LiveViewMode.JSONView}>
        <LiveJSON JSONOutput={JSONOutput} />
      </If>
    </div>
  );
};

const LiveView = withStyles(styles)(LiveViewView);
export default LiveView;
