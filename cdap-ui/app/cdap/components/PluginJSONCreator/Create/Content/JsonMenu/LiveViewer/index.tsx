/*
 * Copyright © 2018 Cask Data, Inc.
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

import { Button, Tooltip } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import GetAppIcon from '@material-ui/icons/GetApp';
import { MyPipelineApi } from 'api/pipeline';
import ConfigurationGroup from 'components/ConfigurationGroup';
import If from 'components/If';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonMenu/LiveViewer/JsonLiveViewer';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJSONImporter';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import ToggleSwitch from 'components/ToggleSwitch';
import * as React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';

export enum LiveViewMode {
  JSONView = 'JSON_VIEW',
  ConfigurationGroupsView = 'CONFIGURATION_GROUPS',
}

const LIVE_VIEWER_WIDTH = '600px';

const styles = (theme) => {
  return {
    liveViewer: {
      zIndex: theme.zIndex.drawer,
      width: LIVE_VIEWER_WIDTH,
    },
    liveViewerPaper: {
      width: LIVE_VIEWER_WIDTH,
      backgroundColor: theme.palette.white[50],
    },
    toolbar: {
      minHeight: '48px',
    },
    mainMenu: {
      borderTop: `1px solid ${theme.palette.grey['500']}`,
      paddingTop: theme.Spacing(1),
      paddingBottom: theme.Spacing(1),
    },
    jsonActionButtons: {
      padding: '0px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
    currentFilename: {
      position: 'relative',
      margin: '0 auto',
      left: '25px',
      fontFamily: 'Courier New',
    },
    jsonFailStatus: {
      position: 'relative',
      margin: '0 auto',
      color: theme.palette.red[50],
      fontFamily: 'Courier New',
    },
    jsonSuccessStatus: {
      position: 'relative',
      margin: '0 auto',
      color: theme.palette.blue[50],
      fontFamily: 'Courier New',
    },
    liveViewerConfig: {
      marginLeft: 'auto',
      // order: '3',
      display: 'flex',
      flexDirection: 'row',
    },
    liveViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

const DownloadJSONButton = ({ classes, downloadDisabled, onDownloadClick }) => {
  return (
    <Tooltip
      title={
        downloadDisabled
          ? 'Download is disabled until the required fields are filled in'
          : 'Download Plugin JSON'
      }
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
    >
      <Button disabled={downloadDisabled} onClick={onDownloadClick}>
        <GetAppIcon />
      </Button>
    </Tooltip>
  );
};

const CollapseJSONViewButton = ({ classes, collapseLiveView }) => {
  return (
    <Tooltip
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
      title="Close JSON View"
    >
      <Button onClick={collapseLiveView}>
        <FullscreenExitIcon />
      </Button>
    </Tooltip>
  );
};

const LiveViewModeSwitcher = ({ liveViewMode, setLiveViewMode }) => {
  return (
    <div>
      {/*<ToggleSwitchWidget
        onChange={() => {
          setLiveViewMode(
            liveViewMode === LiveViewMode.JSONView
              ? LiveViewMode.ConfigurationGroupsView
              : LiveViewMode.JSONView
          );
        }}
        value={liveViewMode === LiveViewMode.JSONView}
      />*/}
      <ToggleSwitch
        isOn={liveViewMode === LiveViewMode.JSONView}
        onToggle={() => {
          setLiveViewMode(
            liveViewMode === LiveViewMode.JSONView
              ? LiveViewMode.ConfigurationGroupsView
              : LiveViewMode.JSONView
          );
        }}
      />
    </div>
  );
};

const LiveViewerView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  collapseLiveView,
  JSONConfig,
  downloadDisabled,
  onDownloadClick,
  populateImportResults,
  filename,
  JSONStatus,
  JSONErrorMessage,
  pluginName,
  pluginType,
}) => {
  const [liveViewMode, setLiveViewMode] = React.useState(LiveViewMode.JSONView);
  const [pluginProperties, setPluginProperties] = React.useState({});

  React.useEffect(() => {
    const newPluginProperties = {};
    JSONConfig['configuration-groups'].forEach((group) => {
      return group.properties.forEach((widget) => {
        const widgetName = widget.name;
        newPluginProperties[widgetName] = { name: widgetName };
      });
    });
    debugger;
    setPluginProperties(newPluginProperties);
  }, [JSONConfig]);

  const [values, setValues] = React.useState<Record<string, string>>({});

  /*const name = '';
  const version = '';
  const scope = '';
  const params = {
    namespace: getCurrentNamespace(),
    artifactId: name,
    version,
    scope,
    extensionType: pluginType,
    pluginName,
  };

  React.useEffect(() => {
    debugger;
    MyArtifactApi.fetchPluginDetails(params).subscribe((res) => {
      debugger;
      setPluginProperties(res[0].properties);
      debugger;
    });
  }, []);*/

  const artifactName = 'database-plugins';
  const artifactScope = 'SYSTEM';
  const artifactVersion = '2.3.0-SNAPSHOT';

  React.useEffect(() => {
    // Fetch plugin properties and widget json
    const pluginParams = {
      namespace: getCurrentNamespace(),
      parentArtifact: 'cdap-data-pipeline',
      version: '6.2.0-SNAPSHOT',
      // extension: 'batchsource',
      extension: pluginType,
      // pluginName: 'Database',
      pluginName,
      scope: 'SYSTEM',
      artifactName,
      artifactScope,
      limit: 1,
      order: 'DESC',
    };

    MyPipelineApi.getPluginProperties(pluginParams).subscribe((res) => {
      if (res && res.length > 0) {
        const a = res[0].properties;
        const b = pluginProperties;
        debugger;
        // setPluginProperties(res[0].properties);
      }
    });
  }, [JSONConfig]);

  return (
    <div>
      <Drawer
        open={true}
        variant="persistent"
        className={classes.liveViewer}
        anchor="right"
        disableEnforceFocus={true}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.liveViewerPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <div className={classes.jsonActionButtons}>
            <Tooltip
              classes={{
                tooltip: classes.liveViewerTooltip,
              }}
              title="Import JSON"
            >
              <PluginJSONImporter
                populateImportResults={populateImportResults}
                JSONStatus={JSONStatus}
              />
            </Tooltip>
            <DownloadJSONButton
              classes={classes}
              downloadDisabled={downloadDisabled}
              onDownloadClick={onDownloadClick}
            />
            <If
              condition={
                JSONStatus !== JSONStatusMessage.Success && JSONStatus !== JSONStatusMessage.Fail
              }
            >
              <div className={classes.currentFilename}>{filename}</div>
            </If>
            <If
              condition={
                JSONStatus === JSONStatusMessage.Success || JSONStatus === JSONStatusMessage.Fail
              }
            >
              <div
                className={
                  JSONStatus === JSONStatusMessage.Success
                    ? classes.jsonSuccessStatus
                    : classes.jsonFailStatus
                }
              >
                {JSONErrorMessage}
              </div>
            </If>

            <div className={classes.LiveViewerConfig}>
              <LiveViewModeSwitcher liveViewMode={liveViewMode} setLiveViewMode={setLiveViewMode} />
              <CollapseJSONViewButton classes={classes} collapseLiveView={collapseLiveView} />
            </div>
          </div>

          <If condition={liveViewMode === LiveViewMode.JSONView}>
            <JsonLiveViewer JSONConfig={JSONConfig} />
          </If>
          <If condition={liveViewMode === LiveViewMode.ConfigurationGroupsView}>
            <ConfigurationGroup
              pluginProperties={pluginProperties}
              widgetJson={JSONConfig}
              values={values}
            />
          </If>
        </List>
      </Drawer>
    </div>
  );
};

const LiveViewer = withStyles(styles)(LiveViewerView);
export default LiveViewer;
