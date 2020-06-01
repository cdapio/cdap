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

import Button from '@material-ui/core/Button';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import GetAppIcon from '@material-ui/icons/GetApp';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJsonImporter';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
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
    closeJSONViewerButon: {
      marginLeft: 'auto',
    },
    jsonViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
    jsonLiveCode: {
      padding: '14px',
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
        tooltip: classes.jsonViewerTooltip,
      }}
    >
      <div>
        <Button disabled={downloadDisabled} onClick={onDownloadClick}>
          <GetAppIcon />
        </Button>
      </div>
    </Tooltip>
  );
};

const CollapseJSONViewButton = ({ classes, collapseJSONView }) => {
  return (
    <Tooltip
      classes={{
        tooltip: classes.jsonViewerTooltip,
      }}
      title="Close JSON View"
    >
      <Button className={classes.closeJSONViewerButon} onClick={collapseJSONView}>
        <FullscreenExitIcon />
      </Button>
    </Tooltip>
  );
};

interface IJsonLiveViewerProps extends WithStyles<typeof styles>, ICreateContext {
  JSONConfig: any;
  collapseJSONView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  jsonFilename: string;
  JSONStatus: JSONStatusMessage;
  downloadDisabled: boolean;
  JSONErrorMessage: string;
}

const JsonLiveViewerView: React.FC<IJsonLiveViewerProps> = ({
  classes,
  JSONConfig,
  collapseJSONView,
  onDownloadClick,
  populateImportResults,
  jsonFilename,
  JSONStatus,
  downloadDisabled,
}) => {
  return (
    <div>
      <div className={classes.jsonActionButtons}>
        <Tooltip
          classes={{
            tooltip: classes.jsonViewerTooltip,
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
        <div className={classes.currentFilename}>{jsonFilename}</div>

        <CollapseJSONViewButton classes={classes} collapseJSONView={collapseJSONView} />
      </div>
      <div className={classes.jsonLiveCode}>
        <pre>{JSON.stringify(JSONConfig, undefined, 2)}</pre>
      </div>
    </div>
  );
};

const JsonLiveViewer = withStyles(styles)(JsonLiveViewerView);
export default JsonLiveViewer;
