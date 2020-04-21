/*
 * Copyright © 2019 Cask Data, Inc.
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
import WidgetRenderer from 'components/Ingestion/PluginWidgetRenderer';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      backgroundColor: theme.palette.grey[700],
      padding: '20px',
    },
    propsRenderBlock: {
      margin: '0 40px',
      minWidth: '40%',
      maxHeight: '60vh',
      overflowY: 'scroll',
    },
    propsContainer: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'center',
      width: '100%',
    },
    jobInfo: { display: 'flex', flexDirection: 'column', alignItems: 'center' },
  };
};
interface IPlugin {
  name: string;
  artifact: { version: string };
  widgetJson: any;
}
interface ISourceSinkConfigProps extends WithStyles<typeof styles> {
  plugins: IPlugin[];
  title: string;
  onPluginSelect: (plugin: IPlugin) => void;
  sourceBP: any;
  selectedSource: any;
  sinkBP: any;
  selectedSink: any;
  onSinkChange: any;
  onSourceChange: any;
}

const SourceSinkConfig: React.FC<ISourceSinkConfigProps> = ({
  classes,
  sourceBP,
  selectedSource,
  sinkBP,
  selectedSink,
  onSourceChange,
  onSinkChange,
}) => {
  const sourceCGProps = {
    pluginProperties: sourceBP && sourceBP.properties,
    widgetJson: selectedSource && selectedSource.widgetJson,
    values: selectedSource && selectedSource.properties,
    label: selectedSource.label,
    onChange: (property) => {
      const newSource = { ...selectedSource };
      newSource.properties = { ...newSource.properties, ...property };
      onSourceChange(newSource);
    },
    onLabelChange: (label) => {
      const newSource = { ...selectedSource };
      newSource.label = label;
      onSourceChange(newSource);
    },
  };
  const sinkCGProps = {
    pluginProperties: sinkBP && sinkBP.properties,
    widgetJson: selectedSink && selectedSink.widgetJson,
    values: selectedSink && selectedSink.properties,
    label: selectedSink.label,
    onChange: (property) => {
      const stateCopy = { ...selectedSink };
      stateCopy.properties = { ...stateCopy.properties, ...property };
      onSinkChange(stateCopy);
    },
    onLabelChange: (label) => {
      const stateCopy = { ...selectedSink };
      stateCopy.label = label;
      onSinkChange(stateCopy);
    },
  };
  return (
    <div className={classes.root}>
      <div className={classes.propsContainer}>
        <If condition={sourceBP && selectedSource}>
          <div className={classes.propsRenderBlock}>
            <WidgetRenderer
              title="Source"
              plugin={selectedSource}
              configurationGroupProps={sourceCGProps}
            />
          </div>
        </If>
        <If condition={sinkBP && selectedSink}>
          <div className={classes.propsRenderBlock}>
            <WidgetRenderer
              title="Target"
              plugin={selectedSink}
              configurationGroupProps={sinkCGProps}
            />
          </div>
        </If>
      </div>
    </div>
  );
};

const StyledSourceSinkConfig = withStyles(styles)(SourceSinkConfig);

function SourceSinkConfigurator(props) {
  return (
    <ThemeWrapper>
      <StyledSourceSinkConfig {...props} />
    </ThemeWrapper>
  );
}

export default SourceSinkConfigurator;
