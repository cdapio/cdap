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
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import { fetchPluginInfo, fetchPluginWidget } from 'components/Replicator/utilities';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';
import Documentation from 'components/Replicator/Create/Content/Documentation';
import PluginList from 'components/Replicator/Create/Content/PluginConfig/PluginList';
import { PluginType } from 'components/Replicator/constants';
import { IPluginConfig, IPluginInfo } from 'components/Replicator/types';
import { IWidgetJson } from 'components/ConfigurationGroup/types';
import {
  processConfigurationGroups,
  removeFilteredProperties,
} from 'components/ConfigurationGroup/utilities';
import { filterByCondition } from 'components/ConfigurationGroup/utilities/DynamicPluginFilters';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
    },
    header: {
      marginTop: '35px',
      fontSize: '18px',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
    },
    option: {
      marginRight: '100px',
      cursor: 'pointer',
    },
    active: {
      fontWeight: 'bold',
      borderBottom: `3px solid ${theme.palette.grey[200]}`,
    },
    content: {
      paddingTop: '25px',
    },
  };
};

enum VIEW {
  configuration = 'CONFIGURATION',
  documentation = 'DOCUMENTATION',
}

const TITLE_MAP = {
  [PluginType.source]: 'Configure Source',
  [PluginType.target]: 'Configure Target',
};

interface IPluginConfigProps extends ICreateContext, WithStyles<typeof styles> {
  pluginInfo: IPluginInfo;
  pluginWidget: IWidgetJson;
  pluginConfig: IPluginConfig;
  setPluginInfo: (pluginInfo: IPluginInfo) => void;
  setPluginWidget: (pluginWidget: IWidgetJson) => void;
  setPluginConfig: (config: IPluginConfig) => void;
  pluginType: PluginType;
}

const PluginConfigView: React.FC<IPluginConfigProps> = ({
  classes,
  pluginInfo,
  pluginWidget,
  pluginConfig,
  setPluginInfo,
  setPluginWidget,
  setPluginConfig,
  parentArtifact,
  pluginType,
}) => {
  const [view, setView] = React.useState(VIEW.configuration);
  const [values, setValues] = React.useState(pluginConfig || {});
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    if (pluginWidget || !pluginInfo) {
      return;
    }

    const artifact = pluginInfo.artifact;

    fetchPluginWidget(
      artifact.name,
      artifact.version,
      artifact.scope,
      pluginInfo.name,
      pluginInfo.type
    ).subscribe((res) => {
      setPluginWidget(res);
    });
  }, []);

  // Fetch Target
  function handlePluginSelect(plugin) {
    setLoading(true);

    const artifactName = plugin.artifact.name;
    const pluginName = plugin.name;

    fetchPluginInfo(
      parentArtifact,
      artifactName,
      plugin.artifact.scope,
      pluginName,
      pluginType
    ).subscribe(
      (res) => {
        fetchPluginWidget(
          artifactName,
          res.artifact.version,
          res.artifact.scope,
          pluginName,
          pluginType
        ).subscribe(
          (widget) => {
            setValues({});
            setPluginInfo(res);
            setPluginWidget(widget);
          },
          null,
          () => {
            setLoading(false);
          }
        );
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.error('Error fetching plugin', err);

        // TODO: error handling
      }
    );
  }

  const pluginProperties = objectQuery(pluginInfo, 'properties') || {};

  function isNextDisabled() {
    const requiredProperties = Object.keys(pluginProperties).filter((property) => {
      return pluginProperties[property].required;
    });

    const isPropertyFilled = requiredProperties.map((property) => {
      return values && values[property] && values[property].length > 0;
    });

    return isPropertyFilled.filter((propertyValue) => !propertyValue).length > 0;
  }

  function handleNext() {
    // Remove filtered out values
    const widgetConfigurationGroup = objectQuery(pluginWidget, 'configuration-groups');
    const widgetOutputs = objectQuery(pluginWidget, 'outputs');
    const processedConfigurationGroup = processConfigurationGroups(
      pluginProperties,
      widgetConfigurationGroup,
      widgetOutputs
    );
    const filteredConfigurationGroup = filterByCondition(
      processedConfigurationGroup.configurationGroups,
      pluginWidget,
      pluginProperties,
      values
    );
    const filteredValues = removeFilteredProperties(values, filteredConfigurationGroup);

    setPluginConfig(filteredValues);
  }

  return (
    <div className={classes.root}>
      <PluginList
        onSelect={handlePluginSelect}
        currentSelection={pluginInfo}
        pluginType={pluginType}
      />

      <div className={classes.header}>
        <span
          className={classnames(classes.option, {
            [classes.active]: view === VIEW.configuration,
          })}
          onClick={() => setView(VIEW.configuration)}
        >
          {TITLE_MAP[pluginType]}
        </span>
        <span
          className={classnames(classes.option, {
            [classes.active]: view === VIEW.documentation,
          })}
          onClick={() => setView(VIEW.documentation)}
        >
          Documentation
        </span>
      </div>

      <If condition={pluginInfo && !loading}>
        <div className={classes.content}>
          <If condition={view === VIEW.configuration}>
            <ConfigurationGroup
              widgetJson={pluginWidget}
              pluginProperties={pluginProperties}
              values={values}
              onChange={setValues}
            />
          </If>

          <If condition={view === VIEW.documentation}>
            <Documentation pluginInfo={pluginInfo} />
          </If>
        </div>
      </If>
      <If condition={loading}>
        <LoadingSVG />
      </If>

      <StepButtons onNext={handleNext} nextDisabled={isNextDisabled()} />
    </div>
  );
};

const StyledPluginConfig = withStyles(styles)(PluginConfigView);
const PluginConfig = createContextConnect(StyledPluginConfig);
export default PluginConfig;
