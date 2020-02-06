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
import { PluginType } from 'components/Replicator/constants';
import { fetchPluginInfo, fetchPluginWidget } from 'components/Replicator/utilities';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';
import TargetList from 'components/Replicator/Create/Content/TargetConfig/TargetList';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
    },
  };
};

const TargetConfigView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  targetPluginInfo,
  targetPluginWidget,
  targetConfig,
  setTargetPluginInfo,
  setTargetPluginWidget,
  setTargetConfig,
}) => {
  const [values, setValues] = React.useState(targetConfig);
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    if (targetPluginWidget || !targetPluginInfo) {
      return;
    }

    const artifact = targetPluginInfo.artifact;

    fetchPluginWidget(
      artifact.name,
      artifact.version,
      artifact.scope,
      targetPluginInfo.name,
      targetPluginInfo.type
    ).subscribe((res) => {
      setTargetPluginWidget(res);
    });
  }, []);

  // Fetch Target
  function handleTargetSelect(target) {
    setLoading(true);
    const artifactName = target.artifact.name;
    const pluginName = target.name;

    fetchPluginInfo(artifactName, target.artifact.scope, pluginName, PluginType.target).subscribe(
      (res) => {
        setTargetPluginInfo(res);

        fetchPluginWidget(
          artifactName,
          res.artifact.version,
          res.artifact.scope,
          pluginName,
          PluginType.target
        ).subscribe(
          (widget) => {
            setTargetPluginWidget(widget);
          },
          null,
          () => {
            setLoading(false);
            setValues({});
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

  const pluginProperties = objectQuery(targetPluginInfo, 'properties') || {};

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
    setTargetConfig(values);
  }

  return (
    <div className={classes.root}>
      <TargetList onSelect={handleTargetSelect} currentSelection={targetPluginInfo} />

      <If condition={targetPluginInfo && !loading}>
        <ConfigurationGroup
          widgetJson={targetPluginWidget}
          pluginProperties={pluginProperties}
          values={values}
          onChange={setValues}
        />
      </If>
      <If condition={loading}>
        <LoadingSVG />
      </If>

      <StepButtons onNext={handleNext} nextDisabled={isNextDisabled()} />
    </div>
  );
};

const StyledTargetConfig = withStyles(styles)(TargetConfigView);
const TargetConfig = createContextConnect(StyledTargetConfig);
export default TargetConfig;
