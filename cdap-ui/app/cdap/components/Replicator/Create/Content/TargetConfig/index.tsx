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
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';

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

  // Fetch Target
  React.useEffect(() => {
    // TODO: replace with data from Target List
    const artifactName = 'delta-bigquery-plugins';
    const artifactScope = 'SYSTEM';
    const pluginName = 'bigquery';

    fetchPluginInfo(artifactName, artifactScope, pluginName, PluginType.target).subscribe(
      (res) => {
        setTargetPluginInfo(res);

        fetchPluginWidget(
          artifactName,
          res.artifact.version,
          res.artifact.scope,
          pluginName,
          PluginType.target
        ).subscribe((widget) => {
          setTargetPluginWidget(widget);
        });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.error('Error fetching plugin', err);

        // TODO: error handling
      }
    );
  }, []);

  if (!targetPluginInfo) {
    return <LoadingSVGCentered />;
  }

  const pluginProperties = objectQuery(targetPluginInfo, 'properties');

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
      <ConfigurationGroup
        widgetJson={targetPluginWidget}
        pluginProperties={targetPluginInfo.properties}
        values={values}
        onChange={setValues}
      />

      <StepButtons onNext={handleNext} nextDisabled={isNextDisabled()} />
    </div>
  );
};

const StyledTargetConfig = withStyles(styles)(TargetConfigView);
const TargetConfig = createContextConnect(StyledTargetConfig);
export default TargetConfig;
