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
import Documentation from 'components/Replicator/Create/Content/Documentation';
import classnames from 'classnames';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import { fetchPluginWidget } from 'components/Replicator/utilities';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
      padding: '15px 40px',
    },
    header: {
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

const SourceConfigView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  sourcePluginInfo,
  sourcePluginWidget,
  setSourcePluginWidget,
  sourceConfig,
  setSourceConfig,
}) => {
  const [view, setView] = React.useState(VIEW.configuration);
  const [values, setValues] = React.useState(sourceConfig);

  const pluginProperties = objectQuery(sourcePluginInfo, 'properties');

  React.useEffect(() => {
    if (sourcePluginWidget) {
      return;
    }

    const artifact = sourcePluginInfo.artifact;

    fetchPluginWidget(
      artifact.name,
      artifact.version,
      artifact.scope,
      sourcePluginInfo.name,
      sourcePluginInfo.type
    ).subscribe((res) => {
      setSourcePluginWidget(res);
    });
  }, []);

  function handleNext() {
    setSourceConfig(values);
  }

  function isNextDisabled() {
    const requiredProperties = Object.keys(pluginProperties).filter((property) => {
      return pluginProperties[property].required;
    });

    const isPropertyFilled = requiredProperties.map((property) => {
      return values && values[property] && values[property].length > 0;
    });

    return isPropertyFilled.filter((propertyValue) => !propertyValue).length > 0;
  }

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <span
          className={classnames(classes.option, { [classes.active]: view === VIEW.configuration })}
          onClick={setView.bind(null, VIEW.configuration)}
        >
          Configure Source
        </span>
        <span
          className={classnames(classes.option, { [classes.active]: view === VIEW.documentation })}
          onClick={setView.bind(null, VIEW.documentation)}
        >
          Documentation
        </span>
      </div>

      <div className={classes.content}>
        <If condition={view === VIEW.configuration}>
          <ConfigurationGroup
            widgetJson={sourcePluginWidget}
            pluginProperties={sourcePluginInfo.properties}
            values={values}
            onChange={setValues}
          />
        </If>

        <If condition={view === VIEW.documentation}>
          <Documentation />
        </If>
      </div>

      <StepButtons onNext={handleNext} nextDisabled={isNextDisabled()} />
    </div>
  );
};

const StyledSourceConfig = withStyles(styles)(SourceConfigView);
const SourceConfig = createContextConnect(StyledSourceConfig);
export default SourceConfig;
