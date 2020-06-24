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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {
  ConfigurationGroupContext,
  FilterContext,
  OutputContext,
  PluginInfoContext,
  useAppInternalState,
  WidgetContext,
} from 'components/PluginJSONCreator/Create';
import BasicPluginInfo from 'components/PluginJSONCreator/Create/Content/BasicPluginInfo';
import ConfigurationGroupsCollection from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection';
import Filters from 'components/PluginJSONCreator/Create/Content/Filters';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import Outputs from 'components/PluginJSONCreator/Create/Content/Outputs';
import * as React from 'react';

export const STEPS = [
  {
    label: 'Basic Plugin Information',
  },
  {
    label: 'Configuration Groups',
  },
  {
    label: 'Output',
  },
  {
    label: 'Filters',
  },
];

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    content: {
      width: '50%',
      maxWidth: '1000px',
      minWidth: '600px',
    },
    comp: {
      borderRight: `1px solid ${theme.palette.grey[400]}`,
      width: '60%',
      color: `${theme.palette.grey[400]}`,
    },
  };
};

const ContentView = ({
  classes,
  pluginInfoContextValue,
  configuratioGroupContextValue,
  widgetContextValue,
  outputContextValue,
  filterContextValue,
}) => {
  const { activeStep, JSONStatus, setJSONStatus } = useAppInternalState();
  const [loading, setLoading] = React.useState(false);

  // When JSON status was successful, show loading view for 500ms
  // This is in order to force rerender entire component
  React.useEffect(() => {
    if (JSONStatus && JSONStatus === JSONStatusMessage.Success) {
      setLoading(true);

      const timer = setTimeout(() => {
        setLoading(false);
        setJSONStatus(JSONStatusMessage.Normal);
      }, 500);

      return () => {
        clearTimeout(timer);
      };
    }
  }, [JSONStatus]);

  if (!STEPS[activeStep]) {
    return null;
  }

  const renderContentPage = () => {
    switch (activeStep) {
      case 0:
        return (
          <PluginInfoContext.Provider value={pluginInfoContextValue}>
            <BasicPluginInfo />
          </PluginInfoContext.Provider>
        );
      case 1:
        return (
          <ConfigurationGroupContext.Provider value={configuratioGroupContextValue}>
            <WidgetContext.Provider value={widgetContextValue}>
              <ConfigurationGroupsCollection />
            </WidgetContext.Provider>
          </ConfigurationGroupContext.Provider>
        );
      case 2:
        return (
          <OutputContext.Provider value={outputContextValue}>
            <Outputs />
          </OutputContext.Provider>
        );
      case 3:
        return (
          <FilterContext.Provider value={filterContextValue}>
            <Filters />
          </FilterContext.Provider>
        );
      default:
        return null;
    }
  };

  return (
    <div>
      <If condition={loading}>
        <LoadingSVGCentered />
      </If>
      <If condition={!loading}>
        <div className={classes.root}>
          <div className={classes.content}>{renderContentPage()}</div>
        </div>
      </If>
    </div>
  );
};

const Content = withStyles(styles)(ContentView);
export default Content;
