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
import { JSONStatusMessage } from 'components/PluginJSONCreator/constants';
import {
  ConfigurationGroupContext,
  FilterContext,
  OutputContext,
  PluginInfoContext,
  useAppInternalState,
  WidgetContext,
} from 'components/PluginJSONCreator/Create';
import ConfigurationGroupPage from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage';
import FilterPage from 'components/PluginJSONCreator/Create/Content/FilterPage';
import OutputPage from 'components/PluginJSONCreator/Create/Content/OutputPage';
import PluginInfoPage from 'components/PluginJSONCreator/Create/Content/PluginInfoPage';
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
            <PluginInfoPage />
          </PluginInfoContext.Provider>
        );
      case 1:
        return (
          <ConfigurationGroupContext.Provider value={configuratioGroupContextValue}>
            <WidgetContext.Provider value={widgetContextValue}>
              <ConfigurationGroupPage />
            </WidgetContext.Provider>
          </ConfigurationGroupContext.Provider>
        );
      case 2:
        return (
          <OutputContext.Provider value={outputContextValue}>
            <OutputPage />
          </OutputContext.Provider>
        );
      case 3:
        return (
          <FilterContext.Provider value={filterContextValue}>
            <WidgetContext.Provider value={widgetContextValue}>
              <FilterPage />
            </WidgetContext.Provider>
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
