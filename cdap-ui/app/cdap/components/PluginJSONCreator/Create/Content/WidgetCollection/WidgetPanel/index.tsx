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

import { withStyles } from '@material-ui/core';
import { StyleRules } from '@material-ui/core/styles';
import WidgetActionButtons from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetActionButtons';
import WidgetAttributesCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection';
import WidgetInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetInput';
import {
  CreateContext,
  createContextConnect,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    eachWidget: {
      display: 'grid',
      gridTemplateColumns: '5fr 1fr',
      marginLeft: 'auto',
      marginRight: 'auto',
    },
    widgetInputs: {
      '& > *': {
        marginTop: '20px',
        marginBottom: '20px',
      },
    },
  };
};

export const WidgetPanelView = ({
  classes,
  widgetID,
  closeWidgetAttributes,
  addWidgetToGroup,
  deleteWidgetFromGroup,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetToInfo,
  setWidgetToInfo,
  widgetAttributesOpen,
}) => {
  return React.useMemo(
    () => (
      <div className={classes.eachWidget}>
        <div className={classes.widgetInputs}>
          <WidgetInput
            widgetToInfo={widgetToInfo}
            widgetID={widgetID}
            setWidgetToInfo={setWidgetToInfo}
            widgetToAttributes={widgetToAttributes}
            setWidgetToAttributes={setWidgetToAttributes}
          />
        </div>
        <WidgetActionButtons
          onAddWidgetToGroup={addWidgetToGroup}
          onDeleteWidgetFromGroup={deleteWidgetFromGroup}
        />

        <WidgetAttributesCollection
          open={widgetAttributesOpen}
          onWidgetAttributesClose={closeWidgetAttributes}
          widgetID={widgetID}
          widgetToInfo={widgetToInfo}
          setWidgetToInfo={setWidgetToInfo}
          widgetToAttributes={widgetToAttributes}
          setWidgetToAttributes={setWidgetToAttributes}
        />
      </div>
    ),
    [widgetAttributesOpen, widgetToAttributes[widgetID], widgetToInfo[widgetID]]
  );
};

const StyledWidgetPanel = withStyles(styles)(WidgetPanelView);
const WidgetPanel = createContextConnect(CreateContext, StyledWidgetPanel);
export default WidgetPanel;
