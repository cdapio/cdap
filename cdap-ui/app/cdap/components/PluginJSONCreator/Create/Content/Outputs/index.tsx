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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Heading, { HeadingTypes } from 'components/Heading';
import { createContextConnect, ICreateContext } from 'components/PluginJSONCreator/Create';
import FilterCollection from 'components/PluginJSONCreator/Create/Content/FilterCollection';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonLiveViewer';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import * as React from 'react';

const styles = (): StyleRules => {
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

const OutputsView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  displayName,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  filters,
  setFilters,
  filterToName,
  setFilterToName,
  filterToCondition,
  setFilterToCondition,
  filterToShowList,
  setFilterToShowList,
  showToInfo,
  setShowToInfo,
  outputName,
  setOutputName,
}) => {
  const [localFilters, setLocalFilters] = React.useState(filters);
  const [localFilterToName, setLocalFilterToName] = React.useState(filterToName);
  const [localFilterToCondition, setLocalFilterToCondition] = React.useState(filterToCondition);
  const [localFilterToShowList, setLocalFilterToShowList] = React.useState(filterToShowList);
  const [localShowToInfo, setLocalShowToInfo] = React.useState(showToInfo);

  function saveAllResults() {
    setFilters(localFilters);
    setFilterToName(localFilterToName);
    setFilterToCondition(localFilterToCondition);
    setFilterToShowList(localFilterToShowList);
    setShowToInfo(localShowToInfo);
  }

  return (
    <div className={classes.root}>
      <JsonLiveViewer
        displayName={displayName}
        configurationGroups={configurationGroups}
        groupToInfo={groupToInfo}
        groupToWidgets={groupToWidgets}
        widgetToInfo={widgetToInfo}
        widgetToAttributes={widgetToAttributes}
        filters={localFilters}
        filterToName={localFilterToName}
        filterToCondition={localFilterToCondition}
        filterToShowList={localFilterToShowList}
        showToInfo={localShowToInfo}
        outputName={outputName}
      />
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Outputs" />
        <br />
        <PluginInput
          widgetType={'textbox'}
          value={outputName}
          setValue={setOutputName}
          label={'Output Name'}
          placeholder={'output name'}
          required={false}
        />
        <Heading type={HeadingTypes.h3} label="Filters" />
        <FilterCollection
          filters={localFilters}
          setFilters={setLocalFilters}
          filterToName={localFilterToName}
          setFilterToName={setLocalFilterToName}
          filterToCondition={localFilterToCondition}
          setFilterToCondition={setLocalFilterToCondition}
          filterToShowList={localFilterToShowList}
          setFilterToShowList={setLocalFilterToShowList}
          showToInfo={localShowToInfo}
          setShowToInfo={setLocalShowToInfo}
        />
        <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
      </div>
    </div>
  );
};

const StyledOutputsView = withStyles(styles)(OutputsView);
const Outputs = createContextConnect(StyledOutputsView);
export default Outputs;
