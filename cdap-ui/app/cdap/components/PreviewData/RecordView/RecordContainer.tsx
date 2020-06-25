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

import React, { useState } from 'react';
import ConfigurableTab from 'components/ConfigurableTab';
import { ITableData } from 'components/PreviewData';
import RecordNavigator from 'components/PreviewData/RecordView/Navigator';
import RecordTable from 'components/PreviewData/RecordView/RecordTable';
import { messageTextStyle } from 'components/PreviewData/DataView/Table';
import { INode } from 'components/PreviewData/utilities';
import If from 'components/If';
import { styles as tableStyles } from 'components/PreviewData/DataView/TableContainer';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import T from 'i18n-react';
import classnames from 'classnames';

const I18N_PREFIX = 'features.PreviewData.RecordView.RecordContainer';

const styles = (theme): StyleRules => ({
  ...tableStyles(theme),
  messageText: messageTextStyle,
});

interface IRecordViewContainerProps extends WithStyles<typeof styles> {
  tableData: ITableData;
  selectedNode: INode;
  previewStatus?: string;
}

const RecordViewBase: React.FC<IRecordViewContainerProps> = ({
  classes,
  tableData,
  selectedNode,
  previewStatus,
}) => {
  const [selectedRecord, setRecord] = useState(1);
  const [activeTab, setActiveTab] = useState(null);

  const inputs = tableData.inputs;
  const outputs = tableData.outputs;

  const numRecords = Math.max(tableData.inputFieldCount, tableData.outputFieldCount);

  const updateRecord = (newVal: string) => {
    const recordNum = parseInt(newVal.split(' ')[1], 10);
    setRecord(recordNum);
  };

  const showInputTabs = inputs.length > 1;
  const showOutputTabs = outputs.length > 1;

  const handleTabClick = (id) => {
    setActiveTab(id);
  };

  const getTabConfig = (stagesInfo, recordNum: number, isInput: boolean) => {
    const recordIndex = recordNum - 1;
    const tabs = stagesInfo.map(([stageName, recordInfo], index) => {
      return {
        id: index + 1,
        name: stageName,
        content: (
          <RecordTable
            headers={recordInfo.schemaFields}
            record={recordInfo.records[recordIndex]}
            selectedRecord={selectedRecord}
            isInput={isInput}
            previewStatus={previewStatus}
          />
        ),
        paneClassName: 'record-pane',
      };
    });
    return {
      defaultTab: 1,
      layout: 'horizontal',
      tabs,
    };
  };

  const getTabs = (config) => {
    return <ConfigurableTab tabConfig={config} onTabClick={handleTabClick} activeTab={activeTab} />;
  };

  return (
    <div>
      <If condition={!selectedNode.isCondition}>
        <RecordNavigator
          selectedRecord={selectedRecord}
          numRecords={numRecords}
          updateRecord={updateRecord}
          prevOperation={() => setRecord(selectedRecord - 1)}
          nextOperation={() => setRecord(selectedRecord + 1)}
        />
      </If>
      <div className={classes.outerContainer}>
        <If condition={!selectedNode.isSource && !selectedNode.isCondition}>
          <div
            className={classnames(classes.innerContainer, {
              [classes.split]: !selectedNode.isSource && !selectedNode.isSink,
            })}
          >
            <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.inputHeader`)}</h2>
            {showInputTabs
              ? getTabs(getTabConfig(inputs, selectedRecord, true))
              : inputs.map(([stageName, stageInfo]) => {
                  return (
                    <RecordTable
                      headers={stageInfo.schemaFields}
                      record={stageInfo.records[selectedRecord - 1]}
                      selectedRecord={selectedRecord}
                      isInput={true}
                      previewStatus={previewStatus}
                    />
                  );
                })}
          </div>
        </If>
        <If condition={!selectedNode.isSink && !selectedNode.isCondition}>
          <div
            className={classnames(classes.innerContainer, {
              [classes.split]: !selectedNode.isSource && !selectedNode.isSink,
            })}
          >
            <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.outputHeader`)}</h2>
            {showOutputTabs
              ? getTabs(getTabConfig(outputs, selectedRecord, false))
              : outputs.map(([stageName, stageInfo]) => {
                  return (
                    <RecordTable
                      headers={stageInfo.schemaFields}
                      record={stageInfo.records[selectedRecord - 1]}
                      selectedRecord={selectedRecord}
                      isInput={false}
                      previewStatus={previewStatus}
                    />
                  );
                })}
          </div>
        </If>
        <If condition={selectedNode.isCondition}>
          <div className={classes.innerContainer}>
            <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.conditionHeader`)}</h2>
            <div>
              <RecordTable isCondition={true} />
            </div>
          </div>
        </If>
      </div>
    </div>
  );
};

const RecordContainer = withStyles(styles)(RecordViewBase);

export default RecordContainer;
