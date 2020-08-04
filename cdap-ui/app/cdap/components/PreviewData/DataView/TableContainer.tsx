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
import If from 'components/If';
import DataTable from 'components/PreviewData/DataView/Table';
import ConfigurableTab from 'components/ConfigurableTab';
import { ITableData } from 'components/PreviewData';
import { INode } from 'components/PreviewData/utilities';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import T from 'i18n-react';

const I18N_PREFIX = 'features.PreviewData.DataView.TableContainer';

export const styles = (theme): StyleRules => ({
  outerContainer: {
    display: 'flex',
    '& :last-of-type': {
      borderRight: 0,
    },
  },
  innerContainer: {
    overflow: 'hidden',
    padding: '10px',
    width: '100%',
    height: '100%',
  },
  split: {
    maxWidth: '50%',
    borderBottom: `1px solid ${theme.palette.grey[400]}`,
    padding: '10px',
    borderRight: `1px solid ${theme.palette.grey[400]}`,
    height: 'inherit',
    '& .table-pane': {
      overflow: 'scroll',
      minWidth: '100%',
    },
    '& .cask-tab-headers': {
      overflowX: 'auto',
      overflowY: 'hidden',
    },
  },
  tableContainer: {
    overflow: 'scroll',
  },
  h2Title: {
    fontSize: '1.4rem !important',
    fontWeight: 'bold',
    borderBottom: `1px solid ${theme.palette.grey[400]}`,
    paddingBottom: '5px',
    paddingLeft: '10px',
    margin: '0 -10px',
  },
  headerBumper: {
    height: '50px', // Height of horizontal tabs, to align tables if there are multiple inputs or outputs
  },
});

interface IPreviewTableContainerProps extends WithStyles<typeof styles> {
  tableData: ITableData;
  selectedNode: INode;
  previewStatus?: string;
}

const TableContainer: React.FC<IPreviewTableContainerProps> = ({
  classes,
  tableData,
  selectedNode,
  previewStatus,
}) => {
  const [activeTab, setActiveTab] = useState(null);

  const handleTabClick = (id) => {
    setActiveTab(id);
  };

  const getTabConfig = (stagesInfo, isInput: boolean) => {
    const tabs = stagesInfo.map(([stageName, recordInfo], index) => {
      return {
        id: index + 1,
        name: stageName,
        content: (
          <DataTable
            headers={recordInfo.schemaFields}
            records={recordInfo.records}
            isInput={isInput}
            previewStatus={previewStatus}
          />
        ),
        paneClassName: 'table-pane',
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

  const getTables = (stagesData, isInput: boolean) => {
    const stagesToRender = isInput ? stagesData.inputs : stagesData.outputs;
    const adjacentStages = isInput ? stagesData.outputs : stagesData.inputs;
    const showTabs = stagesToRender.length > 1;

    if (showTabs) {
      return getTabs(getTabConfig(stagesToRender, isInput));
    }
    return stagesToRender.map(([tableKey, tableValue], i) => {
      const headers = tableValue.schemaFields;
      const records = tableValue.records;
      return (
        <div key={`${isInput ? 'input' : 'output'}-table-${i}`} className={classes.tableContainer}>
          {showTabs ? null : (
            <div
              className={classnames({
                [classes.headerBumper]: adjacentStages.length > 1,
              })}
            />
          )}
          <DataTable
            headers={headers}
            records={records}
            isInput={isInput}
            previewStatus={previewStatus}
          />
        </div>
      );
    });
  };

  return (
    <div className={classes.outerContainer}>
      <If condition={!selectedNode.isSource && !selectedNode.isCondition}>
        <div
          className={classnames(classes.innerContainer, {
            [classes.split]: !selectedNode.isSource && !selectedNode.isSink,
          })}
        >
          <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.inputHeader`)}</h2>
          {getTables(tableData, true)}
        </div>
      </If>
      <If condition={!selectedNode.isSink && !selectedNode.isCondition}>
        <div
          className={classnames(classes.innerContainer, {
            [classes.split]: !selectedNode.isSource && !selectedNode.isSink,
          })}
        >
          <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.outputHeader`)}</h2>
          {getTables(tableData, false)}
        </div>
      </If>
      <If condition={selectedNode.isCondition}>
        <div className={classes.innerContainer}>
          <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.conditionHeader`)}</h2>
          <div>
            <DataTable isCondition={true} />
          </div>
        </div>
      </If>
    </div>
  );
};

const StyledTableContainer = withStyles(styles)(TableContainer);

export default StyledTableContainer;
