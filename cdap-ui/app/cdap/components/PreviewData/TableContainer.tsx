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

import React from 'react';
import If from 'components/If';
import DataTable from 'components/PreviewData/Table';
import { INode, IRecords } from 'components/PreviewData/utilities';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import T from 'i18n-react';

const I18N_PREFIX = 'features.PreviewData.TableContainer';

const styles = (theme): StyleRules => ({
  outerContainer: {
    display: 'flex',
  },
  innerContainer: {
    overflow: 'scroll',
    padding: '10px',
    width: '100%',
  },
  split: {
    maxWidth: '50%',
    borderBottom: `1px solid ${theme.palette.grey[400]}`,
    padding: '10px',
    borderRight: `1px solid ${theme.palette.grey[400]}`,
    '& :last-of-type': {
      borderRight: 0,
    },
  },
  h2Title: {
    fontSize: '1.4rem !important',
    fontWeight: 'bold',
    borderBottom: `1px solid ${theme.palette.grey[400]}`,
    paddingBottom: '5px',
    paddingLeft: '10px',
    margin: '0 -10px',
  },
});

interface IPreviewTableContainerProps extends WithStyles<typeof styles> {
  tableData: { inputs: IRecords[]; outputs: IRecords[] };
  selectedNode: INode;
  previewStatus?: string;
}

const TableContainer: React.FC<IPreviewTableContainerProps> = ({
  classes,
  tableData,
  selectedNode,
  previewStatus,
}) => {
  return (
    <div className={classes.outerContainer}>
      <If condition={!selectedNode.isSource && !selectedNode.isCondition}>
        <div
          className={classnames(classes.innerContainer, {
            [classes.split]: !selectedNode.isSource && !selectedNode.isSink,
          })}
        >
          <h2 className={classes.h2Title}>{T.translate(`${I18N_PREFIX}.inputHeader`)}</h2>
          {tableData.inputs.map((input, i) => {
            const inputHeaders = input.schemaFields;
            const inputRecords = input.records;
            return (
              <div key={`input-table-${i}`}>
                <DataTable
                  headers={inputHeaders}
                  records={inputRecords}
                  isInput={true}
                  previewStatus={previewStatus}
                />
              </div>
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
          {tableData.outputs.map((output, j) => {
            const outputHeaders = output.schemaFields;
            const outputRecords = output.records;
            return (
              <div key={`output-table-${j}`}>
                <DataTable
                  headers={outputHeaders}
                  records={outputRecords}
                  isInput={false}
                  previewStatus={previewStatus}
                />
              </div>
            );
          })}
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
