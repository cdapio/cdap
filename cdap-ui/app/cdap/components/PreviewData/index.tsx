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

import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { INode, fetchPreview, IRecords, IPreviewData } from 'components/PreviewData/utilities';
import If from 'components/If';
import isEmpty from 'lodash/isEmpty';
import Heading, { HeadingTypes } from 'components/Heading';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { messageTextStyle } from 'components/PreviewData/DataView/Table';
import PreviewTableContainer from 'components/PreviewData/DataView/TableContainer';
import RecordContainer from 'components/PreviewData/RecordView/RecordContainer';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import T from 'i18n-react';
import { extractErrorMessage } from 'services/helpers';
import classnames from 'classnames';

const I18N_PREFIX = 'features.PreviewData';

const styles = () => {
  return {
    messageText: messageTextStyle,
    headingContainer: {
      paddingLeft: '10px',
    },
  };
};

interface IPreviewDataViewProps extends WithStyles<typeof styles> {
  previewId: string;
  selectedNode: INode;
  getStagesAndConnections: () => any;
  previewStatus: string;
}

export interface ITableData {
  inputs: Array<[string, IRecords]>;
  outputs: Array<[string, IRecords]>;
  inputFieldCount: number;
  outputFieldCount: number;
}

const PreviewDataViewBase: React.FC<IPreviewDataViewProps> = ({
  previewId,
  selectedNode,
  getStagesAndConnections,
  previewStatus,
  classes,
}) => {
  const { stages, connections } = getStagesAndConnections();

  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewData, setPreviewData] = useState<IPreviewData>({});
  const [error, setError] = useState(null);

  const updatePreviewCb = (updatedPreview: IPreviewData) => {
    setPreviewData(updatedPreview);
  };

  const errorCb = (err: any) => {
    setError(extractErrorMessage(err));
  };

  useEffect(() => {
    if (previewId) {
      fetchPreview(
        selectedNode,
        previewId,
        stages,
        connections,
        setPreviewLoading,
        updatePreviewCb,
        errorCb
      );
    }
  }, [previewId]);

  const getTableData = () => {
    let inputs = [];
    let outputs = [];

    if (!isEmpty(previewData)) {
      if (!isEmpty(previewData.input) && !selectedNode.isSource) {
        inputs = Object.entries(previewData.input);
      }
      if (!isEmpty(previewData.output) && !selectedNode.isSink) {
        outputs = Object.entries(previewData.output);
      }
    }

    const fieldCountReducer = (maxCount: number, [tableName, tableInfo]) => {
      const fieldCount = tableInfo.schemaFields.length;
      return fieldCount > maxCount ? fieldCount : maxCount;
    };
    const inputFieldCount = !isEmpty(inputs) ? inputs.reduce(fieldCountReducer, 0) : 0;
    const outputFieldCount = !isEmpty(outputs) ? outputs.reduce(fieldCountReducer, 0) : 0;

    return { inputs, outputs, inputFieldCount, outputFieldCount };
  };

  const tableData: ITableData = getTableData();

  const loadingMsg = (cls) => (
    <div className={cls.headingContainer}>
      <Heading
        type={HeadingTypes.h3}
        label={T.translate(`${I18N_PREFIX}.loading`)}
        className={cls.messageText}
      />
      <LoadingSVGCentered />
    </div>
  );

  const noPreviewDataMsg = (cls) => (
    <div className={cls.headingContainer}>
      <Heading
        type={HeadingTypes.h3}
        label={T.translate(`${I18N_PREFIX}.runPreview`)}
        className={cls.messageText}
      />
    </div>
  );

  const errorMsg = (cls) => (
    <div className={classnames('text-danger', cls.headingContainer)}>
      <Heading
        type={HeadingTypes.h3}
        label={T.translate(`${I18N_PREFIX}.errorHeader`)}
        className={cls.messageText}
      />
      <span>{typeof error === 'string' ? error : JSON.stringify(error)}</span>
    </div>
  );

  const showRecordView = tableData.inputFieldCount >= 100 || tableData.outputFieldCount >= 100;

  return (
    <div>
      <If condition={!previewId}>{noPreviewDataMsg(classes)}</If>
      <If condition={previewLoading}>{loadingMsg(classes)}</If>
      <If condition={error}>{errorMsg(classes)}</If>

      <If condition={!previewLoading && previewId && !isEmpty(previewData) && !showRecordView}>
        <PreviewTableContainer
          tableData={tableData}
          selectedNode={selectedNode}
          previewStatus={previewStatus}
        />
      </If>
      <If condition={!previewLoading && previewId && !isEmpty(previewData) && showRecordView}>
        <RecordContainer
          tableData={tableData}
          selectedNode={selectedNode}
          previewStatus={previewStatus}
        />
      </If>
    </div>
  );
};

const PreviewDataViewStyled = withStyles(styles)(PreviewDataViewBase);
function PreviewDataView(props) {
  return (
    <ThemeWrapper>
      <PreviewDataViewStyled {...props} />
    </ThemeWrapper>
  );
}

(PreviewDataView as any).propTypes = {
  previewId: PropTypes.string,
  selectedNode: PropTypes.object,
  getStagesAndConnections: PropTypes.func,
  previewStatus: PropTypes.string,
};

export default PreviewDataView;
