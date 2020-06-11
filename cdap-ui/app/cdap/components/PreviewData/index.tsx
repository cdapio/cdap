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
import { messageTextStyle } from 'components/PreviewData/DataView/Table';
import PreviewTableContainer from 'components/PreviewData/DataView/TableContainer';
import RecordContainer from 'components/PreviewData/RecordView/RecordContainer';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import T from 'i18n-react';
import { extractErrorMessage } from 'services/helpers';
import ToggleSwitchWidget from 'components/AbstractWidget/ToggleSwitchWidget';
import classnames from 'classnames';
import LoadingSVG from 'components/LoadingSVG';

const I18N_PREFIX = 'features.PreviewData';
// If number of schema fields > maxSchemaSize, show Record view by default
const maxSchemaSize = 100;

const styles = (): StyleRules => {
  return {
    messageText: messageTextStyle,
    headingContainer: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      paddingTop: '50px',
    },
    recordToggle: {
      position: 'absolute',
      top: '8px',
      right: '35px',
    },
  };
};

interface IPreviewDataViewProps extends WithStyles<typeof styles> {
  previewId: string;
  selectedNode: INode;
  getStagesAndConnections: () => any;
  previewStatus: string;
}

enum PreviewMode {
  Record = 'Record',
  Table = 'Table',
}

export interface ITableData {
  inputs?: Array<[string, IRecords]>;
  outputs?: Array<[string, IRecords]>;
  inputFieldCount?: number;
  outputFieldCount?: number;
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
  const [tableData, setTableData] = useState<ITableData>({});
  const [error, setError] = useState(null);
  const [viewMode, setViewMode] = useState(PreviewMode.Table);

  const widgetProps = {
    on: { value: PreviewMode.Record, label: PreviewMode.Record },
    off: { value: PreviewMode.Table, label: PreviewMode.Table },
  };

  const updatePreviewCb = (updatedPreview: IPreviewData) => {
    setPreviewData(updatedPreview);
    const parsedData = getTableData(updatedPreview);
    setTableData(parsedData);
    setViewMode(
      parsedData.inputFieldCount > maxSchemaSize || parsedData.outputFieldCount > maxSchemaSize
        ? PreviewMode.Record
        : PreviewMode.Table
    );
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
        updatePreviewCb
      );
    }
  }, [previewId]);

  const getTableData = (prevData: IPreviewData) => {
    let inputs = [];
    let outputs = [];

    if (!isEmpty(prevData)) {
      if (!isEmpty(prevData.input) && !selectedNode.isSource) {
        inputs = Object.entries(prevData.input);
      }
      if (!isEmpty(prevData.output) && !selectedNode.isSink) {
        outputs = Object.entries(prevData.output);
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

  const getContent = () => {
    // preview has not been run yet
    if (!previewId) {
      return noPreviewDataMsg(classes);
    } else if (previewLoading) {
      // preview data available and getting fetched
      return loadingMsg(classes);
    } else if (error) {
      // i.e. if current preview got overwritten by a more recent preview run
      return errorMsg(classes);
    } else if (!isEmpty(previewData)) {
      // return either table or record container depending on schema size
      return getContainer();
    }
  };

  const loadingMsg = (cls) => (
    <div className={cls.headingContainer}>
      <Heading
        type={HeadingTypes.h3}
        label={T.translate(`${I18N_PREFIX}.loading`)}
        className={cls.messageText}
      />
      <LoadingSVG />
    </div>
  );

  const noPreviewDataMsg = (cls) => (
    <div className={classes.headingContainer}>
      <Heading
        type={HeadingTypes.h3}
        label={'Run preview to generate preview data.'}
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

  const getContainer = () => {
    return (
      <React.Fragment>
        <span className={classes.recordToggle}>
          <ToggleSwitchWidget
            onChange={setViewMode}
            value={viewMode}
            widgetProps={widgetProps}
            disabled={selectedNode.isCondition}
          />
        </span>
        <If condition={viewMode === PreviewMode.Table}>
          <PreviewTableContainer
            tableData={tableData}
            selectedNode={selectedNode}
            previewStatus={previewStatus}
          />
        </If>
        <If condition={viewMode === PreviewMode.Record}>
          <RecordContainer
            tableData={tableData}
            selectedNode={selectedNode}
            previewStatus={previewStatus}
          />
        </If>
      </React.Fragment>
    );
  };

  return <div>{getContent()}</div>;
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
