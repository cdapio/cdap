/*
 * Copyright © 2018 Cask Data, Inc.
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
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import SummaryRow from 'components/FieldLevelLineage/LineageSummary/SummaryRow';
import IconSVG from 'components/IconSVG';
import {Actions} from 'components/FieldLevelLineage/store/Store';
import {getOperations, replaceHistory} from 'components/FieldLevelLineage/store/ActionCreator';
import OperationsModal from 'components/FieldLevelLineage/OperationsModal';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.Summary';


require('./LineageSummary.scss');

function LineageSummaryView({activeField, datasetId, incomingLineage, close}) {
  if (!activeField) { return null; }

  const datasetFieldTitle = `${datasetId}:${activeField}`;

  return (
    <div className="lineage-summary-container">
      <div className="field-lineage-info">
        <div className="title">
          {T.translate(`${PREFIX}.title`)}
        </div>

        <div
          className="dataset-field truncate"
          title={datasetFieldTitle}
        >
          {datasetFieldTitle}
        </div>

        <div className="lineage-count">
          {T.translate(`${PREFIX}.datasetCount`, { context: incomingLineage.length })}
        </div>

        <IconSVG
          name="icon-close"
          className="close-button"
          onClick={close}
        />
      </div>

      {
        incomingLineage.map((entity, i) => {
          return <SummaryRow key={i} entity={entity} />;
        })
      }

      <div className="view-operations">
        <span
          onClick={getOperations}
        >
          {T.translate(`${PREFIX}.viewOperations`)}
        </span>

        <OperationsModal />
      </div>
    </div>
  );
}

LineageSummaryView.propTypes = {
  activeField: PropTypes.string,
  datasetId: PropTypes.string,
  incomingLineage: PropTypes.array,
  close: PropTypes.func
};

const mapStateToProps = (state) => {
  return {
    activeField: state.lineage.activeField,
    datasetId: state.lineage.datasetId,
    incomingLineage: state.lineage.incoming
  };
};

const mapDispatch = (dispatch) => {
  return {
    close: () => {
      dispatch({
        type: Actions.closeSummary
      });

      replaceHistory();
    }
  };
};

const LineageSummary = connect(
  mapStateToProps,
  mapDispatch
)(LineageSummaryView);

export default LineageSummary;
