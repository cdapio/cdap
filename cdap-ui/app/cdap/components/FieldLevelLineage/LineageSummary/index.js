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

require('./LineageSummary.scss');

function LineageSummaryView({activeField, datasetId, backwardLineage, close}) {
  if (!activeField) { return null; }

  return (
    <div className="lineage-summary-container">
      <div className="field-lineage-info">
        <div className="title">
          Sources for:
        </div>

        <div className="dataset-field">
          {`${datasetId}:${activeField}`}
        </div>

        <div className="lineage-count">
          {backwardLineage.length} Datasets
        </div>

        <IconSVG
          name="icon-close"
          className="close-button"
          onClick={close}
        />
      </div>

      {
        backwardLineage.map((entity) => {
          return <SummaryRow entity={entity} />;
        })
      }

      <div className="view-operations">
        <span>
          View Operations
        </span>
      </div>
    </div>
  );
}

LineageSummaryView.propTypes = {
  activeField: PropTypes.string,
  datasetId: PropTypes.string,
  backwardLineage: PropTypes.array,
  close: PropTypes.func
};

const mapStateToProps = (state) => {
  return {
    activeField: state.lineage.activeField,
    datasetId: state.lineage.datasetId,
    backwardLineage: state.lineage.backward
  };
};

const mapDispatch = (dispatch) => {
  return {
    close: () => {
      dispatch({
        type: Actions.closeSummary
      });
    }
  };
};

const LineageSummary = connect(
  mapStateToProps,
  mapDispatch
)(LineageSummaryView);

export default LineageSummary;
