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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {MyReportsApi} from 'api/reports';
import Summary from 'components/Reports/ReportsDetail/Summary';
import Runs from 'components/Reports/ReportsDetail/Runs';
import SaveButton from 'components/Reports/ReportsDetail/SaveButton';
import Expiry from 'components/Reports/ReportsDetail/Expiry';
import ReportsStore, { ReportsActions } from 'components/Reports/store/ReportsStore';
import { Link } from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';
import {humanReadableDate} from 'services/helpers';

require('./ReportsDetail.scss');

class ReportsDetailView extends Component {
  static propTypes = {
    match: PropTypes.object,
    created: PropTypes.number,
    reportName: PropTypes.string,
    error: PropTypes.string,
    status: PropTypes.string,
    detailError: PropTypes.string
  };

  componentWillMount() {
    this.fetchStatus();
  }

  componentWillUnmount() {
    ReportsStore.dispatch({
      type: ReportsActions.detailsReset
    });
  }

  fetchStatus = () => {
    let params = {
      reportId: this.props.match.params.reportId
    };

    MyReportsApi.getReport(params)
      .subscribe((res) => {
        ReportsStore.dispatch({
          type: ReportsActions.setInfoStatus,
          payload: {
            info: res,
            reportId: this.props.match.params.reportId
          }
        });

        if (res.status === 'COMPLETED') {
          this.fetchDetails();
        }
      }, (err) => {
        ReportsStore.dispatch({
          type: ReportsActions.setDetailsError,
          payload: {
            error: err.response
          }
        });
      });
  };

  fetchDetails = () => {
    let params = {
      reportId: this.props.match.params.reportId
    };

    MyReportsApi.getDetails(params)
      .subscribe((res) => {
        ReportsStore.dispatch({
          type: ReportsActions.setRuns,
          payload: {
            runs: res.details
          }
        });
      }, (err) => {
        console.log('err', err);
        ReportsStore.dispatch({
          type: ReportsActions.setDetailsError,
          payload: {
            error: err.response
          }
        });
      });
  };

  renderError = (isDetail) => {
    let errorHeaderLabel = isDetail ? 'Error' : 'Report Generation Failed';
    let error = isDetail ? this.props.detailError : this.props.error;

    return (
      <div className="error-container">
        <h5 className="text-danger">{errorHeaderLabel}</h5>
        <pre>{error}</pre>
      </div>
    );
  };

  renderDetail = () => {
    if (this.props.status === 'FAILED' && this.props.error) {
      return this.renderError(false);
    }

    if (this.props.detailError) {
      return this.renderError(true);
    }

    return (
      <div className="reports-detail-container">
        <div className="action-section clearfix">
          <div className="date-container float-xs-left">
            Report generated on {humanReadableDate(this.props.created)}
            <span className="separator">-</span>
            <Expiry />
          </div>

          <div className="action-button float-xs-right">
            <SaveButton />

            <button className="btn btn-link">
              Export
            </button>
          </div>
        </div>

        <Summary />

        <Runs />
      </div>
    );
  };

  render() {
    return (
      <div className="reports-container">
        <div className="header">
          <div className="reports-view-options">
            <Link to={`/ns/${getCurrentNamespace()}/reports`}>
              <IconSVG name="icon-angle-double-left" />
              <span>Reports</span>
            </Link>
            <span className="separator">|</span>
            <span>
              {this.props.reportName}
            </span>
          </div>
        </div>

        {this.renderDetail()}
      </div>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    match: ownProps.match,
    created: state.details.created,
    reportName: state.details.name,
    error: state.details.error,
    status: state.details.status,
    detailError: state.details.detailError
  };
};

const ReportsDetail = connect(
  mapStateToProps
)(ReportsDetailView);

export default ReportsDetail;
