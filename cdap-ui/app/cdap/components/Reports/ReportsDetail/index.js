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
import ReportsStore, { ReportsActions } from 'components/Reports/store/ReportsStore';
import { Link } from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import IconSVG from 'components/IconSVG';

require('./ReportsDetail.scss');

export default class ReportsDetail extends Component {
  static propTypes = {
    match: PropTypes.object
  };

  componentWillMount() {
    let params = {
      reportId: this.props.match.params.reportId
    };

    MyReportsApi.getSummary(params)
      .combineLatest(MyReportsApi.getRuns(params))
      .subscribe(([summary, runsInfo]) => {
        ReportsStore.dispatch({
          type: ReportsActions.setDetails,
          payload: {
            runs: runsInfo.runs,
            summary
          }
        });
      });
  }

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
              {this.props.match.params.reportId}
            </span>
          </div>
        </div>

        <div className="reports-detail-container">
          <div className="action-section clearfix">
            <div className="date-container float-xs-left">
              Report generated on some date
            </div>

            <div className="action-button float-xs-right">
              <button className="btn btn-primary">
                Save Report
              </button>

              <button className="btn btn-link">
                Export?
              </button>
            </div>
          </div>

          <Summary />

          <Runs />
        </div>
      </div>
    );
  }
}
