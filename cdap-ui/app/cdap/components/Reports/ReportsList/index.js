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
import Customizer from 'components/Reports/Customizer';
import {humanReadableDate} from 'services/helpers';
import IconSVG from 'components/IconSVG';
import ReportsStore, {ReportsActions} from 'components/Reports/store/ReportsStore';
import {connect} from 'react-redux';
import Duration from 'components/Duration';
import { Link } from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';

require('./ReportsList.scss');

class ReportsListView extends Component {
  static propTypes = {
    reports: PropTypes.array
  };

  componentWillMount() {
    let params = {
      offset: 0,
      limit: 20
    };

    MyReportsApi.list(params)
      .subscribe((res) => {
        ReportsStore.dispatch({
          type: ReportsActions.setList,
          payload: res
        });
      });
  }

  renderCreated(report) {
    if (report.status === 'COMPLETED') {
      return humanReadableDate(report.created);
    }

    return (
      <div className="generating">
        <span className="fa fa-spin">
          <IconSVG name="icon-spinner" />
        </span>
        <span>Generating</span>
      </div>
    );
  }

  renderExpiry(report) {
    if (!report.expiry) { return '- -'; }

    return (
      <Duration
        targetTime={report.expiry}
        isMillisecond={false}
      />
    );
  }

  renderHeader() {
    return (
      <div className="grid-header">
        <div className="grid-row">
          <div>Report Name</div>
          <div>Created</div>
          <div>Expiration</div>
          <div>Tags</div>
          <div></div>
        </div>
      </div>
    );
  }

  renderBody() {
    return (
      <div className="grid-body">
        {
          this.props.reports.map((report) => {
            return (
              <Link
                key={report.id}
                to={`/ns/${getCurrentNamespace()}/reports/${report.id}`}
                className="grid-row grid-link"
              >
                <div className="report-name">{report.name}</div>
                <div>
                  {this.renderCreated(report)}
                </div>
                <div>
                  {this.renderExpiry(report)}
                </div>
                <div></div>
                <div>
                  <IconSVG name="icon-cog" />
                </div>
              </Link>
            );
          })
        }
      </div>
    );
  }

  render() {
    return (
      <div className="reports-container">
        <div className="header">
          <div className="reports-view-options">
            <span>Reports</span>
          </div>
        </div>
        <div className="reports-list-container">
          <Customizer />

          <div className="list-view">
            <div className="section-title">
              Select a report to view
            </div>

            <div className="list-container grid-wrapper">
              <div className="grid grid-container">
                {this.renderHeader()}
                {this.renderBody()}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    reports: state.list.reports
  };
};

const ReportsList = connect(
  mapStateToProps
)(ReportsListView);

export default ReportsList;
