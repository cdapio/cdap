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
import Customizer from 'components/Reports/Customizer';
import {humanReadableDate} from 'services/helpers';
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';
import Duration from 'components/Duration';
import { Link } from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {listReports} from 'components/Reports/store/ActionCreator';
import {Observable} from 'rxjs/Observable';
import classnames from 'classnames';
import ActionPopover from 'components/Reports/ReportsList/ActionPopover';
import NamespacesPicker from 'components/NamespacesPicker';
import {setNamespacesPick} from 'components/Reports/store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.Reports.ReportsList';

require('./ReportsList.scss');

class ReportsListView extends Component {
  static propTypes = {
    reports: PropTypes.array,
    activeId: PropTypes.string
  };

  componentWillMount() {
    listReports();
    this.interval$ = Observable.interval(10000)
      .subscribe(listReports);
  }

  componentWillUnmount() {
    if (this.interval$) {
      this.interval$.unsubscribe();
    }
  }

  renderCreated(report) {
    if (report.status === 'COMPLETED') {
      return humanReadableDate(report.created);
    }

    if (report.status === 'FAILED') {
      return T.translate(`${PREFIX}.failed`);
    }

    return (
      <div className="generating">
        <span className="fa fa-spin">
          <IconSVG name="icon-spinner" />
        </span>
        <span>{T.translate(`${PREFIX}.generating`)}</span>
      </div>
    );
  }

  renderExpiry(report) {
    if (!report.expiry) { return '--'; }

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
          <div>{T.translate('features.Reports.reportName')}</div>
          <div>{T.translate(`${PREFIX}.created`)}</div>
          <div>{T.translate(`${PREFIX}.expiration`)}</div>
          <div></div>
        </div>
      </div>
    );
  }

  renderLoadingRow(report) {
    return (
      <div
        key={report.id}
        className={classnames('grid-row grid-link not-allowed', {
          'active': report.id === this.props.activeId,
        })}
      >
        <div className="report-name">{report.name}</div>
        <div>
          {this.renderCreated(report)}
        </div>
        <div>
          {this.renderExpiry(report)}
        </div>
        <div>
          <ActionPopover report={report} />
        </div>
      </div>
    );
  }

  renderLinkRow(report) {
    return (
      <Link
        key={report.id}
        to={`/ns/${getCurrentNamespace()}/reports/details/${report.id}`}
        className={classnames('grid-row grid-link', {
          'active': report.id === this.props.activeId,
        })}
      >
        <div className="report-name">{report.name}</div>
        <div>
          {this.renderCreated(report)}
        </div>
        <div>
          {this.renderExpiry(report)}
        </div>
        <div>
          <ActionPopover report={report} />
        </div>
      </Link>
    );
  }

  renderBody() {
    return (
      <div className="grid-body">
        {
          this.props.reports.map((report) => {
            return report.status === 'RUNNING' ?
              this.renderLoadingRow(report) : this.renderLinkRow(report);
          })
        }
      </div>
    );
  }

  renderEmpty() {
    return (
      <div className="list-container empty">
        <div className="text-xs-center">
          {T.translate(`${PREFIX}.noReports`)}
        </div>
        <div className="text-xs-center">
          {T.translate(`${PREFIX}.makeSelection`)}
        </div>
      </div>
    );
  }

  renderTable() {
    if (this.props.reports.length === 0) {
      return this.renderEmpty();
    }

    return (
      <div className="list-container grid-wrapper">
        <div className="grid grid-container">
          {this.renderHeader()}
          {this.renderBody()}
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="reports-container">
        <div className="header">
          <div className="reports-view-options float-xs-left">
            <span>{T.translate('features.Reports.header')}</span>
          </div>

          <NamespacesPicker setNamespacesPick={setNamespacesPick} />
        </div>
        <div className="reports-list-container">
          <Customizer />

          <div className="list-view">
            <div className="section-title">
              {T.translate(`${PREFIX}.selectAReport`)}
            </div>

            {this.renderTable()}
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    reports: state.list.reports,
    activeId: state.list.activeId
  };
};

const ReportsList = connect(
  mapStateToProps
)(ReportsListView);

export default ReportsList;
