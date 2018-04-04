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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {MySearchApi} from 'api/search';
import {MyNamespaceApi} from 'api/namespace';
import {getCustomAppPipelineDatasetCounts} from 'services/metadata-parser';
import IconSVG from 'components/IconSVG';
import LoadingSVG from 'components/LoadingSVG';
import AddNamespaceWizard from 'components/CaskWizards/AddNamespace';
import classnames from 'classnames';
import globalEvents from 'services/global-events';
import ee from 'event-emitter';
import ViewAllLabel from 'components/ViewAllLabel';
import T from 'i18n-react';
import isEqual from 'lodash/isEqual';

const PREFIX = 'features.Administration.Accordions.Namespace';

export default class NamespacesAccordion extends Component {
  state = {
    loading: this.props.loading,
    namespaceWizardOpen: false,
    namespacesInfo: [],
    viewAll: false
  };

  static propTypes = {
    namespaces: PropTypes.array,
    loading: PropTypes.bool,
    expanded: PropTypes.bool,
    onExpand: PropTypes.func
  };

  eventEmitter = ee(ee);

  componentDidMount() {
    this.eventEmitter.on(globalEvents.NAMESPACECREATED, this.fetchNamespacesAndGetData);
  }

  componentWillReceiveProps(nextProps) {
    if (!isEqual(this.props.namespaces, nextProps.namespaces)) {
      this.getNamespaceData(nextProps.namespaces);
    }
  }

  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.NAMESPACECREATED, this.fetchNamespacesAndGetData);
  }

  fetchNamespacesAndGetData = () => {
    MyNamespaceApi
      .list()
      .subscribe(
        (res) => this.getNamespaceData(res),
        (err) => console.log(err)
      );
  }

  getNamespaceData(namespaces) {
    let searchParams = {
      target: ['dataset', 'app'],
      query: '*'
    };

    let namespacesInfo = [];

    namespaces.forEach(namespace => {
      searchParams.namespace = namespace.name;
      MySearchApi
        .search(searchParams)
        .subscribe(
          (entities) => {
            let {
              pipelineCount,
              customAppCount,
              datasetCount
            } = getCustomAppPipelineDatasetCounts(entities);

            namespacesInfo.push({
              name: namespace.name,
              pipelineCount,
              customAppCount,
              datasetCount
            });
            this.setState({
              namespacesInfo,
              loading: false
            });
          },
          (err) => console.log(err)
        );
    });
  }

  toggleNamespaceWizard = () => {
    this.setState({
      namespaceWizardOpen: !this.state.namespaceWizardOpen
    });
  }

  toggleViewAll = () => {
    this.setState({
      viewAll: !this.state.viewAll
    });
  }

  renderLabel() {
    return (
      <div
        className="admin-config-container-toggle"
        onClick={this.props.onExpand}
      >
        <span className="admin-config-container-label">
          <IconSVG name={this.props.expanded ? "icon-caret-down" : "icon-caret-right"} />
          {
            this.state.loading ?
              (
                <h5>
                  {T.translate(`${PREFIX}.label`)}
                  <IconSVG name="icon-spinner" className="fa-spin" />
                </h5>
              )
            :
              <h5>{T.translate(`${PREFIX}.labelWithCount`, {count: this.state.namespacesInfo.length})}</h5>
          }
        </span>
        <span className="admin-config-container-description">
          {T.translate(`${PREFIX}.description`)}
        </span>
      </div>
    );
  }

  renderGrid() {
    if (this.state.loading) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    let namespacesInfo = [...this.state.namespacesInfo];

    if (!this.state.viewAll && namespacesInfo.length > 10) {
      namespacesInfo = namespacesInfo.slice(0, 10);
    }

    return (
      <div className="grid-wrapper">
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row">
              <strong>{T.translate('commons.nameLabel')}</strong>
              <strong>{T.translate(`${PREFIX}.customApps`)}</strong>
              <strong>{T.translate('commons.pipelines')}</strong>
              <strong>{T.translate('commons.entity.dataset.plural')}</strong>
            </div>
          </div>
          <div className="grid-body">
            {
              namespacesInfo.map((namespaceInfo, i) => {
                return (
                  <div className="grid-row" key={i}>
                    <div>{namespaceInfo.name}</div>
                    <div>{namespaceInfo.customAppCount}</div>
                    <div>{namespaceInfo.pipelineCount}</div>
                    <div>{namespaceInfo.datasetCount}</div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }

  renderContent() {
    if (!this.props.expanded) {
      return null;
    }

    return (
      <div className="admin-config-container-content namespaces-container-content">
        <button
          className="btn btn-secondary"
          onClick={this.toggleNamespaceWizard}
        >
          {T.translate(`${PREFIX}.create`)}
        </button>
        {this.renderGrid()}
        <ViewAllLabel
          arrayToLimit={this.state.namespacesInfo}
          limit={10}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {
          this.state.namespaceWizardOpen ?
            <AddNamespaceWizard
              isOpen={this.state.namespaceWizardOpen}
              onClose={this.toggleNamespaceWizard}
            />
          :
            null
        }
      </div>
    );
  }

  render() {
    return (
      <div className={classnames(
        "admin-config-container namespaces-container",
        {"expanded": this.props.expanded}
      )}>
        {this.renderLabel()}
        {this.renderContent()}
      </div>
    );
  }
}
