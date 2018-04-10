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
import SortableStickyGrid from 'components/SortableStickyGrid';
import {Link} from 'react-router-dom';
import uuidV4 from 'uuid/v4';
require('./NamespacesAccordion.scss');

const PREFIX = 'features.Administration.Accordions.Namespace';

const GRID_HEADERS = [
  {
    property: 'name',
    label: T.translate('commons.nameLabel')
  },
  {
    property: 'customAppCount',
    label: T.translate(`${PREFIX}.customApps`)
  },
  {
    property: 'pipelineCount',
    label: T.translate('commons.pipelines')
  },
  {
    property: 'datasetCount',
    label: T.translate('commons.entity.dataset.plural')
  }
];

const NUM_NS_TO_SHOW = 5;

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

    let currentNamespaces = this.state.namespacesInfo.map(namespace => namespace.name);
    let namespacesInfo = [];
    let hasNewNamespaces = false;

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

            let namespaceIsHighlighted = false;

            if (currentNamespaces.length && currentNamespaces.indexOf(namespace.name) === -1) {
              namespaceIsHighlighted = true;
              hasNewNamespaces = true;
            }

            namespacesInfo.push({
              name: namespace.name,
              pipelineCount,
              customAppCount,
              datasetCount,
              highlighted: namespaceIsHighlighted
            });

            this.setState({
              namespacesInfo,
              loading: false,
              viewAll: hasNewNamespaces || this.state.viewAll
            }, () => {
              if (hasNewNamespaces) {
                setTimeout(() => {
                  namespacesInfo = namespacesInfo.map(namespace => {
                    return {
                      ...namespace,
                      highlighted: false
                    };
                  });
                  this.setState({
                    namespacesInfo
                  });
                }, 4000);
              }
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

  renderGridBody = (namespaces) => {
    return (
      <div className="grid-body">
        {
          namespaces.map((namespace) => {
            return (
              <Link
                to={`/ns/${namespace.name}/details`}
                className={classnames(
                  "grid-row grid-link", {
                    "highlighted": namespace.highlighted
                  }
                )}
                key={uuidV4()}
              >
                {
                  GRID_HEADERS.map((header) => {
                    return (
                      <div key={uuidV4()}>
                        {namespace[header.property]}
                      </div>
                    );
                  })
                }
              </Link>
            );
          })
        }
      </div>
    );
  };

  renderGrid() {
    if (this.state.loading) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    let namespacesInfo = [...this.state.namespacesInfo];

    if (!this.state.viewAll && namespacesInfo.length > NUM_NS_TO_SHOW) {
      namespacesInfo = namespacesInfo.slice(0, NUM_NS_TO_SHOW);
    }

    return (
      <SortableStickyGrid
        entities={namespacesInfo}
        renderGridBody={this.renderGridBody}
        gridHeaders={GRID_HEADERS}
      />
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
        <ViewAllLabel
          arrayToLimit={this.state.namespacesInfo}
          limit={NUM_NS_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderGrid()}
        <ViewAllLabel
          arrayToLimit={this.state.namespacesInfo}
          limit={NUM_NS_TO_SHOW}
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
