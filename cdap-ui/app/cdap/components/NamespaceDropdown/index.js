/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import {Dropdown, DropdownMenu} from 'reactstrap';
import AbstractWizard from 'components/AbstractWizard';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import SetPreferenceAction from 'components/FastAction/SetPreferenceAction';
import {MySearchApi} from 'api/search';
import isObject from 'lodash/isObject';
import sortBy from 'lodash/sortBy';
import T from 'i18n-react';
import {Link} from 'react-router';
const shortid = require('shortid');
import globalEvents from 'services/global-events';
import ee from 'event-emitter';
require('./NamespaceDropdown.scss');

export default class NamespaceDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      openDropdown: false,
      openWizard: false,
      openPreferenceWizard: false,
      preferencesSavedMessage: false,
      namespaceList: sortBy(NamespaceStore.getState().namespaces, this.lowerCaseNamespace),
      currentNamespace: NamespaceStore.getState().selectedNamespace,
      defaultNamespace: localStorage.getItem('DefaultNamespace')
    };

    this.subscription = NamespaceStore.subscribe(() => {
      let selectedNamespace = NamespaceStore.getState().selectedNamespace;
      let namespaces = NamespaceStore.getState().namespaces.map(ns => ns.name);
      if (namespaces.indexOf(selectedNamespace) === -1) {
        this.setState({
          currentNamespace: '--',
          namespaceList : sortBy(NamespaceStore.getState().namespaces, this.lowerCaseNamespace)
        });
      } else {
        // have to set this, because the Namespace store gets reset when we visit other apps
        // e.g. Hydrator or Tracker
        localStorage.setItem('CurrentNamespace', selectedNamespace);
        this.setState({
          currentNamespace : NamespaceStore.getState().selectedNamespace,
          namespaceList : sortBy(NamespaceStore.getState().namespaces, this.lowerCaseNamespace)
        });
      }
    });

    this.toggle = this.toggle.bind(this);
    this.showNamespaceWizard = this.showNamespaceWizard.bind(this);
    this.hideNamespaceWizard = this.hideNamespaceWizard.bind(this);
    this.eventEmitter = ee(ee);
    this.eventEmitter.on(globalEvents.CREATENAMESPACE, () => {
      this.setState({
        openWizard: true
      });
    });
  }
  componentWillUnmount() {
    this.subscription();
    if (this.apiSubscription) {
      this.apiSubscription.dispose();
    }
  }
  toggle() {
    if (!this.state.openPreferenceWizard) {
      if (this.state.openDropdown === false) {
        this.getNumMetrics();
      }
      this.setState({
        openDropdown: !this.state.openDropdown
      });
      document.querySelector('.namespace-list').scrollTop = 0;
    }
  }
  showNamespaceWizard() {
    this.setState({
      openWizard: !this.state.openWizard,
      openDropdown: !this.state.openDropdown
    });
  }
  hideNamespaceWizard() {
    this.setState({
      openWizard: false
    });
  }
  lowerCaseNamespace(namespace) {
    return namespace.name.toLowerCase();
  }
  preferenceWizardIsOpen(openState) {
    this.setState({openPreferenceWizard: openState});
  }
  selectNamespace(name) {
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace : name
      }
    });
    this.toggle();
  }
  preferencesAreSaved() {
    this.setState({preferencesSavedMessage: true});
    setTimeout(() => {
      this.setState({preferencesSavedMessage: false});
    }, 3000);
  }
  setDefault(clickedNamespace, event) {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
    if (this.state.defaultNamespace !== clickedNamespace) {
      this.setState({
        defaultNamespace: clickedNamespace
      });
      localStorage.setItem('DefaultNamespace', clickedNamespace);
    }
  }
  getNumMetrics() {
    this.setState({numMetricsLoading: true});
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      target: ['app', 'dataset', 'stream'],
      query: "*"
    };
    let numApplications = 0;
    let numStreams = 0;
    let numDatasets = 0;
    this.apiSubscription =
      MySearchApi
        .search(params)
        .subscribe(
          (res) => {
            res.results.forEach((entity) => {
              let entityType = entity.entityId.type;
              if (entityType === 'application') {
                numApplications += 1;
              } else if (entityType === 'stream') {
                numStreams += 1;
              } else {
                numDatasets += 1;
              }
            });
            this.setState({
              numApplications,
              numStreams,
              numDatasets,
              numMetricsLoading: false
            });
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        );
  }
  render() {
    let LinkEl = Link;
    let baseurl = '';
    if (this.props.tag) {
      let basename = document.querySelector('base');
      let baseurlname = basename.getAttribute('href');
      // FIXME: This is a one of thing (an interim solution) and that should go away in subsequent releases.
      if (baseurlname.indexOf('logviewer') !== -1) {
        baseurlname = '/cdap/';
      }
      basename = baseurlname ? baseurlname : null;
      LinkEl = this.props.tag;
      baseurl = `${basename}`;
    }
    const defaultNamespace = this.state.defaultNamespace;
    const currentNamespace = this.state.currentNamespace;
    let isValidNamespace = NamespaceStore.getState().namespaces.filter(ns => ns.name === currentNamespace).length;
    let currentNamespaceCardHeader = (
      <div>
        <span className="current-namespace-name">{currentNamespace}</span>
        <span className="current-namespace-default">
          {
            defaultNamespace === currentNamespace ?
              (
                <span>
                  <span className="default-status">(Default)</span>
                  <i
                    className="fa fa-star"
                    onClick={this.setDefault.bind(this, currentNamespace)}
                  />
                </span>
              )
            :
              (
                <span>
                  <span className="default-status">(Set Default)</span>
                  <i
                    className="fa fa-star-o"
                    onClick={this.setDefault.bind(this, currentNamespace)}
                  />
                </span>
              )
          }
        </span>
      </div>
    );
    let preferenceSpecificCardHeader = (
      <div className="preferences-saved-message text-white">
        <span>{T.translate('features.FastAction.setPreferencesSuccess.default', {entityType: 'Namespace'})}</span>
        <span
          className='fa fa-times'
          onClick={() => this.setState({preferencesSavedMessage: false})}
        />
      </div>
    );
    return (
      <div className="namespace-dropdown">
        <Dropdown
          isOpen={this.state.openDropdown}
          toggle={this.toggle}
        >
          <div
            className="current-namespace"
            onClick={this.toggle}
          >
            <div className="namespace-text">
              <small>{T.translate('features.Navbar.NamespaceDropdown.namespaceLabel')}</small>
              <span>{currentNamespace}</span>
            </div>
            <span className="fa fa-caret-down float-xs-right" />
          </div>
          <DropdownMenu>
            {
              isValidNamespace ?
                (
                  <div className="current-namespace-details">
                    <div className="current-namespace-metadata">
                      {
                        this.state.preferencesSavedMessage === true ?
                          preferenceSpecificCardHeader
                        :
                          currentNamespaceCardHeader
                      }

                      <div className="current-namespace-metrics">
                        <table>
                          <thead>
                            <tr>
                              <th>{T.translate('features.Navbar.NamespaceDropdown.applications')}</th>
                              <th>{T.translate('features.Navbar.NamespaceDropdown.datasets')}</th>
                              <th>{T.translate('features.Navbar.NamespaceDropdown.streams')}</th>
                            </tr>
                          </thead>
                          <tbody>
                            <tr>
                              <td>
                                {
                                  this.state.numMetricsLoading ?
                                    <span className = "fa fa-spinner fa-spin" />
                                  :
                                    this.state.numApplications
                                }
                              </td>
                              <td>
                                {
                                  this.state.numMetricsLoading ?
                                    <span className = "fa fa-spinner fa-spin" />
                                  :
                                    this.state.numDatasets
                                }
                              </td>
                              <td>
                                {
                                  this.state.numMetricsLoading ?
                                    <span className = "fa fa-spinner fa-spin" />
                                  :
                                    this.state.numStreams
                                }
                              </td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                    </div>
                    <div className="current-namespace-preferences text-xs-center">
                      <h4 className="btn-group">
                        <SetPreferenceAction
                          setAtNamespaceLevel={true}
                          modalIsOpen={this.preferenceWizardIsOpen.bind(this)}
                          onSuccess={this.preferencesAreSaved.bind(this)}
                          savedMessageState={this.state.preferencesSavedMessage}
                        />
                      </h4>
                    </div>
                  </div>
                )
              :
                null
            }
            <div className="namespace-list">
              {
                this.state.namespaceList
                  .filter(item => item.name !== currentNamespace)
                  .map( (item) => {
                    let starClass = defaultNamespace === item.name ? "fa fa-star": "fa fa-star-o";
                    return (
                      <div
                        className="clearfix namespace-container"
                        key={shortid.generate()}
                      >
                        <LinkEl
                          href={baseurl + `ns/${item.name}`}
                          to={baseurl + `/ns/${item.name}`}
                          className="namespace-link"
                        >
                          <span
                            className="namespace-name float-xs-left"
                            onClick={this.selectNamespace.bind(this, item.name)}
                          >
                            {item.name}
                          </span>
                        </LinkEl>
                        <span
                          className="default-ns-section float-xs-right"
                          onClick={this.setDefault.bind(this, item.name)}
                        >
                          <span className={starClass} />
                        </span>
                      </div>
                    );
                  })
              }
            </div>
            <div
              className="namespace-action text-xs-center"
              onClick={this.showNamespaceWizard}
            >
              {T.translate('features.Navbar.NamespaceDropdown.addNS')}
            </div>
          </DropdownMenu>
        </Dropdown>

        <AbstractWizard
          isOpen={this.state.openWizard}
          onClose={this.hideNamespaceWizard}
          wizardType='add_namespace'
        />
    </div>
    );
  }
}

NamespaceDropdown.propTypes = {
  tag: PropTypes.node
};

NamespaceDropdown.defaultProps = {
  tag: null
};
