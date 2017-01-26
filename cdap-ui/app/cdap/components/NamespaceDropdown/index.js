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
import {MyAppApi} from 'api/app';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import isObject from 'lodash/isObject';
import T from 'i18n-react';
import {Link} from 'react-router';
const shortid = require('shortid');
require('./NamespaceDropdown.scss');
export default class NamespaceDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      openDropdown: false,
      openWizard: false,
      openPreferenceWizard: false,
      savedMessage: false,
      namespaceList: NamespaceStore.getState().namespaces,
      currentNamespace: NamespaceStore.getState().selectedNamespace,
      currentDefaultNamespace: localStorage.getItem('DefaultNamespace'),
      previousDefaultNamespace: null
    };

    this.subscription = NamespaceStore.subscribe(() => {
      this.setState({
        currentNamespace : NamespaceStore.getState().selectedNamespace,
        namespaceList : NamespaceStore.getState().namespaces
      });
      this.getNumApplications();
      this.getNumDatasets();
      this.getNumStreams();
    });

    this.apiSubscriptions = [];
    this.toggle = this.toggle.bind(this);
    this.showNamespaceWizard = this.showNamespaceWizard.bind(this);
    this.hideNamespaceWizard = this.hideNamespaceWizard.bind(this);

  }
  componentWillMount() {
    this.getNumApplications();
    this.getNumDatasets();
    this.getNumStreams();
  }
  componentWillUnmount() {
    this.subscription();
    this.apiSubscriptions.forEach(apiSubscription => apiSubscription.dispose());
  }
  toggle() {
    if (!this.state.openPreferenceWizard) {
      this.setState({
        openDropdown: !this.state.openDropdown
      });
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
  }
  preferencesAreSaved() {
    this.setState({savedMessage: true});
    setTimeout(() => {
      this.setState({savedMessage: false});
    }, 3000);
  }
  toggleDefault(clickedNamespace, event) {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();

    if (this.state.currentDefaultNamespace === clickedNamespace) {
      this.setState({
        currentDefaultNamespace: this.state.previousDefaultNamespace,
        previousDefaultNamespace: this.state.currentDefaultNamespace
      });
    } else {
      this.setState({
        previousDefaultNamespace: this.state.currentDefaultNamespace,
        currentDefaultNamespace: clickedNamespace
      });
      localStorage.setItem('DefaultNamespace', clickedNamespace);
    }
    return false;
  }
  getNumApplications() {
    this.setState({numApplicationsLoading: true});
    this.apiSubscriptions.push(
      MyAppApi.list({namespace: NamespaceStore.getState().selectedNamespace})
        .subscribe(
          (res) => {
            this.setState({
              numApplications: res.length,
              numApplicationsLoading: false
            });
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }
  getNumDatasets() {
    this.setState({numDatasetsLoading: true});
    this.apiSubscriptions.push(
      MyDatasetApi.list({namespace: NamespaceStore.getState().selectedNamespace})
        .subscribe(
          (res) => {
            this.setState({
              numDatasets: res.length,
              numDatasetsLoading: false
            });
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }
  getNumStreams() {
    this.setState({numStreamsLoading: true});
    this.apiSubscriptions.push(
      MyStreamApi.list({namespace: NamespaceStore.getState().selectedNamespace})
        .subscribe(
          (res) => {
            this.setState({
              numStreams: res.length,
              numStreamsLoading: false
            });
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }
  render() {
    let LinkEl = Link;
    let baseurl = '';
    if (this.props.tag) {
      let basename = document.querySelector('base');
      basename = basename.getAttribute('href') ? basename.getAttribute('href') : null;
      LinkEl = this.props.tag;
      baseurl = `${basename}`;
    }
    const defaultNamespace = this.state.currentDefaultNamespace;
    const currentNamespace = this.state.currentNamespace;
    return (
      <div>
        <Dropdown
          isOpen={this.state.openDropdown}
          toggle={this.toggle}
        >
          <div
            className="current-namespace"
            onClick={this.toggle}
          >
            <div className="namespace-text">
              {currentNamespace}
            </div>
            <span className="fa fa-angle-down float-xs-right" />
          </div>
          <DropdownMenu>
            <div className="current-namespace-details">
              <div className="current-namespace-metadata">
                {
                  this.state.savedMessage === true ?
                    (
                      <div className="preferences-saved-message text-white">
                        <span>{T.translate('features.FastAction.setPreferencesSuccess')}</span>
                        <span
                          className='fa fa-times'
                          onClick={() => this.setState({savedMessage: false})}
                        />
                      </div>
                    )
                  :
                    (
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
                                    onClick={this.toggleDefault.bind(this, currentNamespace)}
                                  >
                                  </i>
                                </span>
                              )
                            :
                              (
                                <span>
                                  <span className="default-status">(Set Default)</span>
                                  <i
                                    className="fa fa-star-o"
                                    onClick={this.toggleDefault.bind(this, currentNamespace)}
                                  >
                                  </i>
                                </span>
                              )
                          }
                        </span>
                      </div>
                    )
                }

                <div className="current-namespace-entities">
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
                            this.state.numApplicationsLoading ?
                              <span className = "fa fa-spinner fa-spin" />
                            :
                              this.state.numApplications
                          }
                        </td>
                        <td>
                          {
                            this.state.numDatasetsLoading ?
                              <span className = "fa fa-spinner fa-spin" />
                            :
                              this.state.numDatasets
                          }
                        </td>
                        <td>
                          {
                            this.state.numStreamsLoading ?
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
                <SetPreferenceAction
                  setAtNamespaceLevel={true}
                  modalIsOpen={this.preferenceWizardIsOpen.bind(this)}
                  onSuccess={this.preferencesAreSaved.bind(this)}
                  savedMessageState={this.state.savedMessage}/>
              </div>
            </div>
            <div className="namespace-list">
              {
                this.state.namespaceList
                  .filter(item => item.name !== currentNamespace)
                  .map( (item) => {
                    let starClass = defaultNamespace === item.name ? "fa fa-star": "fa fa-star-o";
                    return (

                      <div className="clearfix namespace-container">
                        <LinkEl
                          href={baseurl + `/ns/${item.name}`}
                          to={baseurl + `/ns/${item.name}`}
                          className="namespace-link"
                          key={shortid.generate()}
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
                          onClick={this.toggleDefault.bind(this, item.name)}
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
          backdrop={true}
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
