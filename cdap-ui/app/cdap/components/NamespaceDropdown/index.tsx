/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';
import React from 'react';
import { Dropdown, DropdownMenu, DropdownToggle } from 'reactstrap';
import AbstractWizard from 'components/AbstractWizard';
import NamespaceStore, { INamespace, fetchNamespaceDetails } from 'services/NamespaceStore';
import SetPreferenceAction from 'components/FastAction/SetPreferenceAction';
import { PREFERENCES_LEVEL } from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';
import IconSVG from 'components/IconSVG';
import { MySearchApi } from 'api/search';
import isObject from 'lodash/isObject';
import sortBy from 'lodash/sortBy';
import T from 'i18n-react';
import { Link } from 'react-router-dom';
import uuidV4 from 'uuid/v4';
import globalEvents from 'services/global-events';
import ee from 'event-emitter';
import EntityType from 'services/metadata-parser/EntityType';
import { preventPropagation } from 'services/helpers';
import { Theme } from 'services/ThemeHelper';
import If from 'components/If';
import classnames from 'classnames';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
require('./NamespaceDropdown.scss');

interface INamespaceDropdownProps {
  tag?: React.ReactNode;
  onNamespaceCreate?: () => void;
  onNamespacePreferenceEdit?: () => void;
  onNamespaceChange?: () => void;
}
interface INamespaceDropdownState {
  openDropdown: boolean;
  openWizard: boolean;
  openPreferenceWizard: boolean;
  numMetricsLoading: boolean;
  preferencesSavedMessage: boolean;
  numApplications: number;
  numDatasets: number;
  namespaceList: INamespace[];
  defaultNamespace: string;
  currentNamespace: string;
  error: string;
}

const getBaseResolvedUrl = (url: string): string => {
  const urlparams = window.location.pathname.split('/').filter((a) => a);
  if (['pipelines', 'metadata', 'logviewer'].indexOf(urlparams[0]) !== -1) {
    return `/cdap/${url}`;
  }
  return `/${url}`;
};

export default class NamespaceDropdown extends React.PureComponent<
  INamespaceDropdownProps,
  INamespaceDropdownState
> {
  constructor(props) {
    super(props);

    this.eventEmitter.on(globalEvents.CREATENAMESPACE, () => {
      this.setState({
        openWizard: true,
      });
    });
  }

  private lowerCaseNamespace = (namespace) => {
    return namespace.name.toLowerCase();
  };

  public state: INamespaceDropdownState = {
    openDropdown: false,
    openWizard: false,
    openPreferenceWizard: false,
    preferencesSavedMessage: false,
    namespaceList: sortBy(NamespaceStore.getState().namespaces, [
      this.lowerCaseNamespace,
    ]) as INamespace[],
    currentNamespace: NamespaceStore.getState().selectedNamespace,
    defaultNamespace: localStorage.getItem('DefaultNamespace'),
    numMetricsLoading: false,
    numApplications: 0,
    numDatasets: 0,
    error: '',
  };

  private eventEmitter = ee(ee);

  private apiSubscription;

  private subscription = NamespaceStore.subscribe(() => {
    const selectedNamespace = NamespaceStore.getState().selectedNamespace;
    const namespaces = NamespaceStore.getState().namespaces.map((ns) => ns.name);
    if (namespaces.indexOf(selectedNamespace) === -1) {
      this.setState({
        currentNamespace: '--',
        namespaceList: sortBy(NamespaceStore.getState().namespaces, [
          this.lowerCaseNamespace,
        ]) as INamespace[],
      });
    } else {
      // have to set this, because the Namespace store gets reset when we visit other apps
      // e.g. Hydrator or Tracker
      localStorage.setItem('CurrentNamespace', selectedNamespace);
      this.setState({
        currentNamespace: NamespaceStore.getState().selectedNamespace,
        namespaceList: sortBy(NamespaceStore.getState().namespaces, [
          this.lowerCaseNamespace,
        ]) as INamespace[],
      });
    }
  });
  public componentWillUnmount() {
    this.subscription();
    if (this.apiSubscription) {
      this.apiSubscription.unsubscribe();
    }
  }

  private selectNamespace = (name) => {
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace: name,
      },
    });
    this.onNamespaceChange(name);
  };

  private onNamespaceChange = (namespace) => {
    // On namespace change, reset if there are any existing authorization
    // message and recheck for the newly selected namespace.
    this.eventEmitter.emit(globalEvents.PAGE_LEVEL_ERROR, { reset: true });
    fetchNamespaceDetails(namespace);
    if (this.props.onNamespaceChange) {
      this.props.onNamespaceChange();
    }
  };

  private toggle = () => {
    if (!this.state.openPreferenceWizard) {
      if (this.state.openDropdown === false) {
        this.getNumMetrics();
      }
      this.setState({
        openDropdown: !this.state.openDropdown,
      });
      document.querySelector('.namespace-list').scrollTop = 0;
    }
  };

  private showNamespaceWizard = () => {
    this.setState(
      {
        openWizard: !this.state.openWizard,
        openDropdown: !this.state.openDropdown,
      },
      () => {
        const { onNamespaceCreate } = this.props;
        if (onNamespaceCreate && typeof onNamespaceCreate === 'function') {
          onNamespaceCreate();
        }
      }
    );
  };

  private hideNamespaceWizard = () => {
    this.setState({
      openWizard: false,
    });
  };

  private preferenceWizardIsOpen = (openState) => {
    this.setState({ openPreferenceWizard: openState }, () => {
      const { onNamespacePreferenceEdit } = this.props;
      if (onNamespacePreferenceEdit && typeof onNamespacePreferenceEdit === 'function') {
        onNamespacePreferenceEdit();
      }
    });
  };

  private preferencesAreSaved = () => {
    this.setState({ preferencesSavedMessage: true });
    setTimeout(() => {
      this.setState({ preferencesSavedMessage: false });
    }, 3000);
  };

  private setDefault = (clickedNamespace, event) => {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
    if (this.state.defaultNamespace !== clickedNamespace) {
      this.setState({
        defaultNamespace: clickedNamespace,
      });
      localStorage.setItem('DefaultNamespace', clickedNamespace);
    }
  };

  private getNumMetrics = () => {
    this.setState({ numMetricsLoading: true });
    const params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      target: ['application', 'dataset'],
      query: '*',
      sort: 'entity-name asc',
      responseFormat: 'v6',
    };
    let numApplications = 0;
    let numDatasets = 0;
    this.apiSubscription = MySearchApi.search(params).subscribe(
      (res) => {
        res.results.forEach((entityObj) => {
          const entityType = entityObj.entity.type;
          if (entityType === EntityType.application) {
            numApplications += 1;
          } else {
            numDatasets += 1;
          }
        });
        this.setState({
          numApplications,
          numDatasets,
          numMetricsLoading: false,
        });
      },
      (error) => {
        this.setState({
          error: isObject(error) ? error.response : error,
        });
      }
    );
  };

  public render() {
    const LinkEl = this.props.tag ? this.props.tag : Link;
    const defaultNamespace = this.state.defaultNamespace;
    const currentNamespace = this.state.currentNamespace;
    const isValidNamespace = NamespaceStore.getState().namespaces.filter(
      (ns) => ns.name === currentNamespace
    ).length;
    const currentNamespaceCardHeader = (
      <div>
        <span className="current-namespace-name">{currentNamespace}</span>
        <span className="current-namespace-default">
          {defaultNamespace === currentNamespace ? (
            <span>
              <span className="default-status">(Default)</span>
              <IconSVG name="icon-star" onClick={this.setDefault.bind(this, currentNamespace)} />
            </span>
          ) : (
            <span>
              <span className="default-status">(Set Default)</span>
              <IconSVG name="icon-star-o" onClick={this.setDefault.bind(this, currentNamespace)} />
            </span>
          )}
        </span>
      </div>
    );
    const preferenceSpecificCardHeader = (
      <div className="preferences-saved-message">
        <span>
          {T.translate('features.FastAction.SetPreferences.success', { entityType: 'Namespace' })}
        </span>
        <IconSVG
          name="icon-close"
          onClick={(e) => {
            preventPropagation(e);
            this.setState({ preferencesSavedMessage: false });
          }}
        />
      </div>
    );
    return (
      <div
        className={classnames('namespace-dropdown', {
          opened: this.state.openDropdown,
        })}
      >
        <Dropdown isOpen={this.state.openDropdown} toggle={this.toggle} inNavbar={true}>
          <DropdownToggle className="current-namespace" tag="div" onClick={this.toggle}>
            <div className="namespace-text">
              <div>{T.translate('features.Navbar.NamespaceDropdown.namespaceLabel')}</div>
            </div>
            <div className="namespace-and-caret">
              <span title={currentNamespace}>{currentNamespace}</span>
              <IconSVG name="icon-caret-down" />
            </div>
          </DropdownToggle>
          <DropdownMenu>
            {isValidNamespace ? (
              <div className="current-namespace-details">
                <LinkEl
                  to={`/ns/${currentNamespace}/details`}
                  href={`/cdap/ns/${currentNamespace}/details`}
                >
                  <div
                    className="current-namespace-metadata"
                    onClick={this.onNamespaceChange.bind(this, currentNamespace)}
                  >
                    {this.state.preferencesSavedMessage === true
                      ? preferenceSpecificCardHeader
                      : currentNamespaceCardHeader}

                    <div className="current-namespace-metrics">
                      <table>
                        <thead>
                          <tr>
                            <th>{T.translate('features.Navbar.NamespaceDropdown.applications')}</th>
                            <th>{T.translate('features.Navbar.NamespaceDropdown.datasets')}</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td>
                              {this.state.numMetricsLoading ? (
                                <IconSVG name="icon-spinner" className="fa-spin" />
                              ) : (
                                this.state.numApplications
                              )}
                            </td>
                            <td>
                              {this.state.numMetricsLoading ? (
                                <IconSVG name="icon-spinner" className="fa-spin" />
                              ) : (
                                this.state.numDatasets
                              )}
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                </LinkEl>
                <div className="current-namespace-preferences text-center">
                  <h4 className="btn-group">
                    <SetPreferenceAction
                      setAtLevel={PREFERENCES_LEVEL.NAMESPACE}
                      modalIsOpen={this.preferenceWizardIsOpen.bind(this)}
                      onSuccess={this.preferencesAreSaved.bind(this)}
                      savedMessageState={this.state.preferencesSavedMessage}
                    />
                  </h4>
                </div>
              </div>
            ) : null}
            <div className="namespace-list">
              {this.state.namespaceList
                .filter((item: INamespace) => item.name !== currentNamespace)
                .map((item: INamespace) => {
                  const starIcon = defaultNamespace === item.name ? 'icon-star' : 'icon-star-o';
                  let url = `ns/${item.name}`;
                  url = `${getBaseResolvedUrl(url)}`;
                  return (
                    <div className="clearfix namespace-container" key={uuidV4()}>
                      <LinkEl href={url} to={url} className="namespace-link">
                        <span
                          className="namespace-name float-left"
                          onClick={this.selectNamespace.bind(this, item.name)}
                        >
                          {item.name}
                        </span>
                      </LinkEl>
                      <span
                        className="default-ns-section float-right"
                        onClick={this.setDefault.bind(this, item.name)}
                      >
                        <IconSVG name={starIcon} />
                      </span>
                    </div>
                  );
                })}
            </div>
            <If condition={Theme.showAddNamespace}>
              <div className="namespace-action text-center" onClick={this.showNamespaceWizard}>
                {T.translate('features.Navbar.NamespaceDropdown.addNS')}
              </div>
            </If>
          </DropdownMenu>
        </Dropdown>

        <AbstractWizard
          isOpen={this.state.openWizard}
          onClose={this.hideNamespaceWizard}
          wizardType="add_namespace"
        />
      </div>
    );
  }
}
