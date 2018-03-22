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
import Popover from 'components/Popover';
import NamespaceStore, {getCurrentNamespace} from 'services/NamespaceStore';
import IconSVG from 'components/IconSVG';
import {setNamespacesPick} from 'components/OpsDashboard/store/ActionCreator';
import {connect} from 'react-redux';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';

class NamespacesPopoverView extends Component {
  static propTypes = {
    namespacesPick: PropTypes.array
  };

  state = {
    namespace: getCurrentNamespace(),
    namespaces: this.getNamespaceList(),
  };

  componentWillMount() {
    this.namespaceStoreSub = NamespaceStore.subscribe(() => {
      this.setState({
        namespaces: this.getNamespaceList()
      });
    });
  }

  componentWillUnmount() {
    if (this.namespaceStoreSub) {
      this.namespaceStoreSub();
    }
  }

  getNamespaceList() {
    return NamespaceStore.getState().namespaces
      .map(ns => ns.name)
      .filter(ns => ns !== this.state.namespace);
  }

  namespaceClick = (ns) => {
    let index = this.props.namespacesPick.indexOf(ns);
    let namespacesPick = [...this.props.namespacesPick];
    if (index === -1) {
      namespacesPick.push(ns);
    } else {
      namespacesPick.splice(index, 1);
    }

    setNamespacesPick(namespacesPick);
  };

  selectAll = () => {
    setNamespacesPick(this.state.namespaces);
  };

  clearAll = () => {
    setNamespacesPick([]);
  };

  render() {
    return (
      <Popover
        target={() => <div className="monitor-more text-xs-right"> Monitor More </div>}
        className="namespaces-list-popover"
        placement="top"
        bubbleEvent={false}
        enableInteractionInPopover={true}
      >
        <div className="popover-content">
          <div className="title">
            Select Namespaces to monitor
          </div>

          <div className="namespaces-count">
            {this.state.namespaces.length + 1} namespaces
          </div>

          <div className="namespaces-list">
            <div className="namespace-row non-selectable">
              <div className="checkbox-column">
                <UncontrolledDropdown
                  className="toggle-all-dropdown"
                >
                  <DropdownToggle className="dropdown-toggle-btn">
                    <IconSVG name="icon-caret-square-o-down" />
                  </DropdownToggle>
                  <DropdownMenu>
                    <DropdownItem
                      className="toggle-option"
                      onClick={this.selectAll}
                    >
                      Select All
                    </DropdownItem>
                    <DropdownItem
                      className="toggle-option"
                      onClick={this.clearAll}
                    >
                      Clear All
                    </DropdownItem>
                  </DropdownMenu>
                </UncontrolledDropdown>
              </div>

              <div className="namespace-section">
                Namespace name
              </div>
            </div>

            <hr />

            <div className="list">
              <div className="namespace-row non-selectable">
                <div className="checkbox-column">
                  <IconSVG name="icon-check" />
                </div>
                <div className="namespace-section">
                  {this.state.namespace}
                </div>
              </div>

              {
                this.state.namespaces.map((ns) => {
                  let isPicked = this.props.namespacesPick.indexOf(ns) !== -1;

                  return (
                    <div
                      className="namespace-row"
                      onClick={this.namespaceClick.bind(this, ns)}
                    >
                      <div className="checkbox-column">
                        <IconSVG name={isPicked ? 'icon-check-square' : 'icon-square-o'} />
                      </div>
                      <div className="namespace-section">
                        {ns}
                      </div>
                    </div>
                  );
                })
              }
            </div>
          </div>
        </div>
      </Popover>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    namespacesPick: state.namespaces.namespacesPick
  };
};

const NamespacesPopover = connect(
  mapStateToProps
)(NamespacesPopoverView);

export default NamespacesPopover;
