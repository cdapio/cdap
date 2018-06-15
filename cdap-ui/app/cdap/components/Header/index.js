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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import globalEvents from 'services/global-events';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import {MyNamespaceApi} from 'api/namespace';
import {objectQuery} from 'services/helpers';
import ee from 'event-emitter';
import NamespaceDropdown from 'components/NamespaceDropdown';
import { withStyles } from '@material-ui/core/styles';
import headerStyles from './Header.scss';

require('./Header.scss');

const styles = {
  appBar: {
    backgroundColor: headerStyles.headerBgColor
  },
  appBarContainer: {
    minHeight: 'unset'
  }
};

class Header extends Component {

  static propTypes = {
    classes: PropTypes.object
  };
  eventEmitter = ee(ee);

  state = {
    currentNamespace: NamespaceStore.getState().selectedNamespace,
  };

  componentWillMount() {
    // Polls for namespace data
    this.namespacesubscription = MyNamespaceApi.pollList()
      .subscribe(
        (res) => {
          if (res.length > 0) {
            NamespaceStore.dispatch({
              type: NamespaceActions.updateNamespaces,
              payload: {
                namespaces : res
              }
            });
          } else {
            // TL;DR - This is emitted for Authorization in main.js
            // This means there is no namespace for the user to work on.
            // which indicates she/he have no authorization for any namesapce in the system.
            this.eventEmitter.emit(globalEvents.NONAMESPACE);
          }
        }
      );
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace = getLastSelectedNamespace();
      let {namespaces} = NamespaceStore.getState();
      if (selectedNamespace === 'system') {
        selectedNamespace = objectQuery(namespaces, 0, 'name');
      }
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace
        });
      }
    });
  }
  componentWillUnmount() {
    this.nsSubscription();
    if (this.namespacesubscription) {
      this.namespacesubscription.unsubscribe();
    }
  }

  render() {
    const {classes} = this.props;
    return (
      <div className="cdap-header">
        <AppBar position="fixed" className={`global-navbar ${classes.appBar}`}>
          <Toolbar className={classes.appBarContainer}>
            <h4> CDAP </h4>
            <NamespaceDropdown />
          </Toolbar>
        </AppBar>
      </div>
    );
  }
}

export default withStyles(styles)(Header);
