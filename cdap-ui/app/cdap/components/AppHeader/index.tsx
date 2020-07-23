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

import * as React from 'react';
import PropTypes from 'prop-types';
import AppBar from '@material-ui/core/AppBar';
import classnames from 'classnames';
import AppDrawer from 'components/AppHeader/AppDrawer/AppDrawer';
import AppToolbar from 'components/AppHeader/AppToolBar/AppToolbar';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { MyNamespaceApi } from 'api/namespace';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import ee from 'event-emitter';
import { ISubscription } from 'rxjs/Subscription';
import { Unsubscribe } from 'redux';
import globalEvents from 'services/global-events';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import { SYSTEM_NAMESPACE } from 'services/global-constants';
import { objectQuery } from 'services/helpers';
import { NamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import ThemeWrapper from 'components/ThemeWrapper';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { loadDefaultExperiments } from 'components/Lab';

require('styles/bootstrap_4_patch.scss');

interface IMyAppHeaderState {
  toggleDrawer: boolean;
  currentNamespace: string;
}
const styles = (theme) => {
  return {
    grow: theme.grow,
    appbar: {
      backgroundColor: theme.navbarBgColor,
      zIndex: theme.zIndex.drawer + 1,
    },
  };
};

interface IMyAppHeaderProps extends WithStyles<typeof styles> {
  nativeLink: boolean;
}

class MyAppHeader extends React.PureComponent<IMyAppHeaderProps, IMyAppHeaderState> {
  public state: IMyAppHeaderState = {
    toggleDrawer: false,
    currentNamespace: '',
  };

  private namespacesubscription: ISubscription;
  private nsSubscription: Unsubscribe;
  private eventEmitter = ee(ee);

  public componentDidMount() {
    loadDefaultExperiments();
    // Polls for namespace data
    this.namespacesubscription = MyNamespaceApi.pollList().subscribe(
      (res) => {
        if (res.length > 0) {
          NamespaceStore.dispatch({
            type: NamespaceActions.updateNamespaces,
            payload: {
              namespaces: res,
            },
          });
        } else {
          // TL;DR - This is emitted for Authorization in main.js
          // This means there is no namespace for the user to work on.
          // which indicates she/he have no authorization for any namesapce in the system.
          this.eventEmitter.emit(globalEvents.NONAMESPACE);
        }
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('Error retrieving list of namespaces', err);
      }
    );
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace: string = getLastSelectedNamespace() as string;
      const { namespaces } = NamespaceStore.getState();
      if (selectedNamespace === SYSTEM_NAMESPACE) {
        selectedNamespace = objectQuery(namespaces, 0, 'name');
      }
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace,
        });
      }
    });
  }
  public componentWillUnmount() {
    this.nsSubscription();
    if (this.namespacesubscription) {
      this.namespacesubscription.unsubscribe();
    }
  }

  public toggleDrawer = () => {
    this.setState({
      toggleDrawer: !this.state.toggleDrawer,
    });
  };

  public componentDidNavigate = () => {
    this.eventEmitter.emit(globalEvents.CLOSEMARKET);
    this.toggleDrawer();
  };

  public render() {
    const { classes } = this.props;
    const namespaceLinkContext = {
      namespace: this.state.currentNamespace,
      isNativeLink: this.props.nativeLink,
    };
    // (TODO): This still doesn't capture correctly how we handle
    // when authorization is enabled and user has access to no namespaces.
    if (
      !this.state.currentNamespace ||
      (typeof this.state.currentNamespace === 'string' && !this.state.currentNamespace.length)
    ) {
      return <LoadingSVGCentered showFullPage />;
    }
    return (
      <AppBar
        position="fixed"
        className={classnames(classes.grow, classes.appbar)}
        data-cy="app-navbar"
      >
        <NamespaceLinkContext.Provider value={namespaceLinkContext}>
          <AppToolbar onMenuIconClick={this.toggleDrawer} nativeLink={this.props.nativeLink} />
          <AppDrawer
            open={this.state.toggleDrawer}
            onClose={this.toggleDrawer}
            componentDidNavigate={this.componentDidNavigate}
          />
        </NamespaceLinkContext.Provider>
      </AppBar>
    );
  }
}
const AppHeaderWithStyles = withStyles(styles)(MyAppHeader);
/**
 * Detecting environment is angular. If angular then add the theme wrapper
 * as MuiThemeProvider is not available at the root in angular apps
 *
 * In react app we add MuiThemeProvider at main.js which passes on the theme
 * via context to children down.
 *
 * Doing this here to enable us to transition to material design which supports
 * proper themeing and css modules for any old or new components we write.
 * As we move slowly everything to react we will remmove this check and
 * let the root pass on the theme to the children.
 */
export default function CustomHeader({ nativeLink }) {
  if (typeof window.angular !== 'undefined' && window.angular.version) {
    return (
      <ThemeWrapper>
        <AppHeaderWithStyles nativeLink={nativeLink} />
      </ThemeWrapper>
    );
  }
  return <AppHeaderWithStyles nativeLink={nativeLink} />;
}
// Apparently this is needed for ngReact
(CustomHeader as any).propTypes = {
  nativeLink: PropTypes.bool,
};
(CustomHeader as any).defaultProps = {
  nativeLink: false,
};
