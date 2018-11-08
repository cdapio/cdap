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
import MyUserStoreApi from 'api/userstore';
import If from 'components/If';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import Tour from 'components/EntityListView/Tour';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';
import { Theme } from 'services/ThemeHelper';

import './Welcome.scss';

// Setting the store value to be an integer so that subsequent release, we can simply increment this number
// to show the guided tour again.
const USER_STORE_KEY = 'showWelcome';
const USER_STORE_VALUE = 1;
const SESSION_STORAGE_KEY = USER_STORE_KEY;
const SESSION_STORAGE_VALUE = 'false';
const PREFIX = 'features.NUX.Welcome';

interface IWelcomeState {
  showModal: boolean;
  showAgain: boolean;
}

export default class Welcome extends React.PureComponent<void, IWelcomeState> {
  public state: IWelcomeState = {
    showModal: false,
    showAgain: false,
  };

  public componentDidMount() {
    // Checking for session storage so that the welcome modal is not shown
    // when user is navigating back and forth between react and angular pages.
    const sessionValue = window.sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (sessionValue === SESSION_STORAGE_VALUE) {
      return;
    }

    MyUserStoreApi.get().subscribe((res) => {
      const storeValue = objectQuery(res, 'property', USER_STORE_KEY);

      if (!storeValue || storeValue !== USER_STORE_VALUE) {
        this.setState({
          showModal: true,
        });
      }
    });
  }

  private close = () => {
    this.setState({
      showModal: false,
    });

    window.sessionStorage.setItem(SESSION_STORAGE_KEY, SESSION_STORAGE_VALUE);

    this.saveUserState();
  };

  private startTour = () => {
    this.close();
    Tour.start();
  };

  private toggleShowAgain = () => {
    this.setState({
      showAgain: !this.state.showAgain,
    });
  };

  private saveUserState = () => {
    if (!this.state.showAgain) {
      return;
    }

    MyUserStoreApi.get().subscribe((res) => {
      const obj = {
        ...res.property,
        [USER_STORE_KEY]: USER_STORE_VALUE,
      };

      MyUserStoreApi.set(null, obj);
    });
  };

  public render() {
    return (
      <If condition={this.state.showModal}>
        <Modal isOpen={true} size="md" zIndex="1061" className="welcome-modal">
          <ModalHeader>
            <span className="header-text">
              {T.translate(`${PREFIX}.header`, {
                productName: Theme.productName,
              })}
            </span>

            <div className="close-section float-right">
              <IconSVG name="icon-close" onClick={this.close} />
            </div>
          </ModalHeader>

          <ModalBody>
            <p>{Theme.productDescription}</p>
            <p>{T.translate(`${PREFIX}.takeTour`)}</p>

            <div className="show-again-selection">
              <span onClick={this.toggleShowAgain}>
                <IconSVG name={this.state.showAgain ? 'icon-check-square' : 'icon-square-o'} />

                <span>{T.translate(`${PREFIX}.showAgainToggle`)}</span>
              </span>
            </div>

            <div className="action-buttons">
              <button className="btn btn-primary" onClick={this.startTour}>
                {T.translate(`${PREFIX}.startTour`)}
              </button>

              <button className="btn btn-secondary" onClick={this.close}>
                {T.translate(`${PREFIX}.close`)}
              </button>
            </div>
          </ModalBody>
        </Modal>
      </If>
    );
  }
}
