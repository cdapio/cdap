/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import React from 'react';
import { CSSTransition } from 'react-transition-group';
import { Modal, ModalBody, ModalFooter, ModalHeader } from 'reactstrap';
import { Theme } from 'services/ThemeHelper';

const styles = (): StyleRules => {
  return {};
};

interface IStandardModalProps extends WithStyles<typeof styles> {
  widgetID: string;
}

const StandardModalView: React.FC<IStandardModalProps> = ({ classes, children }) => {
  // onError = (error, extendedError) => {
  //   this.setState({
  //     error,
  //     extendedError,
  //   });
  // };
  const [error, setError] = React.useState(null);
  const renderError = () => {
    if (!error) {
      return null;
    }

    return (
      <ModalFooter>
        {/* <CardActionFeedback
          type="DANGER"
          message={this.state.error}
          extendedMessage={this.state.extendedError}
        /> */}
      </ModalFooter>
    );
  };

  const market = Theme.featureNames.hub;
  const resourceCenter = T.translate('commons.resource-center');

  return (
    <Modal
      isOpen={true} // this.props.isOpen}
      toggle={() => {}} // this.closeHandler.bind(this)}
      className={classnames('plus-button-modal cdap-modal', {
        // 'cask-market': this.state.viewMode === 'marketplace',
        // 'add-entity-modal': this.state.viewMode === 'resourcecenter',
      })}
      size="lg"
      zIndex="1061"
      fade
    >
      <ModalHeader>
        <span className="float-left">
          <span className="plus-modal-header-text">
            {/* {this.state.viewMode === 'resourcecenter' ? resourceCenter : market} */}
            Hellommmm
          </span>
        </span>
        <div className="float-right">
          <div className="modal-close-btn" onClick={() => {}}>
            <IconSVG name="icon-close" />
          </div>
        </div>
      </ModalHeader>
      <ModalBody>
        <CSSTransition
          transitionName="plus-button-modal-content"
          transitionEnterTimeout={500}
          transitionLeaveTimeout={300}
          timeout={5000}
          component="div"
        >
          <div>
            hello
            {/* {this.state.viewMode === 'marketplace' ? (
            <Market key="1" />
          ) : (
            <ResourceCenter key="2" onError={this.onError} />
          )} */}
            {children}
          </div>
        </CSSTransition>
      </ModalBody>
      {renderError()}
    </Modal>
  );
};

const StandardModal = withStyles(styles)(StandardModalView);
export default StandardModal;
