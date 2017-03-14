/*
 * Copyright © 2017 Cask Data, Inc.
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
import FastActionButton from '../FastActionButton';
import T from 'i18n-react';
import { Tooltip } from 'reactstrap';
import SendEventModal from 'components/FastAction/SendEventAction/SendEventModal';
require('./SendEventAction.scss');

export default class SendEventAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      modal: false
    };

    this.eventText = '';
    this.toggleModal = this.toggleModal.bind(this);
    this.toggleTooltip = this.toggleTooltip.bind(this);
  }

  componentWillMount() {
    if (this.props.opened) {
      this.setState({modal: true});
    }
  }

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }

  toggleModal(event) {
    this.setState({
      modal: !this.state.modal,
    });
    if (event) {
      event.stopPropagation();
      event.nativeEvent.stopImmediatePropagation();
    }
  }

  render() {
    let tooltipID = `${this.props.entity.uniqueId}-sendevents`;
    return (
      <span className="btn btn-secondary btn-sm">
        <FastActionButton
          icon="fa fa-upload"
          action={this.toggleModal}
          id={tooltipID}
        />
        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          target={tooltipID}
          toggle={this.toggleTooltip}
          delay={0}
        >
          {T.translate('features.FastAction.sendEventsLabel')}
        </Tooltip>

        {
          this.state.modal ? (
            <SendEventModal
              entity={this.props.entity}
              onClose={this.toggleModal}
            />
          ) : null
        }
      </span>
    );
  }
}

SendEventAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onSuccess: PropTypes.func,
  opened: PropTypes.bool
};
