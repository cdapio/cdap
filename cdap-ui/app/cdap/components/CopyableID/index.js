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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import Popover from 'components/Popover';
import Clipboard from 'clipboard';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import uuidV4 from 'uuid/v4';
import classnames from 'classnames';
import { preventPropagation } from 'services/helpers';

require('./CopyableID.scss');

const PREFIX = `features.CopyableID`;

export default class CopyableID extends Component {
  static propTypes = {
    id: PropTypes.string.isRequired,
    idprefix: PropTypes.string,
    label: PropTypes.string,
    placement: PropTypes.string,
    tooltipText: PropTypes.oneOfType([PropTypes.string, PropTypes.bool]),
    bubbleEvent: PropTypes.bool,
  };

  static defaultProps = {
    label: T.translate(`${PREFIX}.label`),
    tooltipText: true,
    bubbleEvent: false,
  };

  state = {
    showTooltip: false,
  };

  onIDClickHandler = (e) => {
    this.setState({
      showTooltip: !this.state.showTooltip,
    });
    if (!this.props.bubbleEvent) {
      preventPropagation(e);
      return false;
    }
  };

  // When the user clicks the link we show a "Copied!" message
  // This has to go away after moving away so that we show the
  // tooltip with the original message.
  onPopoverClose = (showPopover) => {
    if (!showPopover) {
      this.setState({
        showTooltip: showPopover,
      });
    }
  };

  renderToolTipText() {
    if (!this.props.tooltipText) {
      return null;
    }

    if (typeof this.props.tooltipText === 'string' && this.props.tooltipText) {
      return this.props.tooltipText;
    }
    if (this.props.id) {
      return this.props.id;
    }
    return T.translate(`${PREFIX}.notAvailable`);
  }

  render() {
    let idlabel = `A-${uuidV4()}`;
    if (this.props.idprefix) {
      idlabel = `${this.props.idprefix}-${this.props.id}`;
    }
    // FIXME: Not sure how else to do this. Looks adhoc. Need this for copy to clipboard.
    new Clipboard(`#${idlabel}`);
    const target = (
      <span
        className="copyable-id btn-link"
        id={idlabel}
        onClick={this.onIDClickHandler}
        data-clipboard-text={this.props.id}
      >
        <span>{this.props.label}</span>
      </span>
    );
    return (
      <Popover
        target={() => target}
        showOn="Hover"
        bubbleEvent={false}
        placement="right"
        className={classnames('copyable-id', {
          hide: !this.props.tooltipText && !this.state.showTooltip,
        })}
        showPopover={this.state.showTooltip}
        modifiers={{
          shift: {
            order: 800,
            enabled: true,
          },
        }}
        onTogglePopover={this.onPopoverClose}
      >
        <span>{this.renderToolTipText()}</span>
        {this.state.showTooltip ? (
          <div className="copied-label text-success">
            <IconSVG name="icon-check-circle" />
            <span>{T.translate(`${PREFIX}.copiedLabel`)}</span>
          </div>
        ) : null}
      </Popover>
    );
  }
}
