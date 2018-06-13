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
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import Clipboard from 'clipboard';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import uuidV4 from 'uuid/v4';

require('./CopyableID.scss');

const PREFIX = `features.CopyableID`;

export default class CopyableID extends Component {
  static propTypes = {
    id: PropTypes.string.isRequired,
    idprefix: PropTypes.string,
    label: PropTypes.string,
    placement: PropTypes.string,
    tooltipText: PropTypes.oneOfType([PropTypes.string, PropTypes.bool])
  };

  static defaultProps = {
    label: T.translate(`${PREFIX}.label`),
    tooltipText: true
  };

  state = {
    showTooltip: false
  };

  onIDClickHandler() {
    this.setState({
      showTooltip: !this.state.showTooltip
    });
  }

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

  renderTooltip(idlabel) {
    if (!this.props.tooltipText && !this.state.showTooltip) {
      return null;
    }

    let tetherConfig = {
      classPrefix: 'copyable-id-tooltip'
    };
    let tooltipProps = {
      target: idlabel,
      placement: this.props.placement || 'right',
      tether: tetherConfig,
      delay: 0
    };
    if (this.state.showTooltip) {
      tooltipProps.isOpen = true;
    }
    return (
      <UncontrolledTooltip
        {...tooltipProps}
      >
        <span>{this.renderToolTipText()}</span>
        {
          this.state.showTooltip ?
            <span className="copied-label text-success">
              <IconSVG name="icon-check-circle" />
              <span>{T.translate(`${PREFIX}.copiedLabel`)}</span>
            </span>
          :
            null
        }
      </UncontrolledTooltip>
    );
  }

  render() {
    let idlabel = `A-${uuidV4()}`;
    if (this.props.idprefix) {
      idlabel = `${this.props.idprefix}-${this.props.id}`;
    }
    // FIXME: Not sure how else to do this. Looks adhoc. Need this for copy to clipboard.
    new Clipboard(`#${idlabel}`);
    return (
      <span
        className="copyable-id btn-link"
        id={idlabel}
        onClick={this.onIDClickHandler.bind(this, this.props.id)}
        onMouseOut={() => {
          this.state.showTooltip ? this.onIDClickHandler() : null;
        }}
        data-clipboard-text={this.props.id}
      >
        <span>{this.props.label}</span>
        {this.renderTooltip(idlabel)}
      </span>
    );
  }
}
