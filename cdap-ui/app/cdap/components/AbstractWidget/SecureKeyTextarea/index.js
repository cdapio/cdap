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
import { WIDGET_PROPTYPES, DEFAULT_WIDGET_PROPS } from 'components/AbstractWidget';
import { MySecureKeyApi } from 'api/securekey';
import { getCurrentNamespace } from 'services/NamespaceStore';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';
import { SECURE_KEY_PREFIX, SECURE_KEY_SUFFIX, SYSTEM_NAMESPACE } from 'services/global-constants';
require('./SecureKeyTextarea.scss');

const PREFIX = 'features.AbstractWidget.SecureKeyTextarea';

export default class SecureKeyTextarea extends Component {
  static propTypes = WIDGET_PROPTYPES;
  static defaultProps = DEFAULT_WIDGET_PROPS;

  state = {
    secureKeys: [],
    expanded: false,
    customEntry: false,
    customEntryText: '',
  };

  componentWillMount() {
    const namespace = objectQuery(this.props, 'extraConfig', 'namespace') || getCurrentNamespace();

    if (namespace === SYSTEM_NAMESPACE) {
      return;
    }

    const params = {
      namespace,
    };

    MySecureKeyApi.list(params).subscribe((res) => {
      const keys = Object.keys(res);

      this.setState({
        secureKeys: keys,
      });
    });
  }

  toggleExpand = () => {
    this.setState({
      expanded: !this.state.expanded,
      customEntry: false,
      customEntryText: '',
    });
  };

  toggleCustomEntry = () => {
    this.setState({
      customEntry: !this.state.customEntry,
      customEntryText: '',
    });
  };

  handleKeyPress = (e) => {
    const keyText = this.state.customEntryText;

    if (e.nativeEvent.keyCode !== 13 || keyText.length === 0) {
      return;
    }

    this.onSecureKeySelect(keyText);
  };

  onSecureKeySelect = (key) => {
    const secureKey = `${SECURE_KEY_PREFIX}${key}${SECURE_KEY_SUFFIX}`;

    this.props.onChange(secureKey);
    this.toggleExpand();
  };

  handleCustomEntryChange = (e) => {
    this.setState({
      customEntryText: e.target.value,
    });
  };

  renderCustomEntry = () => {
    const helperText = (
      <div className="helper-text text-center" onClick={this.toggleCustomEntry}>
        {T.translate(`${PREFIX}.customEntryHelper`)}
      </div>
    );

    const textbox = (
      <input
        type="text"
        className="form-control"
        value={this.state.customEntryText}
        onChange={this.handleCustomEntryChange}
        onKeyPress={this.handleKeyPress}
        placeholder={T.translate(`${PREFIX}.customEntryPlaceholder`)}
      />
    );

    return (
      <div className={classnames('custom-entry-row', { expanded: this.state.customEntry })}>
        {this.state.customEntry ? textbox : helperText}
      </div>
    );
  };

  renderSecureKeyContent = () => {
    if (!this.state.expanded) {
      return null;
    }

    return (
      <div className="secure-key-content">
        <div className="keys-list">
          {this.state.secureKeys.map((key) => {
            return (
              <div
                key={key}
                className="secure-key-row"
                onClick={this.onSecureKeySelect.bind(this, key)}
              >
                {key}
              </div>
            );
          })}
        </div>

        {this.renderCustomEntry()}
      </div>
    );
  };

  renderSecureKey = () => {
    const text = <span className="title">{T.translate(`${PREFIX}.title`)}</span>;

    return (
      <div className={classnames('secure-key', { expanded: this.state.expanded })}>
        <div className="top-part">
          <IconSVG name="icon-shield" onClick={this.toggleExpand} />

          {this.state.expanded ? text : null}
        </div>

        {this.renderSecureKeyContent()}
      </div>
    );
  };

  render() {
    return (
      <div className="secure-key-textarea-widget">
        <textarea
          className="form-control raw-text-input"
          onChange={this.props.onChange}
          value={this.props.value}
          {...this.props.widgetProps}
        />

        {this.renderSecureKey()}
      </div>
    );
  }
}
