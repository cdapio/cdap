/*
 * Copyright © 2019 Cask Data, Inc.
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
import classnames from 'classnames';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import T from 'i18n-react';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MouseTrap from 'mousetrap';
import { setPopoverOffset } from 'components/DataPrep/helper';
import { preventPropagation } from 'services/helpers';

require('./Hash.scss');

const PREFIX = 'features.DataPrep.Directives.Hash';
const VALID_TYPES = ['string', 'byte[]'];

interface IHashProps {
  column: string;
  onComplete: () => void;
  isOpen: boolean;
  close: () => void;
}

export default class Hash extends Component<IHashProps> {
  public hashAlgorithmOptions = [
    'BLAKE2B-160',
    'BLAKE2B-256',
    'BLAKE2B-384',
    'BLAKE2B-512',
    'GOST3411',
    'GOST3411-2012-256',
    'GOST3411-2012-512',
    'KECCAK-224',
    'KECCAK-256',
    'KECCAK-288',
    'KECCAK-384',
    'KECCAK-512',
    'MD2',
    'MD4',
    'MD5',
    'RIPEMD128',
    'RIPEMD160',
    'RIPEMD256',
    'RIPEMD320',
    'SHA',
    'SHA-1',
    'SHA-224',
    'SHA-256',
    'SHA-384',
    'SHA-512',
    'SHA-512/224',
    'SHA-512/256',
    'SHA3-224',
    'SHA3-256',
    'SHA3-384',
    'SHA3-512',
    'Skein-1024-1024',
    'Skein-1024-384',
    'Skein-1024-512',
    'Skein-256-128',
    'Skein-256-160',
    'Skein-256-224',
    'Skein-256-256',
    'Skein-512-128',
    'Skein-512-160',
    'Skein-512-224',
    'Skein-512-256',
    'Skein-512-384',
    'Skein-512-512',
    'SM3',
    'Tiger',
    'WHIRLPOOL',
  ];

  public columnType = DataPrepStore.getState().dataprep.typesCheck[this.props.column];

  public state = {
    encode: true,
    selectedHashAlgorithm: this.hashAlgorithmOptions[0],
    isDisabled: VALID_TYPES.indexOf(this.columnType) === -1,
  };

  private calculateOffset = null;

  public componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('hash-directive'));
  }

  public componentDidUpdate() {
    if (this.props.isOpen && !this.state.isDisabled) {
      if (this.calculateOffset) {
        this.calculateOffset();
      }
      // This is not in componentDidMount as Mousetrap can bind to 'enter' only once.
      // So the last .bind will get the callback always when all the directives are rendered in ColumnActionDropdown
      // So we bind to enter key only when the directive is opened.
      MouseTrap.bind('enter', this.applyDirective);
    }
  }

  public componentWillUnmount() {
    // We can safely unbind here as we just need to unbind the last bind
    // The ugh.. part is we don't know what was last bound but we can unbind multiple
    // times and it shouldn't affect anything.
    MouseTrap.unbind('enter');
  }

  public handleHashAlgorithmSelect = (e) => {
    this.setState({ selectedHashAlgorithm: e.target.value });
  };

  public toggleEncode = () => {
    this.setState({ encode: !this.state.encode });
  };

  public handleKeyPress = (e) => {
    if (e.nativeEvent.keyCode !== 13 || this.state.selectedHashAlgorithm) {
      return;
    }

    this.applyDirective();
  };

  public handleRowFilter = (type) => {
    this.setState({ rowFilter: type });
  };

  public applyDirective = () => {
    let directive;
    const column = this.props.column;
    const hashAlgorithm = this.state.selectedHashAlgorithm;
    const encode = this.state.encode;
    directive = `hash ${column} ${hashAlgorithm} ${encode}`;

    this.execute([directive]);
  };

  public execute = (addDirective) => {
    execute(addDirective).subscribe(
      () => {
        this.props.close();
        this.props.onComplete();
      },
      (err) => {
        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message,
          },
        });
      }
    );
  };

  public renderEncode = () => {
    const encode = (
      <div>
        <span className="cursor-pointer" onClick={this.toggleEncode}>
          <span
            className={classnames('fa', {
              'fa-square-o': !this.state.encode,
              'fa-check-square': this.state.encode,
            })}
          />
          <span>{T.translate(`${PREFIX}.encode`)}</span>
        </span>
      </div>
    );

    return <div>{encode}</div>;
  };

  public renderAlgorithm = () => {
    const hashAlgorithms = this.hashAlgorithmOptions.map((algorithm) => {
      return {
        algorithm,
        displayText: T.translate(`${PREFIX}.${algorithm}`),
      };
    });

    return (
      <div>
        <div className="hash-algorithm">
          <div className="algorithm-select">
            <div>
              <select
                className="form-control mousetrap"
                value={this.state.selectedHashAlgorithm}
                onChange={this.handleHashAlgorithmSelect}
              >
                {hashAlgorithms.map((algorithm) => {
                  return (
                    <option value={algorithm.algorithm} key={algorithm.algorithm}>
                      {algorithm.displayText}
                    </option>
                  );
                })}
              </select>
            </div>
          </div>
        </div>
      </div>
    );
  };

  public renderDetail = () => {
    if (!this.props.isOpen || this.state.isDisabled) {
      MouseTrap.unbind('enter');
      return null;
    }

    return (
      <div className="hash-detail second-level-popover" onClick={preventPropagation}>
        {this.renderAlgorithm()}

        {this.renderEncode()}
        <hr />

        <div className="action-buttons">
          <button className="btn btn-primary float-left" onClick={this.applyDirective}>
            {T.translate('features.DataPrep.Directives.apply')}
          </button>

          <button className="btn btn-link float-right" onClick={this.props.close}>
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </div>
      </div>
    );
  };

  public render() {
    return (
      <div
        id="hash-directive"
        className={classnames('hash-directive clearfix action-item', {
          active: this.props.isOpen && !this.state.isDisabled,
          disabled: this.state.isDisabled,
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className="float-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}
