import React, { Component } from 'react';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import classnames from 'classnames';

const PREFIX = 'features.DataPrep.Directives.MapToTarget';

export default class MapToTarget extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    isDisabled: PropTypes.bool,
    column: PropTypes.string,
    onComplete: PropTypes.func,
  };

  render() {
    const id = 'map-to-target-directive';

    return (
      <div
        id={id}
        className={classnames('clearfix action-item', {
          active: this.props.isOpen && !this.props.isDisabled,
          disabled: this.props.isDisabled,
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className='float-right'>
          <span className='fa fa-caret-right' />
        </span>
      </div>
    );
  }

}
