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
import React from 'react';
import IconSVG from 'components/IconSVG';
import Popper from '@material-ui/core/Popper';
import ClickAwayListener from '@material-ui/core/ClickAwayListener';
import Paper from '@material-ui/core/Paper';
import If from 'components/If';
import classnames from 'classnames';
import Grow from '@material-ui/core/Grow';

require('./PipelineModeless.scss');

export default function PipelineModeless({
  title,
  secondaryTitle,
  onClose,
  open,
  anchorEl,
  suppressAnimation,
  children,
  className,
  fullScreen,
  arrow,
  placement = 'bottom',
  popoverClassName = '',
}) {
  let anchorElCb;
  if (typeof anchorEl === 'string') {
    anchorElCb = () => document.getElementById(anchorEl);
  }
  const transitionDuration = suppressAnimation ? 0 : 'auto';

  const [arrowRef, setArrowRef] = React.useState(null);

  return (
    <Popper
      id="pipeline-modeless"
      className={classnames(
        {
          'pipeline-modeless': true,
          'full-screen': fullScreen,
          arrow,
        },
        popoverClassName
      )}
      open={open}
      anchorEl={anchorElCb || anchorEl}
      placement={placement}
      modifiers={{
        preventOverflow: {
          enabled: true,
          padding: 0, // Go flush with screen edge in fullScreen
        },
        flip: {
          enabled: true,
        },
        keepTogether: {
          enabled: true,
        },
        arrow: {
          enabled: !!arrow,
          element: arrowRef,
        },
      }}
      transition
    >
      {({ TransitionProps }) => (
        <Grow
          {...TransitionProps}
          style={{ transformOrigin: 'center top' }}
          timeout={transitionDuration}
        >
          <div className={className}>
            {arrow ? <div className="pipeline-modeless-arrow" ref={setArrowRef} /> : null}
            <ClickAwayListener onClickAway={onClose}>
              <Paper className="pipeline-modeless-container" variant="outlined">
                <div className="pipeline-modeless-header">
                  <div className="pipeline-modeless-title">{title}</div>
                  <If condition={!!secondaryTitle}>
                    <div className="secondary-title text-right">{secondaryTitle}</div>
                  </If>
                  <div className="btn-group">
                    <a className="btn" data-cy="pipeline-modeless-close-btn" onClick={onClose}>
                      <IconSVG name="icon-close" />
                    </a>
                  </div>
                </div>
                <div className="pipeline-modeless-content">{children}</div>
              </Paper>
            </ClickAwayListener>
          </div>
        </Grow>
      )}
    </Popper>
  );
}

PipelineModeless.propTypes = {
  title: PropTypes.node,
  secondaryTitle: PropTypes.node,
  onClose: PropTypes.func,
  open: PropTypes.bool,
  anchorEl: PropTypes.oneOf([PropTypes.element, PropTypes.string]),
  arrow: PropTypes.bool,
  suppressAnimation: PropTypes.bool,
  className: PropTypes.string,
  fullScreen: PropTypes.bool,
  children: PropTypes.node,
  placement: PropTypes.string,
  popoverClassName: PropTypes.string,
};
