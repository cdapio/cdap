/*
 * Copyright © 2021 Cask Data, Inc.
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
import Popper, { PopperPlacementType } from '@material-ui/core/Popper';
import CommentBox from 'components/AbstractWidget/Comment/CommentBox';
import ClickAwayListener from '@material-ui/core/ClickAwayListener';
import { COMMENT_WIDTH, IComment } from 'components/AbstractWidget/Comment/CommentConstants';
import makeStyles from '@material-ui/core/styles/makeStyles';
import cloneDeep from 'lodash/cloneDeep';
import If from 'components/If';
import isNil from 'lodash/isNil';

const useStyles = makeStyles(() => {
  return {
    root: {
      position: 'relative',
      display: 'block',
    },
    popperRoot: {
      zIndex: 2000,
      width: COMMENT_WIDTH,
    },
  };
});
const DEFAULT_COMMENTS = [{ content: '', user: null, createDate: Date.now() }];

interface IDynamicAnchorProps {
  comments: IComment[];
  onChange: (commentsId: string, comments: IComment[]) => void;
  isOpen: boolean;
  placement?: PopperPlacementType;
  onClose: () => void;
  anchorEl: string | React.ReactNode;
  commentsId: string;
  disabled?: boolean;
}
function DynamicAnchor({
  comments,
  onChange,
  anchorEl,
  isOpen,
  placement = 'right-start',
  onClose,
  commentsId,
  disabled = false,
}: IDynamicAnchorProps) {
  if (!comments || (Array.isArray(comments) && !comments.length)) {
    comments = cloneDeep(DEFAULT_COMMENTS);
  }
  const classes = useStyles();
  const [localComments, setLocalComments] = React.useState(comments);
  const [localAnchorEl, setLocalAnchorEl] = React.useState(null);

  const isEmptyComment = (lc: IComment[]) => {
    return Array.isArray(lc) && lc.length === 1 && lc[0].content.length === 0;
  };

  const setAnchorElement = () => {
    if (typeof anchorEl === 'string') {
      const anchorElById = document.getElementById(anchorEl);
      setLocalAnchorEl(anchorElById);
    }
    if (typeof anchorEl === 'object') {
      setLocalAnchorEl(anchorEl);
    }
  };
  const onSave = (id, updatedComment) => {
    if (typeof onChange === 'function') {
      localComments[id] = updatedComment;
      setLocalComments(localComments);
      onChange(commentsId, localComments);
      if (isEmptyComment(localComments)) {
        onClickAway();
      }
    }
  };
  const onDelete = (id) => {
    if (typeof onChange === 'function') {
      if (id === 0) {
        setLocalComments(cloneDeep(DEFAULT_COMMENTS));
        onChange(commentsId, null);
        onClickAway();
      } else {
        const newComments = [...comments.slice(0, id), ...comments.slice(id + 1)];
        setLocalComments(newComments);
        onChange(commentsId, newComments);
      }
    }
  };
  const onClickAway = () => {
    if (!isOpen) {
      return;
    }
    if (typeof onClose === 'function') {
      onClose();
    }
    setLocalAnchorEl(null);
  };
  React.useEffect(() => {
    if (!isOpen) {
      return setLocalAnchorEl(null);
    }
    setAnchorElement();
  }, [isOpen, anchorEl]);

  React.useEffect(() => {
    const commentsFromProps = comments.reduce(
      (prev, curr) => `${prev}##${curr.content}`,
      comments[0].content
    );
    const commentsFromState = localComments.reduce(
      (prev, curr) => `${prev}##${curr.content}`,
      localComments[0].content
    );
    if (commentsFromProps !== commentsFromState) {
      setLocalComments(comments);
    }
  }, [comments]);

  return (
    <ClickAwayListener onClickAway={onClickAway} mouseEvent="onMouseDown">
      <div className={classes.root}>
        <If condition={!isNil(localAnchorEl)}>
          <Popper
            className={classes.popperRoot}
            anchorEl={localAnchorEl}
            open={isOpen}
            placement={placement}
            modifiers={{
              flip: {
                enabled: true,
              },
              preventOverflow: {
                enabled: true,
                boundariesElement: 'scrollParent',
              },
              arrow: {
                enabled: false,
              },
              offset: {
                enabled: true,
                offset: '95%, 10%',
              },
            }}
          >
            {localComments.map((comment, id) => {
              return (
                <CommentBox
                  key={id}
                  comment={comment}
                  onSave={onSave.bind(null, id)}
                  onDelete={onDelete.bind(null, id)}
                  focus={localComments.length - 1 === id}
                  disabled={disabled}
                />
              );
            })}
          </Popper>
        </If>
      </div>
    </ClickAwayListener>
  );
}

export default DynamicAnchor;
