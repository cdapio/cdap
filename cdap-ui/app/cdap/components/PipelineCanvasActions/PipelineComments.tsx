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
import Popper from '@material-ui/core/Popper';
import CommentBox from 'components/AbstractWidget/Comment/CommentBox';
import { COMMENT_WIDTH } from 'components/AbstractWidget/Comment/CommentConstants';
import makeStyles from '@material-ui/core/styles/makeStyles';
import If from 'components/If';
import isNil from 'lodash/isNil';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';
import PostAddIcon from '@material-ui/icons/PostAdd';
import { preventPropagation } from 'services/helpers';
import { IComment } from 'components/AbstractWidget/Comment/CommentConstants';
import cloneDeep from 'lodash/cloneDeep';
import { UniversalBackdrop } from 'components/UniversalBackdrop';
import { IconButton } from '@material-ui/core';
import CloseIcon from '@material-ui/icons/Close';
const useStyles = makeStyles((theme) => {
  return {
    root: {
      position: 'relative',
      display: 'block',
    },
    popper: {
      zIndex: 2000,
      width: COMMENT_WIDTH,
      '&[x-placement*="left"] $arrow': {
        right: 0,
        marginRight: '-0.9em',
        height: '3em',
        width: '1em',
        '&::before': {
          borderWidth: '1em 0 1em 1em',
          borderColor: `transparent transparent transparent ${theme.palette.grey[600]}`,
        },
      },
    },
    pipelineCommentsWrapper: {
      padding: '10px',
      gap: '10px',
      maxHeight: '500px',
      background: theme.palette.grey[600],
      overflowY: 'auto',
      '& > *': {
        marginBottom: '10px',
      },
    },
    arrow: {
      position: 'absolute',
      fontSize: 7,
      width: '3em',
      height: '3em',
      '&::before': {
        content: '""',
        margin: 'auto',
        display: 'block',
        width: 0,
        height: 0,
        borderStyle: 'solid',
      },
    },
    popperHeading: {
      display: 'flex',
      justifyContent: 'flex-end',
    },
  };
});

interface IPipelineCommentsProps {
  comments: IComment[];
  anchorEl: React.ReactNode;
  onChange: (comments: IComment[]) => void;
  disabled?: boolean;
  onClose: () => void;
}

const DEFAULT_COMMENTS = [{ content: '', createDate: Date.now() }];
const getDefaultComments = (comments) => {
  if (!Array.isArray(comments) || (Array.isArray(comments) && !comments.length)) {
    return cloneDeep(DEFAULT_COMMENTS);
  }
  return comments;
};
export function PipelineComments({
  comments,
  anchorEl,
  onChange,
  disabled = false,
  onClose,
}: IPipelineCommentsProps) {
  const [localComments, setLocalComments] = React.useState(getDefaultComments(comments));
  const [localAnchorEl, setLocalAnchorEl] = React.useState(null);
  const [isOpen, setIsOpen] = React.useState(false);
  const classes = useStyles();
  const [arrowRef, setArrowRef] = React.useState(null);

  const filterEmptyComments = (lc: IComment[]) => {
    return lc.filter((comment: IComment) => comment.content.length);
  };

  const onSave = (id, updatedComment) => {
    if (typeof onChange === 'function') {
      localComments[id] = updatedComment;
      setLocalComments(filterEmptyComments(localComments));
      onChange(localComments);
    }
  };
  const onDelete = (id) => {
    if (typeof onChange === 'function') {
      const newComments = [...comments.slice(0, id), ...comments.slice(id + 1)];
      setLocalComments(getDefaultComments(newComments));
      onChange(newComments);
    }
  };

  const onAddNewComment = () => {
    setLocalComments([{ content: '', createDate: Date.now() }, ...localComments]);
  };

  React.useEffect(() => {
    if (anchorEl !== null && typeof anchorEl === 'object') {
      setIsOpen(true);
      setLocalAnchorEl(anchorEl);
    } else {
      setIsOpen(false);
      setLocalAnchorEl(null);
    }
  }, [anchorEl]);
  return (
    <div className={classes.root} onClick={preventPropagation}>
      <If condition={!isNil(localAnchorEl)}>
        <Popper
          className={classes.popper}
          anchorEl={localAnchorEl}
          open={isOpen}
          placement="left"
          modifiers={{
            flip: {
              enabled: true,
            },
            preventOverflow: {
              enabled: true,
              boundariesElement: 'scrollParent',
            },
            arrow: {
              enabled: true,
              element: arrowRef,
            },
            offset: {
              enabled: true,
              offset: '200%, 10%',
            },
          }}
        >
          <UniversalBackdrop open={isOpen} onClose={onClose} invisible />
          <div className={classes.arrow} ref={setArrowRef} />
          <Paper className={classes.pipelineCommentsWrapper}>
            <If condition={!disabled}>
              <div className={classes.popperHeading}>
                <Button
                  onClick={onAddNewComment}
                  disabled={localComments.length && !localComments[0].content.length}
                >
                  <PostAddIcon />
                  <span>Add Comment</span>
                </Button>
                <IconButton onClick={onClose}>
                  <CloseIcon />
                </IconButton>
              </div>
            </If>
            {localComments.map((comment, id) => {
              return (
                <CommentBox
                  key={id}
                  comment={comment}
                  onSave={onSave.bind(null, id)}
                  onDelete={onDelete.bind(null, id)}
                  focus={!comment.content.length}
                  disabled={disabled}
                  onClose={onClose}
                />
              );
            })}
          </Paper>
        </Popper>
      </If>
    </div>
  );
}
