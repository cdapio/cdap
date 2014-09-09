/*
 * Copyright 2012-2014 Cask, Inc.
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

package co.cask.cdap.shell.util;

import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 * @param <T>
 */
public class TreeNode<T> {

  private final T data;
  private final List<TreeNode<T>> children;
  private final TreeNode<T> parent;

  public TreeNode(T data, TreeNode<T> parent) {
    this.data = data;
    this.parent = parent;
    this.children = Lists.newArrayList();
  }

  public TreeNode() {
    this(null, null);
  }

  public TreeNode<T> findChild(T child) {
    for (TreeNode<T> candidate : children) {
      if (child.equals(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Finds a child node, and if it doesn't exist yet, create it before returning.
   * @param child
   * @return
   */
  public TreeNode<T> findOrCreateChild(T child) {
    for (TreeNode<T> candidate : children) {
      if (child.equals(candidate)) {
        return candidate;
      }
    }
    return addChild(child);
  }

  public TreeNode<T> addChild(T data) {
    TreeNode<T> result = new TreeNode<T>(data, this);
    if (children.add(result)) {
      return result;
    }
    return null;
  }

  public T getData() {
    return data;
  }

  public TreeNode<T> getParent() {
    return parent;
  }

  public List<TreeNode<T>> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return "TreeNode{" + data + '}';
  }
}
