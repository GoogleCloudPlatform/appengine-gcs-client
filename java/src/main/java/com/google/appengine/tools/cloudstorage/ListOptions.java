/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;


/**
 * GcsService listing options.
 */
public class ListOptions implements Serializable {

  /**
   * Default settings for including all objects in the result.
   */
  public static final ListOptions DEFAULT = new ListOptions.Builder().build();
  private static final long serialVersionUID = 235084157142037730L;

  private final String prefix;
  private final boolean recursive;

  /**
   * Listing options builder.
   */
  public static final class Builder {

    private String prefix = "";
    private boolean recursive = true;

    public ListOptions build() {
      return new ListOptions(this);
    }

    public Builder setPrefix(String prefix) {
      this.prefix = checkNotNull(prefix);
      return this;
    }

    public Builder setRecursive(boolean recursive) {
      this.recursive = recursive;
      return this;
    }
  }

  private ListOptions(Builder builder) {
    prefix = builder.prefix;
    recursive = builder.recursive;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isRecursive() {
    return recursive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, recursive);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ListOptions other = (ListOptions) obj;
    return prefix.equals(other.prefix) && recursive == other.recursive;
  }

  @Override
  public String toString() {
    return "ListOptions [prefix=" + prefix + ", recursive=" + recursive + "]";
  }
}
