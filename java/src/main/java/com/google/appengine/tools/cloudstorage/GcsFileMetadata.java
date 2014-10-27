/*
 * Copyright 2012 Google Inc. All Rights Reserved.
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

import com.google.common.base.Preconditions;

import java.util.Date;
import java.util.Objects;

/**
 * Contains metadata about a Google Cloud Storage Object.
 */
public final class GcsFileMetadata {

  private final GcsFilename filename;
  private final GcsFileOptions options; private final String etag;
  private final long length; private final Date lastModified;

  public GcsFileMetadata(
      GcsFilename filename, GcsFileOptions options, String etag, long length, Date lastModified) {
    Preconditions.checkArgument(length >= 0, "Length must be positive");
    this.filename = checkNotNull(filename, "Null filename");
    this.options = checkNotNull(options, "Null options");
    this.etag = etag;
    this.length = length;
    this.lastModified = lastModified;
  }

  public GcsFilename getFilename() {
    return filename;
  }

  public GcsFileOptions getOptions() {
    return options;
  }

  public String getEtag() {
    return etag;
  }

  public long getLength() {
    return length;
  }

  public Date getLastModified() {
    return lastModified;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + filename + ", " + length + ", " + etag + ", "
        + options + ", " + String.valueOf(lastModified) + ")";
  }

  @Override
  public final boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GcsFileMetadata other = (GcsFileMetadata) o;
    return length == other.length && Objects.equals(filename, other.filename)
      && Objects.equals(etag, other.etag) && Objects.equals(options, other.options);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(filename, options, etag, length);
  }
}
