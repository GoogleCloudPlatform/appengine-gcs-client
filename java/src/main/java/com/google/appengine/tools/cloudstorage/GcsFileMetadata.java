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
import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Contains metadata about a Google Cloud Storage Object.
 */
public final class GcsFileMetadata {

  private final GcsFilename filename;
  private final GcsFileOptions options;
  private final String etag;
  private final long length;
  private final Date lastModified;
  private final Map<String, String> xGoogHeaders;

  public GcsFileMetadata(
      GcsFilename filename, GcsFileOptions options, String etag, long length, Date lastModified) {
    this(filename, options, etag, length, lastModified, ImmutableMap.<String, String>of());
  }

  public GcsFileMetadata(GcsFilename filename, GcsFileOptions options, String etag, long length,
      Date lastModified, Map<String, String> xGoogHeaders) {
    Preconditions.checkArgument(length >= 0, "Length must be positive");
    this.filename = checkNotNull(filename, "Null filename");
    this.options = checkNotNull(options, "Null options");
    this.etag = etag;
    this.length = length;
    this.lastModified = lastModified;
    this.xGoogHeaders = ImmutableMap.copyOf(checkNotNull(xGoogHeaders));
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

  /**
   * Returns the Google headers that were provided together with the metadata.
   * Some of the headers such as 'x-goog-component-count' could be considered as
   * an extended metadata set.
   *
   * @see <a href="https://cloud.google.com/storage/docs/reference-headers#extension">
   *     Goog extension headers</a> for a complete list of headers.
   */
  public Map<String, String> getXGoogHeaders() {
    return xGoogHeaders;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + filename + ", " + length + ", " + etag + ", "
        + options + ", " + lastModified + ", " + xGoogHeaders + ")";
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
    return length == other.length
        && Objects.equals(filename, other.filename)
        && Objects.equals(etag, other.etag)
        && Objects.equals(options, other.options)
        && Objects.equals(xGoogHeaders, other.xGoogHeaders);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(filename, options, etag, length);
  }
}
