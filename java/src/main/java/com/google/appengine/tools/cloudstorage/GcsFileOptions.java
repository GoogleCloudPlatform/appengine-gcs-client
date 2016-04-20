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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Container class for holding options for creating Google Storage files.
 *
 * <p>
 * To construct {@code GcsFileOptions}, first create a {@link GcsFileOptions.Builder}. The builder
 * is mutable and each of the parameters can be set (any unset parameters will fallback to the
 * defaults). The {@code Builder} can be then used to create an immutable {@code GcsFileOptions}
 * object.
 * </p>
 *
 * <p>
 * For default {@code GcsFileOptions} use {@link #getDefaultInstance}. Default settings are subject
 * to change release to release. Currently the default values are to not specify any of the options.
 * If you require specific settings, explicitly create an instance of {@code GcsFileOptions} with
 * the required settings.
 * </p>
 *
 * @see <a href="https://cloud.google.com/storage/docs">Google Storage API</a>
 */
public final class GcsFileOptions implements Serializable {
  private static final long serialVersionUID = -7350111525144535653L;

  private final String mimeType;
  private final String acl;
  private final String cacheControl;
  private final String contentEncoding;
  private final String contentDisposition;
  private final ImmutableMap<String, String> userMetadata;

  private static final GcsFileOptions DEFAULT_INSTANCE = new Builder().build();

  private GcsFileOptions(Builder builder) {
    mimeType = builder.mimeType;
    acl = builder.acl;
    cacheControl = builder.cacheControl;
    contentEncoding = builder.contentEncoding;
    contentDisposition = builder.contentDisposition;
    userMetadata = builder.userMetadata.build();
  }

  /**
   * Retrieve an instance with the default parameters
   */
  public static GcsFileOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  /**
   * @return The mime type for the file. (May be null)
   */
  public String getMimeType() {
    return mimeType;
  }

  /**
   * @return The acl for the file. (May be null)
   */
  public String getAcl() {
    return acl;
  }

  /**
   * @return The cache control string for the file. (May be null)
   */
  public String getCacheControl() {
    return cacheControl;
  }

  /**
   * @return The content encoding of the file. (May be null)
   */
  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * @return The content disposition of the file. (May be null)
   */
  public String getContentDisposition() {
    return contentDisposition;
  }

  /**
   * @return Any user data associated with the file. (This map is unmodifiable)
   */
  public Map<String, String> getUserMetadata() {
    return userMetadata;
  }

  @Override
  public String toString() {
    return "GcsFileOptions [" + (mimeType != null ? "mimeType=" + mimeType + ", " : "")
        + (acl != null ? "acl=" + acl + ", " : "")
        + (cacheControl != null ? "cacheControl=" + cacheControl + ", " : "")
        + (contentEncoding != null ? "contentEncoding=" + contentEncoding + ", " : "")
        + (contentDisposition != null ? "contentDisposition=" + contentDisposition + ", " : "")
        + (userMetadata != null ? "userMetadata=" + userMetadata : "") + "]";
  }

  @Override
  public final boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GcsFileOptions other = (GcsFileOptions) o;
    return Objects.equals(mimeType, other.mimeType) && Objects.equals(acl, other.acl)
        && Objects.equals(cacheControl, other.cacheControl)
        && Objects.equals(contentEncoding, other.contentEncoding)
        && Objects.equals(contentDisposition, other.contentDisposition)
        && Objects.equals(userMetadata, other.userMetadata);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(mimeType,
        acl,
        cacheControl,
        contentEncoding,
        contentDisposition,
        userMetadata);
  }

  /**
   * A builder of GcsFileOptions.
   */
  public static final class Builder {

    private String mimeType;
    private String acl;
    private String cacheControl;
    private String contentEncoding;
    private String contentDisposition;
    private final ImmutableMap.Builder<String, String> userMetadata = ImmutableMap.builder();

    /**
     * Sets the mime type of the object. If not set, default Google Storage mime type is used when
     * served out of Google Storage.
     * @see <a href="https://cloud.google.com/storage/docs/reference-headers?csw=1#contenttype">
     *     GCS and the Content-Type Header</a>
     *
     * @param mimeType of the Google Storage object.
     * @return this builder for chaining.
     */
    public Builder mimeType(String mimeType) {
      this.mimeType = checkNotEmpty(mimeType, "MIME type");
      return this;
    }

    /**
     * Sets the acl of the object. If not set, defaults to none (i.e., bucket default).
     * @see <a href="https://cloud.google.com/storage/docs/accesscontrol">GCS Access Controls</a>
     *
     * @param acl to use for the Google Storage object.
     * @return this builder for chaining.
     */
    public Builder acl(String acl) {
      this.acl = checkNotEmpty(acl, "ACL");
      return this;
    }

    /**
     * Sets the cache control for the object. If not set, default value is used.
     * @see <a href="https://cloud.google.com/storage/docs/reference-headers?csw=1#cachecontrol">
     *     GCS and the Cache-Control Header</a>
     *
     * @param cacheControl to use for the Google Storage object.
     * @return this builder for chaining.
     */
    public Builder cacheControl(String cacheControl) {
      this.cacheControl = checkNotEmpty(cacheControl, "cache control");
      return this;
    }

    /**
     * Sets the content encoding for the object. If not set, default value is used.
     * @see <a href="https://cloud.google.com/storage/docs/reference-headers?csw=1#contentencoding">
     *     GCS and the Content-Encoding Header</a>
     *
     * @param contentEncoding to use for the Google Storage object.
     * @return this builder for chaining.
     */
    public Builder contentEncoding(String contentEncoding) {
      this.contentEncoding = checkNotEmpty(contentEncoding, "content encoding");
      return this;
    }

    /**
     * Sets the content disposition for the object. If not set, default value is used.
     * @see <a href="https://cloud.google.com/storage/docs/reference-headers?csw=1#contentdisposition">
     *     GCS and the Content-Disposition Header</a>
     *
     * @param contentDisposition to use for the Google Storage object.
     * @return this builder for chaining.
     */
    public Builder contentDisposition(String contentDisposition) {
      this.contentDisposition = checkNotEmpty(contentDisposition, "content disposition");
      return this;
    }

    /**
     * Adds user specific metadata that will be added to object headers when served through Google
     * Storage.
     *
     * <p>
     * Each entry will be prefixed with {@code x-goog-meta-} when serving out. For example, if you
     * add {@code foo->bar} entry to userMetadata map, it will be served out as a header: {@code
     * x-goog-meta-foo: bar}.
     * </p>
     *
     * @see <a href="https://cloud.google.com/storage/docs/reference-headers?csw=1#xgoogmeta">
     *     GCS and Custom User Metadata Headers</a>
     * @param key metadata/header name suffix
     * @param value metadata/header value
     * @return this builder for chaining.
     */
    public Builder addUserMetadata(String key, String value) {
      checkNotEmpty(key, "key");
      checkNotEmpty(value, "value");
      userMetadata.put(key, value);
      return this;
    }

    private static String checkNotEmpty(String value, String what) {
      Preconditions.checkNotNull(value, "Null %s", what);
      Preconditions.checkArgument(!value.isEmpty(), "Empty %s", what);
      return value;
    }

    /**
     * Create an instance of GcsFileOptions with the parameters set in this builder
     *
     * @return a new instance of GcsFileOptions
     */
    public GcsFileOptions build() {
      return new GcsFileOptions(this);
    }
  }
}
