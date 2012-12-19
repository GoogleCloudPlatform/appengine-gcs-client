package com.google.appengine.tools.cloudstorage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Container class for holding options for creating Google Storage files.
 * @see <a href="http://code.google.com/apis/storage/">Google Storage API</a>
 */
public final class GcsFileOptions implements Serializable {
  private static final long serialVersionUID = -7350111525144535653L;
  private String mimeType;private String acl;private String cacheControl;private String contentEncoding;private String contentDisposition;
  private final Map<String, String> userMetadata = new HashMap<String, String>();

  private GcsFileOptions(String mimeType,String acl,String cacheControl,String contentEncoding,String contentDisposition) {
    this.mimeType = mimeType;
    this.acl = acl;
    this.cacheControl = cacheControl;
    this.contentEncoding = contentEncoding;
    this.contentDisposition = contentDisposition;
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
    return Objects.equal(mimeType, other.mimeType) && Objects.equal(acl, other.acl)
        && Objects.equal(cacheControl, other.cacheControl)
        && Objects.equal(contentEncoding, other.contentEncoding)
        && Objects.equal(contentDisposition, other.contentDisposition)
        && Objects.equal(userMetadata, other.userMetadata);
  }

  @Override
  public final int hashCode() {
    return Objects.hashCode(mimeType,
        acl,
        cacheControl,
        contentEncoding,
        contentDisposition,
        userMetadata);
  }

  public static Builder builder() {
    return new Builder();
  }

  private static String checkNotEmpty(String value, String what) {
    Preconditions.checkNotNull(value, "Null %s", what);
    Preconditions.checkArgument(!value.isEmpty(), "Empty %s", what);
    return value;
  }


  /**
   * Sets the mime type of the object. If not set, default Google Storage mime type is used when
   * served out of Google Storage.
   * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contenttype"}
   *
   * @param mimeType of the Google Storage object.
   * @return this for chaining.
   */
  public GcsFileOptions mimeType(String mimeType) {
    this.mimeType = checkNotEmpty(mimeType, "MIME type");
    return this;
  }

  /**
   * Sets the acl of the object. If not set, defaults to none (i.e., bucket default).
   * {@link "http://code.google.com/apis/storage/docs/accesscontrol.html"}
   *
   * @param acl to use for the Google Storage object.
   * @return this for chaining.
   */
  public GcsFileOptions acl(String acl) {
    this.acl = checkNotEmpty(acl, "ACL");
    return this;
  }

  /**
   * Sets the cache control for the object. If not set, default value is used.
   * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#cachecontrol"}
   *
   * @param cacheControl to use for the Google Storage object.
   * @return this for chaining.
   */
  public GcsFileOptions cacheControl(String cacheControl) {
    this.cacheControl = checkNotEmpty(cacheControl, "cache control");
    return this;
  }

  /**
   * Sets the content encoding for the object. If not set, default value is used.
   * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contentencoding"}
   *
   * @param contentEncoding to use for the Google Storage object.
   * @return this for chaining.
   */
  public GcsFileOptions contentEncoding(String contentEncoding) {
    this.contentEncoding = checkNotEmpty(contentEncoding, "content encoding");
    return this;
  }

  /**
   * Sets the content disposition for the object. If not set, default value is used.
   * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contentdisposition"}
   *
   * @param contentDisposition to use for the Google Storage object.
   * @return this for chaining.
   */
  public GcsFileOptions contentDisposition(String contentDisposition) {
    this.contentDisposition = checkNotEmpty(contentDisposition, "content disposition");
    return this;
  }

  /**
   * Adds user specific metadata that will be added to object headers when served through Google
   * Storage: {@link "http://code.google.com/apis/storage/docs/reference-headers.html#xgoogmeta"}
   * Each entry will be prefixed with x-goog-meta- when serving out. For example, if you add
   * 'foo'->'bar' entry to userMetadata map, it will be served out as a header: x-goog-meta-foo: bar
   *
   * @param key
   * @param value
   * @return this for chaining.
   */
  public GcsFileOptions addUserMetadata(String key, String value) {
    checkNotEmpty(key, "key");
    checkNotEmpty(value, "value");
    userMetadata.put(key, value);
    return this;
  }


  /**
   * A builder of GcsFileOptions.
   */
  public static final class Builder {

    private Builder() {}

    /**
     * Sets the mime type of the object. If not set, default Google Storage mime type is used when
     * served out of Google Storage.
     * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contenttype"}
     *
     * @param mimeType of the Google Storage object.
     * @return this for chaining.
     */
    public GcsFileOptions withMimeType(String mimeType) {
      return withDefaults().mimeType(mimeType);
    }

    /**
     * Sets the acl of the object. If not set, defaults to none (ie, bucket default).
     * {@link "http://code.google.com/apis/storage/docs/accesscontrol.html"}
     *
     * @param acl to use for the Google Storage object.
     * @return this for chaining.
     */
    public GcsFileOptions withAcl(String acl) {
      return withDefaults().acl(acl);
    }

    /**
     * Sets the cache control for the object. If not set, default value is used.
     * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#cachecontrol"}
     *
     * @param cacheControl to use for the Google Storage object.
     * @return this for chaining.
     */
    public GcsFileOptions withCacheControl(String cacheControl) {
      return withDefaults().cacheControl(cacheControl);
    }

    /**
     * Sets the content encoding for the object. If not set, default value is used.
     * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contentencoding"}
     *
     * @param contentEncoding to use for the Google Storage object.
     * @return this for chaining.
     */
    public GcsFileOptions withContentEncoding(String contentEncoding) {
      return withDefaults().contentEncoding(contentEncoding);
    }

    /**
     * Sets the content disposition for the object. If not set, default value is used.
     * {@link "http://code.google.com/apis/storage/docs/reference-headers.html#contentdisposition"}
     *
     * @param contentDisposition to use for the Google Storage object.
     * @return this for chaining.
     */
    public GcsFileOptions withContentDisposition(String contentDisposition) {
      return withDefaults().contentDisposition(contentDisposition);
    }

    /**
     * Adds user specific metadata that will be added to object headers when served through Google
     * Storage: {@link "http://code.google.com/apis/storage/docs/reference-headers.html#xgoogmeta"}
     * Each entry will be prefixed with x-goog-meta- when serving out. For example, if you add
     * 'foo'->'bar' entry to userMetadata map, it will be served out as a header: x-goog-meta-foo:
     * bar
     *
     * @param key
     * @param value
     * @return this for chaining.
     */
    public GcsFileOptions addUserMetadata(String key, String value) {
      return withDefaults().addUserMetadata(key, value);
    }

    public GcsFileOptions withDefaults() {
      return new GcsFileOptions(null, null, null, null, null);
    }
  }

}
