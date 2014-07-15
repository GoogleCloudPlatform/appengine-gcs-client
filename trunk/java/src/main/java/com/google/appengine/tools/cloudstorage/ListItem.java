package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * Contains information of an individual listing item.
 * If listing was non-recursive, GCS objects that match the prefix,
 * {@link ListOptions.Builder#setPrefix(String)}, and has a separator after the prefix
 * would be considered a directory and will only have the directory name set (length would be zero).
 */
public final class ListItem implements Serializable {

  private static final long serialVersionUID = 57184722686054810L;

  private final String name;
  private final boolean isDirectory;
  private final long length;
  private final String etag;
  private final Date lastModified;

  /**
   * ListItem builder.
   */
  public static final class Builder {

    private String name;
    private boolean isDirectory;
    private long length;
    private String etag;
    private Date lastModified;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDirectory(boolean isDirectory) {
      this.isDirectory = isDirectory;
      return this;
    }

    public Builder setLength(long length) {
      this.length = length;
      return this;
    }

    public Builder setEtag(String etag) {
      this.etag = etag;
      return this;
    }

    public Builder setLastModified(Date lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    public ListItem build() {
      return new ListItem(this);
    }
  }

  private ListItem(Builder builder) {
    name = checkNotNull(builder.name);
    isDirectory = builder.isDirectory;
    length = builder.length;
    etag = builder.etag;
    lastModified = builder.lastModified;
  }

  public boolean isDirectory() {
    return isDirectory;
  }

  public String getName() {
    return name;
  }

  public long getLength() {
    return length;
  }

  public String getEtag() {
    return etag;
  }

  public Date getLastModified() {
    return lastModified;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
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
    ListItem other = (ListItem) obj;
    return name.equals(other.name) && isDirectory == other.isDirectory && length == other.length
        && Objects.equals(etag, other.etag) && Objects.equals(lastModified, other.lastModified);
  }

  @Override
  public String toString() {
    return "ListItem [name=" + name + ", isDirectory=" + isDirectory + ", length=" + length
        + ", etag=" + etag + ", lastModified=" + lastModified + "]";
  }
}