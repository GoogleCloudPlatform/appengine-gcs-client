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

package com.google.appengine.tools.cloudstorage.oauth;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.urlfetch.FetchOptions;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.utils.FutureWrapper;
import com.google.appengine.tools.cloudstorage.BadRangeException;
import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper around the Google Cloud Storage REST API.  The subset of features
 * exposed here is intended to be appropriate for implementing
 * {@link RawGcsService}.
 */
final class OauthRawGcsService implements RawGcsService {

  private static final String ACL = "x-goog-acl";
  private static final String CACHE_CONTROL = "Cache-Control";
  private static final String CONTENT_ENCODING = "Content-Encoding";
  private static final String CONTENT_DISPOSITION = "Content-Disposition";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String CONTENT_RANGE = "Content-Range";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String ETAG = "ETag";
  private static final String LAST_MODIFIED = "Last-Modified";
  private static final String LOCATION = "Location";
  private static final String RANGE = "Range";
  private static final String UPLOAD_ID = "upload_id";
  private static final String X_GOOG_META = "x-goog-meta-";
  private static final String STORAGE_API_HOSTNAME = "storage.googleapis.com";
  private static final HTTPHeader RESUMABLE_HEADER = new HTTPHeader("x-goog-resumable", "start");
  private static final HTTPHeader USER_AGENT =
      new HTTPHeader("User-Agent", "App Engine GCS Client");

  private static final Logger log = Logger.getLogger(OauthRawGcsService.class.getName());

  public static final List<String> OAUTH_SCOPES =
      ImmutableList.of("https://www.googleapis.com/auth/devstorage.read_write");

  private static final int READ_LIMIT_BYTES = 31 * 1024 * 1024;
  public static final int WRITE_LIMIT_BYTES = 10_000_000;
  private static final int CHUNK_ALIGNMENT_BYTES = 256 * 1024;

  /**
   * Token used during file creation.
   */
  public static class GcsRestCreationToken implements RawGcsCreationToken {
    private static final long serialVersionUID = 975106845036199413L;

    private final GcsFilename filename;
    private final String uploadId;
    private final long offset;

    GcsRestCreationToken(GcsFilename filename, String uploadId, long offset) {
      this.filename = checkNotNull(filename, "Null filename");
      this.uploadId = checkNotNull(uploadId, "Null uploadId");
      this.offset = offset;
    }

    @Override
    public GcsFilename getFilename() {
      return filename;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + filename + ", " + uploadId + ")";
    }

    @Override
    public long getOffset() {
      return offset;
    }
  }

  private final OAuthURLFetchService urlfetch;
  private volatile ImmutableSet<HTTPHeader> headers = ImmutableSet.of();

  OauthRawGcsService(OAuthURLFetchService urlfetch) {
    this.urlfetch = checkNotNull(urlfetch, "Null urlfetch");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + urlfetch + ")";
  }

  @VisibleForTesting
  static URL makeUrl(GcsFilename filename, String uploadId) {
    String path = new StringBuilder()
        .append('/').append(filename.getBucketName())
        .append('/').append(filename.getObjectName())
        .toString();
    String query = null;
    if (uploadId != null) {
      query = new StringBuilder().append(UPLOAD_ID).append('=').append(uploadId).toString();
    }
    try {
      return new URI("https", null, STORAGE_API_HOSTNAME, -1, path, query, null).toURL();
    } catch (MalformedURLException | URISyntaxException e) {
      throw new RuntimeException(
          "Could not create a URL for " + filename + " with uploadId " + uploadId, e);
    }
  }

  private static HTTPRequest makeRequest(GcsFilename filename, String uploadId,
      HTTPMethod method, long timeoutMillis, Set<HTTPHeader> headers) {
    HTTPRequest request = new HTTPRequest(makeUrl(filename, uploadId), method,
        FetchOptions.Builder.disallowTruncate()
            .doNotFollowRedirects()
            .validateCertificate()
            .setDeadline(timeoutMillis / 1000.0));
    for (HTTPHeader header : headers) {
      request.addHeader(header);
    }
    request.addHeader(USER_AGENT);
    return request;
  }

  private static Error handleError(HTTPRequest req, HTTPResponse resp) throws IOException {
    int responseCode = resp.getResponseCode();
    switch (responseCode) {
      case 400:
        throw new RuntimeException("Server replied with 400, probably bad request: "
            + URLFetchUtils.describeRequestAndResponse(req, resp, true));
      case 401:
        throw new RuntimeException("Server replied with 401, probably bad authentication: "
            + URLFetchUtils.describeRequestAndResponse(req, resp, true));
      case 403:
        throw new RuntimeException(
            "Server replied with 403, check that ACLs are set correctly on the object and bucket: "
            + URLFetchUtils.describeRequestAndResponse(req, resp, true));
      case 404:
        throw new RuntimeException("Server replied with 404, probably no such bucket: "
            + URLFetchUtils.describeRequestAndResponse(req, resp, true));
      case 412:
        throw new RuntimeException("Server replied with 412, precondition failure: "
            + URLFetchUtils.describeRequestAndResponse(req, resp, true));
      default:
        if (responseCode >= 500 && responseCode < 600) {
          throw new IOException("Response code " + resp.getResponseCode() + ", retryable: "
              + URLFetchUtils.describeRequestAndResponse(req, resp, true));
        } else {
          throw new RuntimeException("Unexpected response code " + resp.getResponseCode() + ": "
              + URLFetchUtils.describeRequestAndResponse(req, resp, true));
        }
    }
  }

  @Override
  public int getMaxWriteSizeByte() {
    return WRITE_LIMIT_BYTES;
  }

  @Override
  public RawGcsCreationToken beginObjectCreation(
      GcsFilename filename, GcsFileOptions options, long timeoutMillis) throws IOException {
    HTTPRequest req = makeRequest(filename, null, HTTPMethod.POST, timeoutMillis, headers);
    req.setHeader(RESUMABLE_HEADER);
    addOptionsHeaders(req, options);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(req, e);
    }
    if (resp.getResponseCode() == 201) {
      String location = URLFetchUtils.getSingleHeader(resp, LOCATION);
      String queryString = new URL(location).getQuery();
      Preconditions.checkState(
          queryString != null, LOCATION + " header," + location + ", witout a query string");
      Map<String, String> params = Splitter.on('&').withKeyValueSeparator('=').split(queryString);
      Preconditions.checkState(params.containsKey(UPLOAD_ID),
          LOCATION + " header," + location + ", has a query string without " + UPLOAD_ID);
      return new GcsRestCreationToken(filename, params.get(UPLOAD_ID), 0);
    } else {
      throw handleError(req, resp);
    }
  }

  private static IOException createIOException(HTTPRequest req, Throwable ex) {
    StringBuilder stBuilder = new StringBuilder("URLFetch threw IOException; request: ");
    URLFetchUtils.appendRequest(req, stBuilder);
    return new IOException(stBuilder.toString(), ex);
  }

  private void addOptionsHeaders(HTTPRequest req, GcsFileOptions options) {
    if (options == null) {
      return;
    }
    if (options.getMimeType() != null) {
      req.setHeader(new HTTPHeader(CONTENT_TYPE, options.getMimeType()));
    }
    if (options.getAcl() != null) {
      req.setHeader(new HTTPHeader(ACL, options.getAcl()));
    }
    if (options.getCacheControl() != null) {
      req.setHeader(new HTTPHeader(CACHE_CONTROL, options.getCacheControl()));
    }
    if (options.getContentDisposition() != null) {
      req.setHeader(new HTTPHeader(CONTENT_DISPOSITION, options.getContentDisposition()));
    }
    if (options.getContentEncoding() != null) {
      req.setHeader(new HTTPHeader(CONTENT_ENCODING, options.getContentEncoding()));
    }
    for (Entry<String, String> entry : options.getUserMetadata().entrySet()) {
      req.setHeader(new HTTPHeader(X_GOOG_META + entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public Future<RawGcsCreationToken> continueObjectCreationAsync(
      RawGcsCreationToken token, ByteBuffer chunk, long timeoutMillis) {
    return putAsync((GcsRestCreationToken) token, chunk, false, timeoutMillis);
  }

  @Override
  public void finishObjectCreation(RawGcsCreationToken token, ByteBuffer chunk, long timeoutMillis)
      throws IOException {
    put((GcsRestCreationToken) token, chunk, true, timeoutMillis);
  }

  @Override
  public void putObject(GcsFilename filename, GcsFileOptions options, ByteBuffer content,
      long timeoutMillis) throws IOException {
    HTTPRequest req = makeRequest(filename, null, HTTPMethod.PUT, timeoutMillis, headers);
    req.setHeader(new HTTPHeader(CONTENT_LENGTH, String.valueOf(content.remaining())));
    req.setPayload(peekBytes(content));
    addOptionsHeaders(req, options);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(req, e);
    }
    if (resp.getResponseCode() != 200) {
      throw handleError(req, resp);
    }
  }

  HTTPRequest createPutRequest(final GcsRestCreationToken token, final ByteBuffer chunk,
      final boolean isFinalChunk, long timeoutMillis, final int length) {
    long offset = token.offset;
    Preconditions.checkArgument(offset % CHUNK_ALIGNMENT_BYTES == 0,
        "%s: Offset not aligned; offset=%s, length=%s, token=%s",
        this, offset, length, token);
    Preconditions.checkArgument(isFinalChunk || length % CHUNK_ALIGNMENT_BYTES == 0,
        "%s: Chunk not final and not aligned: offset=%s, length=%s, token=%s",
        this, offset, length, token);
    Preconditions.checkArgument(isFinalChunk || length > 0,
        "%s: Chunk empty and not final: offset=%s, length=%s, token=%s",
        this, offset, length, token);
    if (log.isLoggable(Level.FINEST)) {
      log.finest(this + ": About to write to " + token + " " + String.format("0x%x", length)
          + " bytes at offset " + String.format("0x%x", offset)
          + "; isFinalChunk: " + isFinalChunk + ")");
    }
    long limit = offset + length;
    final HTTPRequest req =
        makeRequest(token.filename, token.uploadId, HTTPMethod.PUT, timeoutMillis, headers);
    req.setHeader(
        new HTTPHeader(CONTENT_RANGE,
            "bytes " + (length == 0 ? "*" : offset + "-" + (limit - 1))
            + (isFinalChunk ? "/" + limit : "/*")));
    req.setPayload(peekBytes(chunk));
    return req;
  }

  /**
   * Given a HTTPResponce, process it, throwing an error if needed and return a Token for the next
   * request.
   */
  GcsRestCreationToken handlePutResponse(final GcsRestCreationToken token,
      final ByteBuffer chunk,
      final boolean isFinalChunk,
      final int length,
      final HTTPRequest req,
      HTTPResponse resp) throws Error, IOException {
    switch (resp.getResponseCode()) {
      case 200:
        if (!isFinalChunk) {
          throw new RuntimeException("Unexpected response code 200 on non-final chunk: "
              + URLFetchUtils.describeRequestAndResponse(req, resp, true));
        } else {
          chunk.position(chunk.limit());
          return null;
        }
      case 308:
        if (isFinalChunk) {
          throw new RuntimeException("Unexpected response code 308 on final chunk: "
              + URLFetchUtils.describeRequestAndResponse(req, resp, true));
        } else {
          chunk.position(chunk.limit());
          return new GcsRestCreationToken(token.filename, token.uploadId, token.offset + length);
        }
      default:
        throw handleError(req, resp);
    }
  }

  /**
   * Write the provided chunk at the offset specified in the token. If finalChunk is set, the file
   * will be closed.
   */
  private RawGcsCreationToken put(final GcsRestCreationToken token, final ByteBuffer chunk,
      final boolean isFinalChunk, long timeoutMillis) throws IOException {
    final int length = chunk.remaining();
    final HTTPRequest req = createPutRequest(token, chunk, isFinalChunk, timeoutMillis, length);
    HTTPResponse response;
    try {
      response = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(req, e);
    }
    return handlePutResponse(token, chunk, isFinalChunk, length, req, response);
  }

  /**
   * Same as {@link #put} but is runs asynchronously and returns a future. In the event of an error
   * the exception out of the future will be an ExecutionException with the cause set to the same
   * exception that would have been thrown by put.
   */
  private Future<RawGcsCreationToken> putAsync(final GcsRestCreationToken token,
      final ByteBuffer chunk, final boolean isFinalChunk, long timeoutMillis) {
    final int length = chunk.remaining();
    final HTTPRequest req = createPutRequest(token, chunk, isFinalChunk, timeoutMillis, length);
    return new FutureWrapper<HTTPResponse, RawGcsCreationToken>(urlfetch.fetchAsync(req)) {
      @Override
      protected Throwable convertException(Throwable e) {
        return OauthRawGcsService.convertException(e, req);
      }

      @Override
      protected GcsRestCreationToken wrap(HTTPResponse resp) throws Exception {
        return handlePutResponse(token, chunk, isFinalChunk, length, req, resp);
      }
    };
  }

  private static byte[] peekBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
        && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      int pos = in.position();
      byte[] buf = new byte[in.remaining()];
      in.get(buf);
      in.position(pos);
      return buf;
    }
  }

  /** True if deleted, false if not found. */
  @Override
  public boolean deleteObject(GcsFilename filename, long timeoutMillis) throws IOException {
    HTTPRequest req = makeRequest(filename, null, HTTPMethod.DELETE, timeoutMillis, headers);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(req, e);
    }
    switch (resp.getResponseCode()) {
      case 204:
        return true;
      case 404:
        return false;
      default:
        throw handleError(req, resp);
    }
  }

  private long getLengthFromContentRange(HTTPResponse resp) {
    String range = URLFetchUtils.getSingleHeader(resp, CONTENT_RANGE);
    Preconditions.checkState(range.matches("bytes [0-9]+-[0-9]+/[0-9]+"),
        "%s: unexpected " + CONTENT_RANGE + ": %s", this, range);
    return Long.parseLong(range.substring(range.indexOf("/") + 1));
  }

  private long getLengthFromContentLength(HTTPResponse resp) {
    return Long.parseLong(URLFetchUtils.getSingleHeader(resp, CONTENT_LENGTH));
  }

  /**
   * Might not fill all of dst.
   */
  @Override
  public Future<GcsFileMetadata> readObjectAsync(
      final ByteBuffer dst, final GcsFilename filename, long startOffsetBytes, long timeoutMillis) {
    Preconditions.checkArgument(startOffsetBytes >= 0, "%s: offset must be non-negative: %s",
        this, startOffsetBytes);
    final int n = dst.remaining();
    Preconditions.checkArgument(n > 0, "%s: dst full: %s", this, dst);
    final int want = Math.min(READ_LIMIT_BYTES, n);

    final HTTPRequest req = makeRequest(filename, null, HTTPMethod.GET, timeoutMillis, headers);
    req.setHeader(
        new HTTPHeader(RANGE, "bytes=" + startOffsetBytes + "-" + (startOffsetBytes + want - 1)));
    return new FutureWrapper<HTTPResponse, GcsFileMetadata>(urlfetch.fetchAsync(req)) {
      @Override
      protected GcsFileMetadata wrap(HTTPResponse resp) throws IOException {
        long totalLength;
        switch (resp.getResponseCode()) {
          case 200:
            totalLength = getLengthFromContentLength(resp);
            break;
          case 206:
            totalLength = getLengthFromContentRange(resp);
            break;
          case 404:
            throw new FileNotFoundException("Cound not find: " + filename);
          case 416:
            throw new BadRangeException("Requested Range not satisfiable; perhaps read past EOF? "
                + URLFetchUtils.describeRequestAndResponse(req, resp, true));
          default:
            throw handleError(req, resp);
        }
        byte[] content = resp.getContent();
        Preconditions.checkState(
            content.length <= want, "%s: got %s > wanted %s", this, content.length, want);
        dst.put(content);
        return getMetadataFromResponse(filename, resp, totalLength);
      }

      @Override
      protected Throwable convertException(Throwable e) {
        return OauthRawGcsService.convertException(e, req);
      }
    };
  }

  private static Throwable convertException(Throwable e, HTTPRequest req) {
    if (e instanceof IOException || e instanceof RuntimeException) {
      return e;
    } else {
      return createIOException(req, e);
    }
  }

  @Override
  public GcsFileMetadata getObjectMetadata(GcsFilename filename, long timeoutMillis)
      throws IOException {
    HTTPRequest req = makeRequest(filename, null, HTTPMethod.HEAD, timeoutMillis, headers);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(req, e);
    }
    int responseCode = resp.getResponseCode();
    if (responseCode == 404) {
      return null;
    }
    if (responseCode != 200) {
      throw handleError(req, resp);
    }
    return getMetadataFromResponse(filename, resp, getLengthFromContentLength(resp));
  }

  private GcsFileMetadata getMetadataFromResponse(
      GcsFilename filename, HTTPResponse resp, long length) {
    List<HTTPHeader> headers = resp.getHeaders();
    GcsFileOptions.Builder optionsBuilder = new GcsFileOptions.Builder();
    String etag = null;
    Date lastModified = null;
    for (HTTPHeader header : headers) {
      if (header.getName().startsWith(X_GOOG_META)) {
        String key = header.getName().substring(X_GOOG_META.length());
        String value = header.getValue();
        optionsBuilder.addUserMetadata(key, value);
      } else {
        switch (header.getName()) {
          case ACL:
            optionsBuilder.acl(header.getValue());
            break;
          case CACHE_CONTROL:
            optionsBuilder.cacheControl(header.getValue());
            break;
          case CONTENT_ENCODING:
            optionsBuilder.contentEncoding(header.getValue());
            break;
          case CONTENT_DISPOSITION:
            optionsBuilder.contentDisposition(header.getValue());
            break;
          case CONTENT_TYPE:
            optionsBuilder.mimeType(header.getValue());
            break;
          case ETAG:
            etag = header.getValue();
            break;
          case LAST_MODIFIED:
            lastModified = URLFetchUtils.parseDate(header.getValue());
            break;
          default:
        }
      }
    }
    GcsFileOptions options = optionsBuilder.build();
    return new GcsFileMetadata(filename, options, etag, length, lastModified);
  }

  @Override
  public int getChunkSizeBytes() {
    return CHUNK_ALIGNMENT_BYTES;
  }

  @Override
  public void setHttpHeaders(ImmutableSet<HTTPHeader> headers) {
    this.headers = headers;
  }
}
