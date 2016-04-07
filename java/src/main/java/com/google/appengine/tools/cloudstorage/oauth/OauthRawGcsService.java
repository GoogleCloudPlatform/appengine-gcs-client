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

import static com.google.appengine.api.urlfetch.HTTPMethod.DELETE;
import static com.google.appengine.api.urlfetch.HTTPMethod.GET;
import static com.google.appengine.api.urlfetch.HTTPMethod.HEAD;
import static com.google.appengine.api.urlfetch.HTTPMethod.POST;
import static com.google.appengine.api.urlfetch.HTTPMethod.PUT;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.appengine.api.urlfetch.FetchOptions;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.utils.FutureWrapper;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.repackaged.com.google.common.escape.Escaper;
import com.google.appengine.repackaged.com.google.common.xml.XmlEscapers;
import com.google.appengine.tools.cloudstorage.BadRangeException;
import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.appengine.tools.cloudstorage.oauth.URLFetchUtils.HTTPRequestInfo;
import com.google.appengine.tools.cloudstorage.oauth.XmlHandler.EventType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import javax.xml.stream.XMLStreamException;

/**
 * A wrapper around the Google Cloud Storage REST API.  The subset of features
 * exposed here is intended to be appropriate for implementing
 * {@link RawGcsService}.
 */
final class OauthRawGcsService implements RawGcsService {

  private static final String X_GOOG_PREFIX = "x-goog-";
  private static final String ACL = X_GOOG_PREFIX + "acl";
  private static final String CACHE_CONTROL = "Cache-Control";
  private static final String CONTENT_ENCODING = "Content-Encoding";
  private static final String CONTENT_DISPOSITION = "Content-Disposition";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String CONTENT_RANGE = "Content-Range";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_MD5 = "Content-MD5";
  private static final String ETAG = "ETag";
  private static final String LAST_MODIFIED = "Last-Modified";
  private static final String LOCATION = "Location";
  private static final String RANGE = "Range";
  private static final String UPLOAD_ID = "upload_id";
  private static final String PREFIX = "prefix";
  private static final String MARKER = "marker";
  private static final String MAX_KEYS = "max-keys";
  private static final String DELIMITER = "delimiter";
  private static final String X_GOOG_META = X_GOOG_PREFIX + "meta-";
  private static final String X_GOOG_CONTENT_LENGTH =  X_GOOG_PREFIX + "stored-content-length";
  private static final String X_GOOG_COPY_SOURCE = X_GOOG_PREFIX + "copy-source";
  private static final String STORAGE_API_HOSTNAME = "storage.googleapis.com";
  public static final String USER_AGENT_PRODUCT = "AppEngine-Java-GCS";
  private static final HTTPHeader RESUMABLE_HEADER =
      new HTTPHeader(X_GOOG_PREFIX + "resumable", "start");
  private static final HTTPHeader REPLACE_METADATA_HEADER =
      new HTTPHeader(X_GOOG_PREFIX + "metadata-directive", "REPLACE");
  private static final HTTPHeader USER_AGENT =
      new HTTPHeader("User-Agent", USER_AGENT_PRODUCT);
  private static final HTTPHeader ZERO_CONTENT_LENGTH = new HTTPHeader(CONTENT_LENGTH, "0");
  private static final Map<String, String> COMPOSE_QUERY_STRINGS =
      Collections.singletonMap("compose", null);
  private static final byte[] EMPTY_PAYLOAD = new byte[0];
  private static final Logger log = Logger.getLogger(OauthRawGcsService.class.getName());

  public static final List<String> OAUTH_SCOPES =
      ImmutableList.of("https://www.googleapis.com/auth/devstorage.full_control");

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
  @SuppressWarnings("unused")
  private final Storage storage;
  private final ImmutableSet<HTTPHeader> headers;

  OauthRawGcsService(OAuthURLFetchService urlfetch, ImmutableSet<HTTPHeader> headers) {
    this.urlfetch = checkNotNull(urlfetch, "Null urlfetch");
    this.headers = checkNotNull(headers, "Null headers");
    AppIdentityCredential cred = new AppIdentityCredential(OAUTH_SCOPES);
    storage = new Storage.Builder(new UrlFetchTransport(), new JacksonFactory(), cred)
        .setApplicationName(SystemProperty.applicationId.get()).build();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + urlfetch + ")";
  }

  static String makePath(GcsFilename filename) {
    return new StringBuilder()
        .append('/').append(filename.getBucketName())
        .append('/').append(filename.getObjectName())
        .toString();
  }

  @VisibleForTesting
  static URL makeUrl(GcsFilename filename, @Nullable Map<String, String> queryStrings) {
    String path = makePath(filename);
    try {
      StringBuilder url =
          new StringBuilder().append(new URI("https", STORAGE_API_HOSTNAME, path, null));
      if (queryStrings != null && !queryStrings.isEmpty()) {
        url.append('?');
        for (Map.Entry<String, String> entry : queryStrings.entrySet()) {
          url.append(URLEncoder.encode(entry.getKey(), UTF_8.name()));
          if (entry.getValue() != null) {
            url.append('=').append(URLEncoder.encode(entry.getValue(), UTF_8.name()));
          }
          url.append('&');
        }
        url.setLength(url.length() - 1);
      }
      return new URL(url.toString());
    } catch (MalformedURLException | URISyntaxException | UnsupportedEncodingException e) {
      throw new RuntimeException(
          "Could not create a URL for " + filename + " with query " + queryStrings, e);
    }
  }

  @VisibleForTesting
  HTTPRequest makeRequest(GcsFilename filename, @Nullable Map<String, String> queryStrings,
      HTTPMethod method, long timeoutMillis) {
    return makeRequest(filename, queryStrings, method, timeoutMillis, EMPTY_PAYLOAD);
  }

  private HTTPRequest makeRequest(GcsFilename filename, @Nullable Map<String, String> queryStrings,
      HTTPMethod method, long timeoutMillis, ByteBuffer payload) {
    return makeRequest(filename, queryStrings, method, timeoutMillis, peekBytes(payload));
  }

  @VisibleForTesting
  HTTPRequest makeRequest(GcsFilename filename, @Nullable Map<String, String> queryStrings,
      HTTPMethod method, long timeoutMillis, byte[] payload) {
    HTTPRequest request = new HTTPRequest(makeUrl(filename, queryStrings), method,
        FetchOptions.Builder.disallowTruncate()
            .doNotFollowRedirects()
            .validateCertificate()
            .setDeadline(timeoutMillis / 1000.0));
    for (HTTPHeader header : headers) {
      request.addHeader(header);
    }
    request.addHeader(USER_AGENT);
    if (payload != null && payload.length > 0) {
      request.setHeader(new HTTPHeader(CONTENT_LENGTH, String.valueOf(payload.length)));
      try {
        request.setHeader(new HTTPHeader(CONTENT_MD5,
            BaseEncoding.base64().encode(MessageDigest.getInstance("MD5").digest(payload))));
      } catch (NoSuchAlgorithmException e) {
        log.severe(
            "Unable to get a MessageDigest instance, no Content-MD5 header sent.\n" + e.toString());
      }
      request.setPayload(payload);
    } else {
      request.setHeader(ZERO_CONTENT_LENGTH);
    }
    return request;
  }

  @Override
  public int getMaxWriteSizeByte() {
    return WRITE_LIMIT_BYTES;
  }

  @Override
  public RawGcsCreationToken beginObjectCreation(
      GcsFilename filename, GcsFileOptions options, long timeoutMillis) throws IOException {
    HTTPRequest req = makeRequest(filename, null, POST, timeoutMillis);
    req.setHeader(RESUMABLE_HEADER);
    addOptionsHeaders(req, options);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
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
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
  }

  private static IOException createIOException(HTTPRequestInfo req, Throwable ex) {
    StringBuilder b = new StringBuilder("URLFetch threw IOException; request: ");
    req.appendToString(b);
    return new IOException(b.toString(), ex);
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
    HTTPRequest req = makeRequest(filename, null, PUT, timeoutMillis, content);
    addOptionsHeaders(req, options);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    if (resp.getResponseCode() != 200) {
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
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
    Map<String, String> queryStrings = Collections.singletonMap(UPLOAD_ID, token.uploadId);
    final HTTPRequest req =
        makeRequest(token.filename, queryStrings, PUT, timeoutMillis, chunk);
    req.setHeader(
        new HTTPHeader(CONTENT_RANGE,
            "bytes " + (length == 0 ? "*" : offset + "-" + (limit - 1))
            + (isFinalChunk ? "/" + limit : "/*")));
    return req;
  }

  /**
   * Given a HTTPResponce, process it, throwing an error if needed and return a Token for the next
   * request.
   */
  GcsRestCreationToken handlePutResponse(final GcsRestCreationToken token,
      final boolean isFinalChunk,
      final int length,
      final HTTPRequestInfo reqInfo,
      HTTPResponse resp) throws Error, IOException {
    switch (resp.getResponseCode()) {
      case 200:
        if (!isFinalChunk) {
          throw new RuntimeException("Unexpected response code 200 on non-final chunk. Request: \n"
              + URLFetchUtils.describeRequestAndResponse(reqInfo, resp));
        } else {
          return null;
        }
      case 308:
        if (isFinalChunk) {
          throw new RuntimeException("Unexpected response code 308 on final chunk: "
              + URLFetchUtils.describeRequestAndResponse(reqInfo, resp));
        } else {
          return new GcsRestCreationToken(token.filename, token.uploadId, token.offset + length);
        }
      default:
        throw HttpErrorHandler.error(resp.getResponseCode(),
            URLFetchUtils.describeRequestAndResponse(reqInfo, resp));
    }
  }

  /**
   * Write the provided chunk at the offset specified in the token. If finalChunk is set, the file
   * will be closed.
   */
  private RawGcsCreationToken put(final GcsRestCreationToken token, ByteBuffer chunk,
      final boolean isFinalChunk, long timeoutMillis) throws IOException {
    final int length = chunk.remaining();
    HTTPRequest req = createPutRequest(token, chunk, isFinalChunk, timeoutMillis, length);
    HTTPRequestInfo info = new HTTPRequestInfo(req);
    HTTPResponse response;
    try {
      response = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(info, e);
    }
    return handlePutResponse(token, isFinalChunk, length, info, response);
  }

  /**
   * Same as {@link #put} but is runs asynchronously and returns a future. In the event of an error
   * the exception out of the future will be an ExecutionException with the cause set to the same
   * exception that would have been thrown by put.
   */
  private Future<RawGcsCreationToken> putAsync(final GcsRestCreationToken token,
      ByteBuffer chunk, final boolean isFinalChunk, long timeoutMillis) {
    final int length = chunk.remaining();
    HTTPRequest request = createPutRequest(token, chunk, isFinalChunk, timeoutMillis, length);
    final HTTPRequestInfo info = new HTTPRequestInfo(request);
    return new FutureWrapper<HTTPResponse, RawGcsCreationToken>(urlfetch.fetchAsync(request)) {
      @Override
      protected Throwable convertException(Throwable e) {
        return OauthRawGcsService.convertException(info, e);
      }

      @Override
      protected GcsRestCreationToken wrap(HTTPResponse resp) throws Exception {
        return handlePutResponse(token, isFinalChunk, length, info, resp);
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
    HTTPRequest req = makeRequest(filename, null, DELETE, timeoutMillis);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    switch (resp.getResponseCode()) {
      case 204:
        return true;
      case 404:
        return false;
      default:
        throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
  }


  private long getLengthFromContentRange(HTTPResponse resp) {
    String range = URLFetchUtils.getSingleHeader(resp, CONTENT_RANGE);
    Preconditions.checkState(range.matches("bytes [0-9]+-[0-9]+/[0-9]+"),
        "%s: unexpected " + CONTENT_RANGE + ": %s", this, range);
    return Long.parseLong(range.substring(range.indexOf("/") + 1));
  }

  private long getLengthFromHeader(HTTPResponse resp, String header) {
    return Long.parseLong(URLFetchUtils.getSingleHeader(resp, header));
  }

  /**
   * Might not fill all of dst.
   */
  @Override
  public Future<GcsFileMetadata> readObjectAsync(final ByteBuffer dst, final GcsFilename filename,
      long startOffsetBytes, long timeoutMillis) {
    Preconditions.checkArgument(startOffsetBytes >= 0, "%s: offset must be non-negative: %s", this,
        startOffsetBytes);
    final int n = dst.remaining();
    Preconditions.checkArgument(n > 0, "%s: dst full: %s", this, dst);
    final int want = Math.min(READ_LIMIT_BYTES, n);

    final HTTPRequest req = makeRequest(filename, null, GET, timeoutMillis);
    req.setHeader(
        new HTTPHeader(RANGE, "bytes=" + startOffsetBytes + "-" + (startOffsetBytes + want - 1)));
    final HTTPRequestInfo info = new HTTPRequestInfo(req);
    return new FutureWrapper<HTTPResponse, GcsFileMetadata>(urlfetch.fetchAsync(req)) {
      @Override
      protected GcsFileMetadata wrap(HTTPResponse resp) throws IOException {
        long totalLength;
        switch (resp.getResponseCode()) {
          case 200:
            totalLength = getLengthFromHeader(resp, X_GOOG_CONTENT_LENGTH);
            break;
          case 206:
            totalLength = getLengthFromContentRange(resp);
            break;
          case 404:
            throw new FileNotFoundException("Could not find: " + filename);
          case 416:
            throw new BadRangeException("Requested Range not satisfiable; perhaps read past EOF? "
                + URLFetchUtils.describeRequestAndResponse(info, resp));
          default:
            throw HttpErrorHandler.error(info, resp);
        }
        byte[] content = resp.getContent();
        Preconditions.checkState(content.length <= want, "%s: got %s > wanted %s", this,
            content.length, want);
        dst.put(content);
        return getMetadataFromResponse(filename, resp, totalLength);
      }

      @Override
      protected Throwable convertException(Throwable e) {
        return OauthRawGcsService.convertException(info, e);
      }
    };
  }

  private static Throwable convertException(HTTPRequestInfo info, Throwable e) {
    if (e instanceof IOException || e instanceof RuntimeException) {
      return e;
    } else {
      return new IOException("URLFetch threw IOException; request: " + info, e);
    }
  }

  @Override
  public GcsFileMetadata getObjectMetadata(GcsFilename filename, long timeoutMillis)
      throws IOException {
    HTTPRequest req = makeRequest(filename, null, HEAD, timeoutMillis);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    int responseCode = resp.getResponseCode();
    if (responseCode == 404) {
      return null;
    }
    if (responseCode != 200) {
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
    return getMetadataFromResponse(
        filename, resp, getLengthFromHeader(resp, X_GOOG_CONTENT_LENGTH));
  }

  private GcsFileMetadata getMetadataFromResponse(
      GcsFilename filename, HTTPResponse resp, long length) {
    List<HTTPHeader> headers = resp.getHeaders();
    GcsFileOptions.Builder optionsBuilder = new GcsFileOptions.Builder();
    String etag = null;
    Date lastModified = null;
    ImmutableMap.Builder<String, String> xGoogHeaders = ImmutableMap.builder();
    for (HTTPHeader header : headers) {
      if (header.getName().startsWith(X_GOOG_PREFIX)) {
        if (header.getName().startsWith(X_GOOG_META)) {
          String key = header.getName().substring(X_GOOG_META.length());
          String value = header.getValue();
          optionsBuilder.addUserMetadata(key, value);
        } else {
          String key = header.getName().substring(X_GOOG_PREFIX.length());
          String value = header.getValue();
          xGoogHeaders.put(key, value);
        }
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
    return new GcsFileMetadata(filename, options, etag, length, lastModified, xGoogHeaders.build());
  }

  @Override
  public int getChunkSizeBytes() {
    return CHUNK_ALIGNMENT_BYTES;
  }

  @Override
  public void composeObject(Iterable<String> source, GcsFilename dest, long timeoutMillis)
      throws IOException {
    StringBuilder xmlContent = new StringBuilder(Iterables.size(source) * 50);
    Escaper escaper = XmlEscapers.xmlContentEscaper();
    xmlContent.append("<ComposeRequest>");
    for (String srcFileName : source) {
      xmlContent.append("<Component><Name>")
          .append(escaper.escape(srcFileName))
          .append("</Name></Component>");
    }
    xmlContent.append("</ComposeRequest>");
    HTTPRequest req = makeRequest(
        dest, COMPOSE_QUERY_STRINGS, PUT, timeoutMillis, xmlContent.toString().getBytes(UTF_8));
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    if (resp.getResponseCode() != 200) {
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
  }

  @Override
  public void copyObject(GcsFilename source, GcsFilename dest, GcsFileOptions fileOptions,
      long timeoutMillis) throws IOException {
    HTTPRequest req = makeRequest(dest, null, PUT, timeoutMillis);
    req.setHeader(new HTTPHeader(X_GOOG_COPY_SOURCE, makePath(source)));
    if (fileOptions != null) {
      req.setHeader(REPLACE_METADATA_HEADER);
      addOptionsHeaders(req, fileOptions);
    }
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    if (resp.getResponseCode() != 200) {
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
  }

  static final List<String> NEXT_MARKER = ImmutableList.of("ListBucketResult", "NextMarker");
  static final List<String> CONTENTS_KEY = ImmutableList.of("ListBucketResult", "Contents", "Key");
  static final List<String> CONTENTS_LAST_MODIFIED =
      ImmutableList.of("ListBucketResult", "Contents", "LastModified");
  static final List<String> CONTENTS_ETAG =
      ImmutableList.of("ListBucketResult", "Contents", "ETag");
  static final List<String> CONTENTS_SIZE =
      ImmutableList.of("ListBucketResult", "Contents", "Size");
  static final List<String> COMMON_PREFIXES_PREFIX =
      ImmutableList.of("ListBucketResult", "CommonPrefixes", "Prefix");

  @SuppressWarnings("unchecked")
  static final Set<List<String>> PATHS = ImmutableSet.of(NEXT_MARKER, CONTENTS_KEY,
      CONTENTS_LAST_MODIFIED, CONTENTS_ETAG, CONTENTS_SIZE, COMMON_PREFIXES_PREFIX);

  @Override
  public ListItemBatch list(String bucket, String prefix, String delimiter, String marker,
      int maxResults,
      long timeoutMillis) throws IOException {
    GcsFilename filename = new GcsFilename(bucket, "");
    Map<String, String> queryStrings = new LinkedHashMap<>();
    if (!Strings.isNullOrEmpty(prefix)) {
      queryStrings.put(PREFIX, prefix);
    }
    if (!Strings.isNullOrEmpty(delimiter)) {
      queryStrings.put(DELIMITER, delimiter);
    }
    if (!Strings.isNullOrEmpty(marker)) {
      queryStrings.put(MARKER, marker);
    }
    if (maxResults >= 0) {
      queryStrings.put(MAX_KEYS, String.valueOf(maxResults));
    }
    HTTPRequest req = makeRequest(filename, queryStrings, GET, timeoutMillis);
    HTTPResponse resp;
    try {
      resp = urlfetch.fetch(req);
    } catch (IOException e) {
      throw createIOException(new HTTPRequestInfo(req), e);
    }
    if (resp.getResponseCode() != 200) {
      throw HttpErrorHandler.error(new HTTPRequestInfo(req), resp);
    }
    String nextMarker = null;
    List<ListItem> items = new ArrayList<>();
    try {
      XmlHandler xmlHandler = new XmlHandler(resp.getContent(), PATHS);
      while (xmlHandler.hasNext()) {
        XmlHandler.XmlEvent event = xmlHandler.next();
        if (event.getEventType() == EventType.CLOSE_ELEMENT) {
          switch (event.getName()) {
            case "NextMarker":
              nextMarker = event.getValue();
              break;
            case "Prefix":
              String name = event.getValue();
              items.add(new ListItem.Builder().setName(name).setDirectory(true).build());
              break;
            default:
              break;
          }
        } else if (event.getName().equals("Contents")) {
          items.add(parseContents(xmlHandler));
        }
      }
    } catch (XMLStreamException e) {
      throw HttpErrorHandler.createException("Failed to parse response", e.getMessage());
    }
    return new ListItemBatch(items, nextMarker);
  }

  private ListItem parseContents(XmlHandler xmlHandler) throws XMLStreamException {
    ListItem.Builder builder = new ListItem.Builder();
    boolean isDone = false;
    while (!isDone && xmlHandler.hasNext()) {
      XmlHandler.XmlEvent event = xmlHandler.next();
      if (event.getEventType() == EventType.OPEN_ELEMENT) {
        continue;
      }
      switch (event.getName()) {
        case "Key":
          builder.setName(event.getValue());
          break;
        case "LastModified":
          builder.setLastModified(DatatypeConverter.parseDateTime(event.getValue()).getTime());
          break;
        case "ETag":
          builder.setEtag(event.getValue());
          break;
        case "Size":
          builder.setLength(DatatypeConverter.parseLong(event.getValue()));
          break;
        case "Contents":
          isDone = true;
          break;
        default:
          break;
      }
    }
    return builder.build();
  }
}
