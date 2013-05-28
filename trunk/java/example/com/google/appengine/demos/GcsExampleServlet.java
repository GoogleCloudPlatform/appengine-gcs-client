package com.google.appengine.demos;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * A simple servlet that proxies reads and writes to its Google Cloud Storage bucket.
 */
public class GcsExampleServlet extends HttpServlet {

  private static final long serialVersionUID = -8289942698798877155L;

  private final GcsService gcsService = GcsServiceFactory.createGcsService();

  private static final int BUFFER_SIZE = 2 * 1024 * 1024;

  private String bucket = "ExampleBucket";

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeFileFromStream(req.getRequestURI().substring(1), req.getInputStream());
  }

  void writeFileFromStream(String name, InputStream inputStream) throws IOException {
    GcsFilename fileName = new GcsFilename(bucket, name);
    GcsOutputChannel outputChannel =
        gcsService.createOrReplace(fileName, GcsFileOptions.getDefaultInstance());
    try {
      copy(inputStream, Channels.newOutputStream(outputChannel));
    } finally {
      outputChannel.close();
      inputStream.close();
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeFileToStream(req.getRequestURI().substring(1), resp.getOutputStream());
  }

  void writeFileToStream(String name, OutputStream outputStream) throws IOException {
    GcsFilename fileName = new GcsFilename(bucket, name);
    ReadableByteChannel readChannel =
        gcsService.openPrefetchingReadChannel(fileName, 0, BUFFER_SIZE);
    try {
      copy(Channels.newInputStream(readChannel), outputStream);
    } finally {
      readChannel.close();
      outputStream.close();
    }
  }

  private void copy(InputStream input, OutputStream output) throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    int bytesRead = input.read(buffer);
    while (bytesRead != -1) {
      output.write(buffer, 0, bytesRead);
      bytesRead = input.read(buffer);
    }
  }

  /**
   * A main method to test to make sure the servlet works locally.
   */
  public static void main(String[] args) throws IOException {
    LocalServiceTestHelper helper = new LocalServiceTestHelper(
        new LocalTaskQueueTestConfig(), new LocalFileServiceTestConfig(),
        new LocalBlobstoreServiceTestConfig(), new LocalDatastoreServiceTestConfig());
    helper.setUp();
    try {
      GcsExampleServlet servlet = new GcsExampleServlet();
      byte[] content = new byte[] {1, 2, 3, 4, 5};
      ByteArrayInputStream contents = new ByteArrayInputStream(content);
      servlet.writeFileFromStream("/foo", contents);
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      servlet.writeFileToStream("/foo", outputStream);
      System.out.println("Wrote " + Arrays.toString(content) + " read: "
          + Arrays.toString(outputStream.toByteArray()));
    } finally {
      helper.tearDown();
    }
  }

}
