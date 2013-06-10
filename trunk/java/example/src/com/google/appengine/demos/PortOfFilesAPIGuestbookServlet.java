
package com.google.appengine.demos;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Create, Write, Read, and Finalize Cloud Storage objects.
 * Access your app at: http://myapp.appspot.com/
 */
@SuppressWarnings("serial")
public class PortOfFilesAPIGuestbookServlet extends HttpServlet {
   public static final String BUCKETNAME = "ExampleBucketName";
   public static final String FILENAME = "ExampleFileName";

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("text/plain");
    resp.getWriter().println("Hello, world from java");
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename(BUCKETNAME, FILENAME);
    GcsFileOptions options = new GcsFileOptions.Builder().mimeType("text/html")
        .acl("public-read").addUserMetadata("myfield1", "my field value").build();
    GcsOutputChannel writeChannel = gcsService.createOrReplace(filename, options);

    PrintWriter out = new PrintWriter(Channels.newWriter(writeChannel, "UTF8"));
    out.println("The woods are lovely dark and deep.");
    out.println("But I have promises to keep.");
    out.flush();

    writeChannel.waitForOutstandingWrites();

    writeChannel.write(ByteBuffer.wrap("And miles to go before I sleep.".getBytes()));

    writeChannel.close();
    resp.getWriter().println("Done writing...");


    GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0);
    BufferedReader reader = new BufferedReader(Channels.newReader(readChannel, "UTF8"));
    String line;
    while ((line = reader.readLine()) != null) {
      resp.getWriter().println("READ:" + line);
    }
    readChannel.close();
  }

}
