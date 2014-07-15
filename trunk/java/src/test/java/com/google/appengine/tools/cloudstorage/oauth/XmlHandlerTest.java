package com.google.appengine.tools.cloudstorage.oauth;

import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.COMMON_PREFIXES_PREFIX;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.CONTENTS_ETAG;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.CONTENTS_KEY;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.CONTENTS_LAST_MODIFIED;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.CONTENTS_SIZE;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.NEXT_MARKER;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.PATHS;
import static com.google.appengine.tools.cloudstorage.oauth.XmlHandler.EventType.CLOSE_ELEMENT;
import static com.google.appengine.tools.cloudstorage.oauth.XmlHandler.EventType.OPEN_ELEMENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import com.google.appengine.tools.cloudstorage.oauth.XmlHandler.XmlEvent;
import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLStreamException;


/**
 * Tests {@link XmlHandler}
 */
@RunWith(JUnit4.class)
public class XmlHandlerTest {

  private static final String CONTENT = "<?xml version=\"1.0\"?>\n"
      + "<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>\n"
      + "   <Name>mybucket</Name>\n\n"
      + "   <Prefix>europe/</Prefix>\n"
      + "   <Marker />\n"
      + "   <IsTruncated>false</IsTruncated>\n"
      + "   <Contents>\n"
      + "       <Key>europe/finland.jpg</Key>\n"
      + "       <Generation>1360887659323000</Generation>\n"
      + "       <MetaGeneration>1</MetaGeneration>\n"
      + "       <LastModified>2010-02-17T03:12:55.561Z</LastModified>\n"
      + "       <ETag>\"etag1\"</ETag>\n"
      + "       <Size>10</Size>\n"
      + "       <StorageClass>STANDARD</StorageClass>\n"
      + "       <Owner>\n"
      + "           <ID>84fac329bceSAMPLE777d5d22b8SAMPLE77d85ac2SAMPLE2dfcf7c4adf34da46</ID>\n"
      + "       </Owner>\n"
      + "   </Contents>\n"
      + "   <Contents>\n"
      + "       <Key>europe/norway.jpg</Key>\n"
      + "       <Generation>1360887659323000</Generation>\n"
      + "       <MetaGeneration>1</MetaGeneration>\n"
      + "       <LastModified>2010-03-17T03:12:55.561Z</LastModified>\n"
      + "       <ETag>\"etag2\"</ETag>\n"
      + "       <Size>20</Size>\n"
      + "       <StorageClass>STANDARD</StorageClass>\n"
      + "       <Owner>\n"
      + "           <ID>84fac329bceSAMPLE777d5d22b8SAMPLE77d85ac2SAMPLE2dfcf7c4adf34da46</ID>\n"
      + "       </Owner>\n"
      + "   </Contents>\n"
      + "   <CommonPrefixes>\n"
      + "       <Prefix>europe/france/</Prefix>\n"
      + "   </CommonPrefixes>\n"
      + "   <CommonPrefixes>\n"
      + "       <Prefix>europe/italy/</Prefix>\n"
      + "   </CommonPrefixes>\n"
      + "   <CommonPrefixes>\n"
      + "       <Prefix>europe/sweden/</Prefix>\n"
      + "   </CommonPrefixes>\n"
      + "   </ListBucketResult>\n";

  private static final List<String> LIST_BUCKET_RESULTS = ImmutableList.of("ListBucketResult");
  private static final List<String> CONTENTS = ImmutableList.of("ListBucketResult", "Contents");
  private static final List<String> COMMON_PREFIXES =
      ImmutableList.of("ListBucketResult", "CommonPrefixes");
  private static final List<XmlEvent> EXPECTED = ImmutableList.of(
      new XmlEvent(OPEN_ELEMENT, LIST_BUCKET_RESULTS, null),
      new XmlEvent(OPEN_ELEMENT, CONTENTS, null),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_KEY, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_KEY, "europe/finland.jpg"),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_LAST_MODIFIED, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_LAST_MODIFIED, "2010-02-17T03:12:55.561Z"),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_ETAG, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_ETAG, "\"etag1\""),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_SIZE, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_SIZE, "10"),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS, ""),
      new XmlEvent(OPEN_ELEMENT, CONTENTS, null),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_KEY, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_KEY, "europe/norway.jpg"),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_LAST_MODIFIED, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_LAST_MODIFIED, "2010-03-17T03:12:55.561Z"),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_ETAG, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_ETAG, "\"etag2\""),
      new XmlEvent(OPEN_ELEMENT, CONTENTS_SIZE, null),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS_SIZE, "20"),
      new XmlEvent(CLOSE_ELEMENT, CONTENTS, ""),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES, null),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES_PREFIX, null),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES_PREFIX, "europe/france/"),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES, ""),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES, null),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES_PREFIX, null),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES_PREFIX, "europe/italy/"),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES, ""),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES, null),
      new XmlEvent(OPEN_ELEMENT, COMMON_PREFIXES_PREFIX, null),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES_PREFIX, "europe/sweden/"),
      new XmlEvent(CLOSE_ELEMENT, COMMON_PREFIXES, ""),
      new XmlEvent(CLOSE_ELEMENT, LIST_BUCKET_RESULTS, ""));

  @Test
  public void testHandler() throws XMLStreamException {
    verify(EXPECTED, CONTENT);

    String content = CONTENT.replace("<Marker />", "<Marker /><NextMarker>bla</NextMarker>");
    List<XmlEvent> expected = new ArrayList<>(EXPECTED.size() + 1);
    expected.add(EXPECTED.get(0));
    expected.add(new XmlEvent(OPEN_ELEMENT, NEXT_MARKER, null));
    expected.add(new XmlEvent(CLOSE_ELEMENT, NEXT_MARKER, "bla"));
    expected.addAll(EXPECTED.subList(1, EXPECTED.size()));
    verify(expected, content);
  }

  private void verify(List<XmlEvent> expected, String content) throws XMLStreamException {
    XmlHandler handler = new XmlHandler(content.getBytes(UTF_8), PATHS);
    Iterator<XmlEvent> expectedIter = expected.iterator();
    while (handler.hasNext()) {
      if (!expectedIter.hasNext()) {
        fail("Unexpected " + handler.next());
      }
      assertEquals(expectedIter.next(), handler.next());
    }
    assertFalse(expectedIter.hasNext());
  }
}