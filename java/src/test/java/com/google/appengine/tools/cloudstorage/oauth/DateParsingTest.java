package com.google.appengine.tools.cloudstorage.oauth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Date;


/**
 * Tests {@link URLFetchUtils#parseDate(String)}
 */
@RunWith(JUnit4.class)
public class DateParsingTest {

  final
  @Test
  public void testRoundtrip() {
    Date now = new Date((System.currentTimeMillis() / 1000) * 1000);
    String printed = URLFetchUtils.DATE_FORMAT.print(now.getTime());
    assertEquals(now.getTime(), URLFetchUtils.parseDate(printed).getTime());
  }

  @Test
  public void testSampleDate() {
    String printed = "Tue, 22 Oct 2013 19:28:45 GMT";
    Date date = URLFetchUtils.parseDate(printed);
    assertNotNull(date);

    DateTimeFormatter gcsFormat =
        DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withZoneUTC();
    assertEquals(printed.substring(0, printed.length() - 4),
        gcsFormat.print(date.getTime()).substring(0, printed.length() - 4));
  }

}
