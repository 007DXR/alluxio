/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.s3;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.hamcrest.CoreMatchers.is;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import org.mockito.Mockito;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link S3InputStream}.
 */
public class S3InputStreamTest {

  private static final String BUCKET_NAME = "testBucket";
  private static final String OBJECT_KEY = "testObjectKey";

  private S3InputStream mS3InputStream;
  private S3Service mS3Service;
  private S3Object mS3Object;
  private InputStream mInputStreamSpy;

  @Rule
  public ExpectedException mExceptionRule = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() throws ServiceException {}

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() throws ServiceException {
    mInputStreamSpy = Mockito.spy(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    mS3Service = Mockito.mock(S3Service.class);
    mS3Object = Mockito.mock(S3Object.class);
    when(mS3Object.getDataInputStream()).thenReturn(mInputStreamSpy);
    when(mS3Service.getObject(BUCKET_NAME, OBJECT_KEY)).thenReturn(mS3Object);
    mS3InputStream = new S3InputStream(BUCKET_NAME, OBJECT_KEY, mS3Service);
  }

  @After
  public void tearDown() {}

  /**
   * Test of close method, of class S3InputStream.
   */
  @Test
  public void close() throws IOException {
    assertNotNull(mS3InputStream);
    mS3InputStream.close();
    verify(mInputStreamSpy).close();

    mExceptionRule.expect(IOException.class);
    mExceptionRule.expectMessage(is("Stream closed"));
    mS3InputStream.read();
  }

  /**
   * Test of read method, of class S3InputStream.
   */
  @Test
  public void read() throws IOException {
    assertNotNull(mS3InputStream);
    assertEquals(1, mS3InputStream.read());
    assertEquals(2, mS3InputStream.read());
    assertEquals(3, mS3InputStream.read());
  }

  /**
   * Test of read method, of class S3InputStream.
   */
  @Test
  public void readWithArgs() throws Exception {
    assertNotNull(mS3InputStream);
    byte[] bytes = new byte[3];
    int readedCount = mS3InputStream.read(bytes, 0, 3);
    assertEquals(3, readedCount);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
  }

  /**
   * Test of skip method, of class S3InputStream.
   */
  @Test
  public void skip() throws IOException {
    assertNotNull(mS3InputStream);
    assertEquals(1, mS3InputStream.read());
    mS3InputStream.skip(1);
    assertEquals(3, mS3InputStream.read());
  }
}
