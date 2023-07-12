package alluxio.client.rest;

import static org.junit.Assert.assertEquals;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPOptions;
import alluxio.master.journal.JournalType;
import alluxio.proxy.s3.S3Constants;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.List;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class CreateObjectTest extends RestApiTest {
  private FileSystem mFileSystem;
  private static final String TEST_BUCKET = "test-bucket";
  private AmazonS3 mS3Client = null;
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeProxy(true)
          .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, Constants.MB * 16)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Long.MAX_VALUE)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, Constants.MB * 16 / 2)
          .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "1GB")
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.NOOP)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001")
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
          .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.WORKER_HTTP_SERVER_ENABLED, false)
          .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
          .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
          .setNumWorkers(2)
          .setStartCluster(false)
          .build();

  @Before
  public void before() throws Exception {
    mLocalAlluxioClusterResource.start();

    mS3Client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(mS3Proxy.getUri().toString(),
                Regions.US_WEST_2.getName()))
        .build();
    mS3Client.createBucket(TEST_BUCKET);
    mHostname = mLocalAlluxioClusterResource.get().getHostname();
    mPort = mLocalAlluxioClusterResource.get().getProxyProcess().getWebLocalPort();
    mBaseUri = String.format("/api/v1/s3");
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() {
    mS3Client = null;
  }

  @Test
  public void putObject() throws Exception {
    final String bucket = "bucket";
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());
    Assert.assertArrayEquals(object, getObjectRestCall(objectKey).getBytes());
  }

  @Test
  public void overwriteObject() throws Exception {
    final String bucket = "bucket";
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());
    Assert.assertArrayEquals(object, getObjectRestCall(objectKey).getBytes());
//   This object will be overwritten.
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object2, TEST_USER_NAME).getResponseCode());
    Assert.assertArrayEquals(object2, getObjectRestCall(objectKey).getBytes());
  }

  @Test
  public void getNonexistentObject() throws Exception {
    final String bucket = "bucket";
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        objectKey, NO_PARAMS, HttpMethod.GET,
        getDefaultOptionsWithAuth()).execute();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_KEY, response.getCode());
  }

  @Test
  public void putDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder0"+AlluxioURI.SEPARATOR + "folder1/";
    final byte[] object = new byte[] {};

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());

    Assert.assertEquals(0, headRestCall(objectKey).getContentLength());
    Assert.assertEquals(0, headRestCall(objectKey).getContentLength());
//    Assert.assertArrayEquals(object, getObjectRestCall(objectKey).is());
  }
//  @Test
//  public void putObjectAndDirectoryWithSameName() throws Exception {
//    final String bucket = "bucket";
//
//    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder";
//    final String foldertKey = objectKey + AlluxioURI.SEPARATOR ;
//
//
//    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
//    Assert.assertEquals(Response.Status.OK.getStatusCode(),
//        createBucketRestCall(bucket).getResponseCode());
//
//    Assert.assertEquals(Response.Status.OK.getStatusCode(),
//        createObjectRestCall(objectKey, NO_PARAMS, object,TEST_USER_NAME).getResponseCode());
//    HttpURLConnection connection=headRestCall(objectKey);
//    Assert.assertEquals(Constants.KB, connection.getContentLength());
//
//    Assert.assertEquals(Response.Status.OK.getStatusCode(),
//        createObjectRestCall(foldertKey, NO_PARAMS, new byte[] {},TEST_USER_NAME).getResponseCode());
//
//    List<URIStatus> statuses = mFileSystem.listStatus(new AlluxioURI("/bucket"),
//        ListStatusPOptions.newBuilder().setRecursive(true).build());
//    ListBucketResult expected = new ListBucketResult("bucket", statuses,
//        ListBucketOptions.defaults().setDelimiter(AlluxioURI.SEPARATOR));
////    assertEquals(2, statuses.size());
////    assertEquals(false, statuses.get(0).isFolder());
////    assertEquals(false, statuses.get(1).isFolder());
//    assertEquals(Constants.KB, statuses.get(0).getLength());
////    assertEquals(Constants.KB, statuses.get(1).getLength());
//    assertEquals("folder", expected.getContents().get(0).getKey());
////    assertEquals("folder/object", expected.getContents().get(1).getKey());
//    assertEquals("folder/", expected.getCommonPrefixes().get(0).getPrefix());
////    assertEquals("folder0/", expected.getCommonPrefixes().get(0).getPrefix());
////    assertEquals("folder0/", expected.getCommonPrefixes().get(1).getPrefix());
////    connection=headRestCall(objectKey);
////    Assert.assertEquals(Constants.KB, connection.getContentLength());
////     connection=headRestCall(objectKey2);
////    Assert.assertEquals(Constants.KB, connection.getContentLength());
////    connection=headRestCall(foldertKey);
////    Assert.assertEquals(0, connection.getContentLength());
//  }

  /**
   * Creates object first, and then creates directory with the same name.
   */
  @Test
  public void putObjectAndDirectoryWithSameName() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder";
    final String objectKey2 = objectKey + AlluxioURI.SEPARATOR + "object";
    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB * 2).getBytes();

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey2, NO_PARAMS, object2, TEST_USER_NAME).getResponseCode());

    Assert.assertEquals(Constants.KB, headRestCall(objectKey).getContentLength());
    Assert.assertEquals(Constants.KB * 2, headRestCall(objectKey2).getContentLength());
  }

  /**
   * Creates directory first, and then creates object with the same name.
   */
  @Test
  public void putDirectoryAndObjectWithSameName() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder";
    final String objectKey2 = objectKey + AlluxioURI.SEPARATOR + "object";
    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();
    final byte[] object2 = CommonUtils.randomAlphaNumString(Constants.KB * 2).getBytes();

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey2, NO_PARAMS, object2, TEST_USER_NAME).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());

    Assert.assertArrayEquals(object, getObjectRestCall(objectKey).getBytes());
    Assert.assertArrayEquals(object2, getObjectRestCall(objectKey2).getBytes());
  }

  @Test
  public void putDirectoryToNonexistentBucket() throws Exception {

    final String bucket = "non-existent-bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";
    final byte[] object = new byte[] {};
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(bucket).getResponseCode());
    HttpURLConnection connection =
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }


  @Test
  public void putObjectToNonExistentBucket() throws Exception {
    final String bucket = "non-existent-bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(bucket).getResponseCode());
    HttpURLConnection connection =
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void putObjectWithWrongMD5() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    HttpURLConnection connection = createObjectRestCall(objectKey, NO_PARAMS,
        getDefaultOptionsWithAuth()
            .setBody(object)
            .setContentType(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE)
            .setMD5(null));
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.BAD_DIGEST, response.getCode());
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToAnotherBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String sourcePath = "bucket1/object";
    final String targetPath = "bucket2/object";

    final byte[] object = "Hello World!".getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket1).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket2).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(sourcePath, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());

    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).runAndGetResponse();

    Assert.assertArrayEquals(object, getObjectRestCall(targetPath).getBytes());
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToNonexistentBucket() throws Exception {
    final String bucket1 = "bucket1";
    final String bucket2 = "bucket2";
    final String sourcePath = "bucket1/object";
    final String targetPath = "bucket2/object";

    final byte[] object = "Hello World!".getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket1).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(sourcePath, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(bucket2).getResponseCode());
    // copy object
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).execute();

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(targetPath, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObjectToAnotherFolder() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/sourceDir/object";
    final String targetPath = "bucket/targetDir/object";

    final byte[] object = "Hello World!".getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(sourcePath, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());


    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).runAndGetResponse();

    Assert.assertArrayEquals(object, getObjectRestCall(targetPath).getBytes());
  }

  /**
   * Copies an object and renames it.
   */
  @Test
  public void copyObjectAsAnotherObject() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/object1";
    final String targetPath = "bucket/object2";

    final byte[] object = "Hello World!".getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(sourcePath, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());


    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).runAndGetResponse();

    Assert.assertArrayEquals(object, getObjectRestCall(targetPath).getBytes());
  }

  /**
   * Copies an object from one folder to a different folder.
   */
  @Test
  public void copyObject() throws Exception {
    final String bucket = "bucket";
    final String sourcePath = "bucket/sourceDir/object";
    final String targetPath = "bucket/targetDir/object";

    final byte[] object = "Hello World!".getBytes();
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(sourcePath, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());


    // copy object
    new TestCase(mHostname, mPort, mBaseUri,
        targetPath,
        NO_PARAMS, HttpMethod.PUT,
        getDefaultOptionsWithAuth()
            .addHeader(S3Constants.S3_METADATA_DIRECTIVE_HEADER,
                S3Constants.Directive.REPLACE.name())
            .addHeader(S3Constants.S3_COPY_SOURCE_HEADER, sourcePath)).runAndGetResponse();

    Assert.assertArrayEquals(object, getObjectRestCall(targetPath).getBytes());
  }

  @Test
  public void deleteObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";
    final byte[] object = "Hello World!".getBytes();

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());
    deleteRestCall(objectKey);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());

  }

  @Test
  public void deleteObjectInNonexistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";

//    deleteRestCall(objectKey);
    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        objectKey, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).execute();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void deleteNonexistentObject() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "object";

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());
    deleteRestCall(objectKey);

  }

  @Test
  public void deleteDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, new byte[] {},
            TEST_USER_NAME).getResponseCode());
    deleteRestCall(objectKey);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());
  }

  @Test
  public void deleteDirectoryInNonexistentBucket() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";

    HttpURLConnection connection = new TestCase(mHostname, mPort, mBaseUri,
        objectKey, NO_PARAMS, HttpMethod.DELETE,
        getDefaultOptionsWithAuth()).execute();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), connection.getResponseCode());
    S3Error response =
        new XmlMapper().readerFor(S3Error.class).readValue(connection.getErrorStream());
    Assert.assertEquals(objectKey, response.getResource());
    Assert.assertEquals(S3ErrorCode.Name.NO_SUCH_BUCKET, response.getCode());
  }

  @Test
  public void deleteNonexistentDirectory() throws Exception {
    final String bucket = "bucket";
    final String objectKey = bucket + AlluxioURI.SEPARATOR + "folder/";

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());
    deleteRestCall(objectKey);
  }

  @Test
  public void deleteNonEmptyDirectory() throws Exception {
    final String bucket = "bucket";
    final String folderKey = bucket + AlluxioURI.SEPARATOR + "folder";
    final String objectKey = folderKey + AlluxioURI.SEPARATOR + "object";
    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());

    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());

//    The directory can't be deleted because of non-empty directory.
    deleteRestCall(folderKey);
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        headRestCall(objectKey).getResponseCode());
  }

  @Test
  public void headDirectoryAndObject() throws Exception {
    final String bucket = "bucket";
    final String folderKey = bucket + AlluxioURI.SEPARATOR + "folder";
    final String objectKey = folderKey + AlluxioURI.SEPARATOR + "object";
    final byte[] object = CommonUtils.randomAlphaNumString(Constants.KB).getBytes();

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(objectKey).getResponseCode());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(folderKey).getResponseCode());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        headRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createBucketRestCall(bucket).getResponseCode());
    Assert.assertEquals(Response.Status.OK.getStatusCode(),
        createObjectRestCall(objectKey, NO_PARAMS, object, TEST_USER_NAME).getResponseCode());

    Assert.assertArrayEquals(object, getObjectRestCall(objectKey).getBytes());
    Assert.assertEquals(0, headRestCall(folderKey).getContentLength());
  }
}