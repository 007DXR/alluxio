package alluxio.client.rest;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.CompleteMultipartUploadRequest;
import alluxio.proxy.s3.CompleteMultipartUploadResult;
import alluxio.proxy.s3.InitiateMultipartUploadResult;
import alluxio.proxy.s3.CompleteMultipartUploadRequest.Part;
import alluxio.proxy.s3.S3Error;
import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3RestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import javax.ws.rs.core.Response.Status;
import java.net.HttpURLConnection;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MultipartUploadTest extends RestApiTest {
  private FileSystem mFileSystem;
  private AmazonS3 mS3Client = null;
  private static final int UFS_PORT = 8004;
  final String bucketName = "bucket";
  final String objectName = "object";
  final String fullKey = bucketName + AlluxioURI.SEPARATOR + objectName;
  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withPort(UFS_PORT)
      .withCredentials("_", "_")
      .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeProxy(true)
          .setProperty(PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_MIN_PART_SIZE, "1KB")
          //Each part must be at least 1 KB in size, except the last part
          .setProperty(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL,
              "0s")  //always sync the metadata
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:" + UFS_PORT)
          .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
          .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, "s3://" + TEST_BUCKET)
          .setProperty(PropertyKey.WORKER_HTTP_SERVER_ENABLED, false)
          .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
          .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
          .setNumWorkers(2)
          .build();

  @Before
  public void before() throws Exception {
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

  public String initiateMultipartUpload() throws Exception {
    // Initiate the multipart upload.
    createBucketTestCase(bucketName).checkResponseCode(Status.OK.getStatusCode());
    final InitiateMultipartUploadResult result =
        initiateMultipartUploadTestCase(fullKey).getResponse(InitiateMultipartUploadResult.class);
    final String uploadId = result.getUploadId();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + fullKey + "_" + uploadId);
    final URIStatus mpTempDirStatus = mFileSystem.getStatus(tmpDir);
    final URIStatus mpMetaFileStatus = mFileSystem.getStatus(
        new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)));

    Assert.assertEquals(bucketName, result.getBucket());
    Assert.assertEquals(objectName, result.getKey());
    Assert.assertTrue(mpMetaFileStatus.isCompleted());
    Assert.assertTrue(mpTempDirStatus.isCompleted());
    Assert.assertTrue(mpTempDirStatus.getFileInfo().isFolder());
    return uploadId;
  }

  public void uploadParts(String uploadId, List<String> objects, List<Integer> parts) throws Exception {
    // Upload parts
    for (int partNum : parts) {
      createObjectTestCase(fullKey, objects.get(partNum).getBytes(), uploadId, partNum)
          .checkResponseCode(Status.OK.getStatusCode());
    }
    for (int partNum : parts) {
      getTestCase(fullKey+ "_" + uploadId+AlluxioURI.SEPARATOR+partNum)
          .checkResponseCode(Status.OK.getStatusCode())
          .checkResponse(objects.get(partNum).getBytes());
    }
  }

  public void completeMultipartUpload(String uploadId, List<Part> partList) throws Exception {
    // Complete the multipart upload.
    CompleteMultipartUploadResult completeMultipartUploadResult =
        completeMultipartUploadTestCase(fullKey, uploadId,
            new CompleteMultipartUploadRequest(partList))
            .checkResponseCode(Status.OK.getStatusCode())
            .getResponse(CompleteMultipartUploadResult.class);

    // Verify that the response is expected.
    Assert.assertEquals(bucketName, completeMultipartUploadResult.getBucket());
    Assert.assertEquals(objectName, completeMultipartUploadResult.getKey());
  }

  @Test
  public void uploadPartWithNonExistentUploadId() throws Exception {
    createObjectTestCase(fullKey, EMPTY_CONTENT, "wrong", 1)
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
    initiateMultipartUpload();
    createObjectTestCase(fullKey, EMPTY_CONTENT, "wrong", 1)
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
  }

  @Test
  public void completeAllPartsUpload() throws Exception {
    final int partsNum = 50;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + fullKey + "_" + uploadId);
    for (int i = 0; i < partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);

    uploadParts(uploadId, objects, parts);
    // Verify that all parts are uploaded to the temporary directory.
    Assert.assertEquals(partsNum, mFileSystem.listStatus(tmpDir).size());

    completeMultipartUpload(uploadId, partList);
    // Verify that the temporary directory is deleted.
    Assert.assertFalse(mFileSystem.exists(tmpDir));
    getTestCase(fullKey).checkResponse(String.join("", objects).getBytes());
  }

  @Test
  public void completePartialPartsUpload() throws Exception {
    final int partsNum = 50;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    final AlluxioURI tmpDir = new AlluxioURI(
        AlluxioURI.SEPARATOR + fullKey + "_" + uploadId);
    for (Integer i = 0; i < partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);

    uploadParts(uploadId, objects, parts);
    completeMultipartUpload(uploadId, partList.subList(10,partsNum-10));
    getTestCase(fullKey).checkResponse(String.join("", objects.subList(10,partsNum-10)).getBytes());
  }

  @Test
  public void completeInvalidPartsUpload() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 0; i < partsNum; i++) {
      parts.add(i);
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);
    partList.add(new Part("", -1));

    completeMultipartUploadTestCase(fullKey, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.INVALID_PART);
  }

  @Test
  public void completeInvalidPartsUpload2() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 0; i < partsNum; i++) {
      parts.add(i);
      objects.add(CommonUtils.randomAlphaNumString(Constants.KB));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);
    partList.add(new Part("", 0));
    partList.add(new Part("", 2));

    completeMultipartUploadTestCase(fullKey, uploadId,
        new CompleteMultipartUploadRequest(partList,true))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.INVALID_PART_ORDER);
  }

  @Test
  public void completeTooSmallPartsUpload() throws Exception {
    final int partsNum = 10;
    final List<String> objects = new ArrayList<>();
    final List<Integer> parts = new ArrayList<>();
    final List<Part> partList = new ArrayList<>();
    final String uploadId = initiateMultipartUpload();
    for (int i = 0; i < partsNum; i++) {
      parts.add(i);
      partList.add(new Part("", i));
      objects.add(CommonUtils.randomAlphaNumString(1));
    }
    Collections.shuffle(parts);
    uploadParts(uploadId, objects, parts);

    completeMultipartUploadTestCase(fullKey, uploadId,
        new CompleteMultipartUploadRequest(partList,true))
        .checkResponseCode(Status.BAD_REQUEST.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.ENTITY_TOO_SMALL);
  }

  @Test
  public void completeNonExistentUpload() throws Exception {
    final String uploadId = "wrong";
    final List<Part> partList = new ArrayList<>();

    initiateMultipartUpload();
    completeMultipartUploadTestCase(fullKey, uploadId,
        new CompleteMultipartUploadRequest(partList))
        .checkResponseCode(Status.NOT_FOUND.getStatusCode())
        .checkErrorCode(S3ErrorCode.Name.NO_SUCH_UPLOAD);
  }
}
