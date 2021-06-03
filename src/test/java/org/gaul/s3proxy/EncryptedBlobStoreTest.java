// CHECKSTYLE:OFF
package org.gaul.s3proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gaul.s3proxy.S3ProxyConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.google.inject.Module;

import org.assertj.core.api.Fail;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class EncryptedBlobStoreTest {
    private static final int BYTE_SOURCE_SIZE = 1024;
    private static final ByteSource BYTE_SOURCE = TestUtils.randomByteSource()
        .slice(0, BYTE_SOURCE_SIZE);
    private static final String peacemakrTestOrgAPIKey = "d1Maw58P2xCQ8d0GV15n22SQNI6lYXHzWLCTEvNPHnY=";
    private BlobStoreContext context;
    private BlobStore blobStore;
    private String containerName;
    private BlobStore encryptedBlobStore;
    private Properties properties;

    @Before
    public void setUp() throws Exception {
        String peacemakrAPIAuthKey =
            Boolean.parseBoolean(System.getProperty("usePeacemakrTestOrg")) ? peacemakrTestOrgAPIKey : "";
        containerName = TestUtils.createRandomContainerName();

        context = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);

        properties = new Properties();
        properties.put(PROPERTY_ENCRYPTED_BLOBSTORE, "true");
        properties.put(PROPERTY_PEACEMAKR_API_KEY, peacemakrAPIAuthKey);
        properties.put(PROPERTY_CLIENT_NAME, "encrypted-blob-store-test");

        newBlobStore();
    }

    @After
    public void tearDown() throws Exception {
        if (context != null) {
            blobStore.deleteContainer(containerName);
            context.close();
        }
    }

    @Test
    public void testCreateBlobGetBlob() throws Exception {
        String blobName = TestUtils.createRandomBlobName();
        Blob blob = makeBlob(encryptedBlobStore, blobName);
        encryptedBlobStore.putBlob(containerName, blob);

        blob = encryptedBlobStore.getBlob(containerName, blobName);
        validateBlobMetadata(blob.getMetadata());

        // content differs, only compare length
        InputStream actual = blob.getPayload().openStream();
        InputStream expected = BYTE_SOURCE.openStream();
        long actualLength = ByteStreams.copy(actual,
            ByteStreams.nullOutputStream());
        long expectedLength = ByteStreams.copy(expected,
            ByteStreams.nullOutputStream());
        assertThat(actualLength).isEqualTo(expectedLength);

        PageSet<? extends StorageMetadata> pageSet = encryptedBlobStore.list(
            containerName);
        assertThat(pageSet).hasSize(1);
        StorageMetadata sm = pageSet.iterator().next();
        assertThat(sm.getName()).isEqualTo(blobName);
    }

    @Test
    public void testCreateBlobBlobMetadata() throws Exception {
        String blobName = TestUtils.createRandomBlobName();
        Blob blob = makeBlob(encryptedBlobStore, blobName);
        encryptedBlobStore.putBlob(containerName, blob);
        BlobMetadata metadata = encryptedBlobStore.blobMetadata(containerName,
            blobName);
        validateBlobMetadata(metadata);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateMultipartBlobGetBlob() throws Exception {
        BlobMetadata blobMetadata = makeBlob(encryptedBlobStore, TestUtils
            .createRandomBlobName()).getMetadata();
        encryptedBlobStore.initiateMultipartUpload(TestUtils
            .createRandomContainerName(), blobMetadata, new PutOptions());
        Fail.failBecauseExceptionWasNotThrown(UnsupportedOperationException
            .class);
    }

    private Blob makeBlob(BlobStore blobStore, String blobName)
            throws IOException {
        return blobStore.blobBuilder(blobName)
                .payload(BYTE_SOURCE)
                .contentDisposition("attachment; filename=secret-recording.mp4")
                .contentEncoding("compress")
                .contentLength(BYTE_SOURCE.size())
                .contentType(MediaType.MP4_AUDIO)
                .contentMD5(BYTE_SOURCE.hash(TestUtils.MD5))
                .userMetadata(ImmutableMap.of("key", "value"))
                .build();
    }

    private void validateBlobMetadata(BlobMetadata metadata)
            throws IOException {
        assertThat(metadata).isNotNull();

        ContentMetadata contentMetadata = metadata.getContentMetadata();
        assertThat(contentMetadata.getContentDisposition())
            .isEqualTo("attachment; filename=secret-recording.mp4");
        assertThat(contentMetadata.getContentEncoding())
            .isEqualTo("compress");
        assertThat(contentMetadata.getContentType())
            .isEqualTo(MediaType.MP4_AUDIO.toString());

        assertThat(metadata.getUserMetadata())
            .isEqualTo(ImmutableMap.of("key", "value"));
    }

    private BlobStore newBlobStore() {
        return encryptedBlobStore = EncryptedBlobStore.newEncryptedBlobStore(blobStore, properties);
    }
}
