package org.gaul.s3proxy;

import com.google.common.hash.HashCode;
import io.peacemakr.crypto.Factory;
import io.peacemakr.crypto.ICrypto;
import io.peacemakr.crypto.exception.CoreCryptoException;
import io.peacemakr.crypto.exception.PeacemakrException;
import io.peacemakr.crypto.impl.persister.InMemoryPersister;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.Payload;

import org.apache.log4j.BasicConfigurator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public final class EncryptedBlobStore extends ForwardingBlobStore {
    private final ICrypto peacemakrSDK;
    // Could be null if the property is not specified
    private final String useDomain;
    private EncryptedBlobStore(BlobStore blobStore, Properties properties) {
        super(blobStore);

        BasicConfigurator.configure();

        String peacemakrApiKey = properties.getProperty(S3ProxyConstants.PROPERTY_PEACEMAKR_API_KEY);
        String clientName = properties.getProperty(S3ProxyConstants.PROPERTY_CLIENT_NAME);
        useDomain = properties.getProperty(S3ProxyConstants.PROPERTY_PEACEMAKR_USE_DOMAIN);
        if (clientName.isEmpty()) {
            clientName = "s3proxy" + properties.getProperty(S3ProxyConstants.PROPERTY_IDENTITY);
        }

        try {
            peacemakrSDK = Factory.getCryptoSDK(peacemakrApiKey, clientName, null, new InMemoryPersister(), null);
            peacemakrSDK.register();
        } catch (PeacemakrException e) {
            throw new RuntimeException(e);
        }
    }

    static BlobStore newEncryptedBlobStore(BlobStore blobStore,
                                           Properties properties) {
        return new EncryptedBlobStore(blobStore, properties);
    }

    private byte[] getByteArrayFromStream(Blob blob) throws IOException {
        InputStream blobStream = blob.getPayload().openStream();
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        byte[] buffer = new byte[1024];
        int len;

        // read bytes from the input stream and store them in buffer
        while ((len = blobStream.read(buffer)) != -1) {
            // write bytes from the buffer into output stream
            os.write(buffer, 0, len);
        }

        return os.toByteArray();
    }

    private Blob rebuildWithNewPayload(String container, Blob blob, byte[] newPayload) {
        BlobMetadata blobMeta = blob.getMetadata();
        ContentMetadata contentMeta = blob.getMetadata().getContentMetadata();
        Blob newBlob = blobBuilder(container)
                .name(blobMeta.getName())
                .type(blobMeta.getType())
                .tier(blobMeta.getTier())
                .userMetadata(blobMeta.getUserMetadata())
                .payload(newPayload)
                .cacheControl(contentMeta.getCacheControl())
                .contentDisposition(contentMeta.getContentDisposition())
                .contentEncoding(contentMeta.getContentEncoding())
                .contentLanguage(contentMeta.getContentLanguage())
                .contentLength(newPayload.length)
                .contentType(contentMeta.getContentType())
                .contentMD5((HashCode) null)
                .build();

        newBlob.getMetadata().setUri(blobMeta.getUri());
        newBlob.getMetadata().setETag(blobMeta.getETag());
        newBlob.getMetadata().setLastModified(blobMeta.getLastModified());
        newBlob.getMetadata().setSize(blobMeta.getSize());
        newBlob.getMetadata().setPublicUri(blobMeta.getPublicUri());
        newBlob.getMetadata().setContainer(blobMeta.getContainer());

        return newBlob;
    }

    private Blob encryptBlob(String container, Blob blob) {
        try {
            byte[] payload = getByteArrayFromStream(blob);
            byte[] encrypted = null;
            if (useDomain != null && !useDomain.isEmpty()) {
                encrypted = peacemakrSDK.encryptInDomain(payload, useDomain);
            } else {
                encrypted = peacemakrSDK.encrypt(payload);
            }

            if (encrypted == null) {
                throw new CoreCryptoException("unable to encrypt data in use domain: " + useDomain);
            }

            // Now re-build the blob and return it
            return rebuildWithNewPayload(container, blob, encrypted);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Blob decryptBlob(String container, Blob blob) {
        try {
            byte[] payload = getByteArrayFromStream(blob);
            byte[] decrypted = peacemakrSDK.decrypt(payload);

            return rebuildWithNewPayload(container, blob, decrypted);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return decryptBlob(containerName, delegate().getBlob(containerName,
                blobName));
    }

    @Override
    public Blob getBlob(String containerName, String blobName,
                        GetOptions getOptions) {
        return decryptBlob(containerName, delegate().getBlob(containerName,
                blobName, getOptions));
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return delegate().putBlob(containerName, encryptBlob(containerName,
                blob));
    }

    @Override
    public String putBlob(String containerName, Blob blob,
                          PutOptions putOptions) {
        return delegate().putBlob(containerName, encryptBlob(containerName,
                blob), putOptions);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container,
                                                   BlobMetadata blobMetadata,
                                                   PutOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu,
                                          List<MultipartPart> parts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu,
                                             int partNumber, Payload payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        throw new UnsupportedOperationException();
    }
}
