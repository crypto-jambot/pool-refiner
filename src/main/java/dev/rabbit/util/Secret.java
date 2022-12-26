package dev.rabbit.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import dev.rabbit.exception.RetrieveSecretFailure;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Secret {
    public static String getSecret(String projectId, String secretName, String secretVersion) {
        final String ERR_PREFIX = "Failed to retrieve the secret: " + secretName;
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretName, secretVersion);
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            byte[] data = response.getPayload().getData().toByteArray();
            Checksum checksum = new CRC32C();
            checksum.update(data, 0, data.length);
            if (response.getPayload().getDataCrc32C() != checksum.getValue()) {
                throw new RetrieveSecretFailure(ERR_PREFIX + " due to corrupt data detected.");
            }
            return response.getPayload().getData().toStringUtf8();
        } catch (IOException e) {
            throw new RetrieveSecretFailure(ERR_PREFIX, e);
        }
    }
}
