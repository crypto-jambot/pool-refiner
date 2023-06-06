package dev.rabbit.util;

import dev.rabbit.exception.RefreshTokenFailure;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.http.HttpService;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Web3 {
    private static final String DOMAIN = "https://rabbit.dev";
    private static final String NODE_ENDPOINT = DOMAIN + "/http";
    private static final HttpService httpService;
    private static final Web3j instance = init();
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.MINUTES)
            .writeTimeout(10, TimeUnit.MINUTES)
            .readTimeout(10, TimeUnit.MINUTES)
            .build();
    private static final String AUTH_ENDPOINT = DOMAIN + "/auth";
    private static final String API_KEY = Secret.getSecret("crypto-351521", "api-secret-key");
    private static Long exp = null;

    static {
        httpService = new HttpService(NODE_ENDPOINT, client);
    }

    public static Web3j getInstance() {
//        if (isExpired()) refreshToken();
        return instance;
    }

    public static HttpService getHttpService() {
        return httpService;
    }

    public static long getBlockByTimestamp(long epochSeconds) throws IOException {
//        if (isExpired()) refreshToken();
        long minBlockNumber = 0;
        long maxBlockNumber = instance.ethBlockNumber().send().getBlockNumber().longValue();

        long midBlockNumber = Math.floorDiv(minBlockNumber + maxBlockNumber, 2);
        EthBlock.Block midBlock = instance.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(midBlockNumber)), true).send().getBlock();

        while (minBlockNumber <= maxBlockNumber) {
            if (midBlock.getTimestamp().longValue() == epochSeconds) {
                break;
            }

            if (midBlock.getTimestamp().longValue() > epochSeconds) {
                maxBlockNumber = midBlockNumber - 1;
            } else {
                minBlockNumber = midBlockNumber + 1;
            }

            midBlockNumber = Math.floorDiv(minBlockNumber + maxBlockNumber, 2);
            midBlock = instance.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(midBlockNumber)), true).send().getBlock();
        }
        return midBlockNumber;
    }

    private static Web3j init() {
//        refreshToken();
        return Web3j.build(httpService);
    }

    private static void refreshToken() {
        final Request request = new Request.Builder()
                .url(AUTH_ENDPOINT)
                .addHeader("api-key", API_KEY)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assert response.body() != null;
            Map<String, Object> responseMap = Mapper.INSTANCE.readJson(response.body().string());
            Map<String, Object> tokenObj = (Map<String, Object>) responseMap.get("token");
            exp = ((Integer) tokenObj.get("exp")).longValue();
            String accessToken = (String) tokenObj.get("token");
            httpService.addHeader("Authorization", String.format("Bearer %s", accessToken));
        } catch (IOException e) {
            throw new RefreshTokenFailure("Failed to refresh an access-token while sending a request to get a JWT.", e);
        }
    }

    private static boolean isExpired() {
        return (exp - Instant.now().getEpochSecond()) / 60 <= 30;
    }
}
