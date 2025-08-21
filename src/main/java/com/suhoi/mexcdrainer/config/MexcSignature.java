package com.suhoi.mexcdrainer.config;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class MexcSignature {

    public static Map<String, String> signedParams(String secret, long recvWindow, Map<String, String> params) {
        Map<String, String> allParams = new java.util.HashMap<>(params);
        allParams.put("recvWindow", String.valueOf(recvWindow));
        allParams.put("timestamp", String.valueOf(System.currentTimeMillis()));

        String queryString = canonicalQS(allParams);
        String signature = hmacSha256(queryString, secret);

        allParams.put("signature", signature);
        return allParams;
    }

    public static String canonicalQS(Map<String, String> params) {
        return params.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
    }

    private static String hmacSha256(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate signature", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
}
