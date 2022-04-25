package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class S3ErrorMessage {
    private static final String ErrorTemplate1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                                 "<Error>\n" +
                                                 "  <Code>%s</Code>\n" +
                                                 "</Error>";
    private static final String ErrorTemplate2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                                 "<Error>\n" +
                                                 "  <Code>%s</Code>\n" +
                                                 "  <Resource>%s</Resource> \n" +
                                                 "</Error>";
    private static final String ErrorTemplate3 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                                 "<Error>\n" +
                                                 "  <Code>%s</Code>\n" +
                                                 "  <Message>%s</Message>\n" +
                                                 "</Error>";
    private static final String ErrorTemplate4 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                                 "<Error>\n" +
                                                 "  <Code>%s</Code>\n" +
                                                 "  <Message>%s</Message>\n" +
                                                 "  <Resource>%s</Resource> \n" +
                                                 "</Error>";

    public static String makeErrorXMLResponse(S3ErrorCode code) {
        return String.format(ErrorTemplate1, code);
    }

    public static String makeErrorXMLResponse(S3ErrorCode code, String resource) {
        return String.format(ErrorTemplate2, code, resource);
    }

    public static String makeErrorXMLResponseWithMessage(S3ErrorCode code, String message) {
        return String.format(ErrorTemplate3, code, message);
    }

    public static String makeErrorXMLResponseWithMessage(S3ErrorCode code, String resource, String message) {
        return String.format(ErrorTemplate4, code, message, resource);
    }

    public static void makeS3ErrorResponse(HttpServletResponse resp, S3ErrorCode code, String resource, String message) throws IOException {
        var body = S3ErrorMessage.makeErrorXMLResponseWithMessage(code, resource, message).getBytes(StandardCharsets.UTF_8);
        resp.setStatus(code.httpCode());
        resp.setHeader("Content-Length", String.valueOf(body.length));
        resp.getOutputStream().write(body);
        resp.flushBuffer();
    }

    public static void handleIOException(HttpServletResponse resp, AsyncContext ac) {
        resp.setStatus(S3ErrorCode.InternalError.httpCode());
        if (ac != null) {
            ac.complete();
        }
    }

    public static void handleIOException(HttpServletResponse resp) {
        resp.setStatus(S3ErrorCode.InternalError.httpCode());
    }
}
