package org.jgroups.protocols.aws.s3;

import java.io.IOException;
import java.net.HttpURLConnection;

public class Response {
    public HttpURLConnection connection;

    public Response(HttpURLConnection connection) throws IOException {
        this.connection=connection;
    }
}
