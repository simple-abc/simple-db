package com.northeastern.edu.simpledb.client;

import com.northeastern.edu.simpledb.transport.Package;
import com.northeastern.edu.simpledb.transport.Packager;

import java.io.IOException;

public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws IOException {
        packager.close();
    }
}
