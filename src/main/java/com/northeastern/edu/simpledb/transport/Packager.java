package com.northeastern.edu.simpledb.transport;

import org.apache.commons.codec.DecoderException;

import java.io.IOException;

public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws IOException {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    public Package receive() throws Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    public void close() throws IOException {
        transporter.close();
    }

}
