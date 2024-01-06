package com.northeastern.edu.simpledb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

public class Transporter {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public byte[] receive() throws IOException, DecoderException {
        String line = reader.readLine();
        if (line == null) close();
        return hexDecode(line);
    }

    public void send(byte[] data) throws IOException {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    private String hexEncode(byte[] data) {
        return Hex.encodeHexString(data, true) + "\n";
    }

    private byte[] hexDecode(String line) throws DecoderException {
        return Hex.decodeHex(line);
    }

    public void close() throws IOException {
        this.writer.close();
        this.reader.close();
        this.socket.close();
    }


}
