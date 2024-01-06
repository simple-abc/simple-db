package com.northeastern.edu.simpledb.client;

import com.northeastern.edu.simpledb.transport.Encoder;
import com.northeastern.edu.simpledb.transport.Packager;
import com.northeastern.edu.simpledb.transport.Transporter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket(InetAddress.getLocalHost().getHostAddress(), 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
