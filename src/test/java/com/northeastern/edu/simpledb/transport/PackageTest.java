package com.northeastern.edu.simpledb.transport;

import com.northeastern.edu.simpledb.backend.utils.Panic;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class PackageTest {

    @Test
    void testPackager() throws Exception {
        new Thread(() -> {
            try {
                ServerSocket welcomeSocket = new ServerSocket(10888);
                Socket socket = welcomeSocket.accept();
                Transporter t = new Transporter(socket);
                Encoder e = new Encoder();
                Packager p = new Packager(t, e);

                Package one = p.receive();
                assert "pkg1 test".equals(new String(one.getData()));
                Package two = p.receive();
                assert "pkg2 test".equals(new String(two.getData()));

                p.send(new Package("pkg3 test".getBytes(), null));
                welcomeSocket.close();
            } catch (Exception e) {
                Panic.panic(e);
            }
        }).start();

        Socket socket = new Socket(InetAddress.getLocalHost().getHostAddress(), 10888);
        Transporter t = new Transporter(socket);
        Encoder e = new Encoder();
        Packager p = new Packager(t, e);

        p.send(new Package("pkg1 test".getBytes(), null));
        p.send(new Package("pkg2 test".getBytes(), null));

        Package three = p.receive();
        assert "pkg3 test".equals(new String(three.getData()));
    }
}
