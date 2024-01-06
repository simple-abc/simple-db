package com.northeastern.edu.simpledb.backend.server;

import com.northeastern.edu.simpledb.backend.tbm.TableManager;
import com.northeastern.edu.simpledb.transport.Encoder;
import com.northeastern.edu.simpledb.transport.Package;
import com.northeastern.edu.simpledb.transport.Packager;
import com.northeastern.edu.simpledb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket welcomeSocket = null;

        try {
            welcomeSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return ;
        }

        try {
            while (true) {
                Socket socket = welcomeSocket.accept();

                System.out.println("Server listen to port: " + port);
                ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

                Runnable worker = new HandleSocket(socket, tbm);
                tpe.submit(worker);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                welcomeSocket.close();
            } catch (IOException e) {}
        }

    }

}
class HandleSocket implements Runnable {

    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        Executor executor = new Executor(tbm);
        while (true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch (Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = executor.executeTx(sql);
            } catch (Exception ex) {
                e = ex;
                e.printStackTrace();
            }

            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (IOException ex) {
                ex.printStackTrace();
                break;
            }
        }

        executor.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
