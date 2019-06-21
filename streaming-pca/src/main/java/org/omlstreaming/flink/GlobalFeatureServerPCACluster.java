package org.omlstreaming.flink;

import org.ejml.data.DMatrixRMaj;
import org.ejml.ops.MatrixIO;
import org.ejml.simple.SimpleMatrix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;

import static java.lang.System.exit;

public class GlobalFeatureServerPCACluster implements Runnable {

    private Socket socketClient = null;

    private final int evtBurstSize = 1000; // events
    private final int evtBurstLatency = 50; // ms

    private static SimpleMatrix X;

    public GlobalFeatureServerPCACluster(Socket socket, String dataSet) {
        socketClient = socket;
        try {
            DMatrixRMaj inputDataRead = MatrixIO
                    .loadCSV(dataSet, true);
            X = SimpleMatrix.wrap(inputDataRead);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        String inputDatasetFile = args[0];
        int commPort = Integer.valueOf(args[1]);
        ServerSocket listener = new ServerSocket(commPort);

        try {
            while (true) {
                Socket socket = listener.accept();
                GlobalFeatureServerPCACluster hand = new GlobalFeatureServerPCACluster(socket, inputDatasetFile);

                (new Thread(hand)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                listener.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {

        int counter = 0;
        try {
            PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);
            long timestamp = 1460730000050L;
            double it = 0.12;
            int sampleSize = 10000;
            int initialSampleSize = 5000;
            String message;
            while (true) {
                String s = "-r 2000000";
                if (s.contains("-r")) {
                    counter = Integer.parseInt(s.split(" ")[1]);
                    for (int id = 0; id < counter; id++) {
                        for (int didx = initialSampleSize; didx < sampleSize; didx++) {
                            SimpleMatrix curData = X.extractVector(true, didx);
                            message = timestamp + "," + String.valueOf(curData.get(0));
                            for (int dId = 1; dId < curData.getNumElements(); dId++) {
                                message += "," + String.valueOf(curData.get(dId));
                            }
                            out.println(message);
                        }
                        if (id % evtBurstSize == 0) {
                            try {
                                Thread.sleep(evtBurstLatency);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    socketClient.close();
                    System.exit(0);
                }
                it = it + 1;
                if (out.checkError())
                    exit(-1);
                timestamp += 7200000;
            }
        } catch (IOException e) {
            e.printStackTrace();
            if (socketClient.isClosed())
                exit(-2);
        } finally {
            try {
                socketClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
