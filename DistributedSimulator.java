import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DistributedSimulator {

    public static class Monitor extends Thread {
        private final int port = 6000;

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Monitor] Escutando na porta " + port + " para receber mensagens...");
                while (true) {
                    Socket sender = serverSocket.accept();
                    new Thread(() -> {
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(sender.getInputStream()))) {
                            String msg;
                            while ((msg = in.readLine()) != null) {
                                System.out.println("[Monitor] " + msg);
                            }
                        } catch (IOException e) {
                            System.err.println("[Monitor] Erro: " + e.getMessage());
                        }
                    }).start();
                }
            } catch (IOException e) {
                System.err.println("[Monitor] Erro: " + e.getMessage());
            }
        }
    }

    public static class Node extends Thread {
        private final int id;
        private final int myPort;
        private final int[] ports;
        private final int totalNodes;

        private volatile int logicalClock = 0;
        private volatile boolean requesting = false;
        private volatile boolean inCriticalSection = false;
        private int nodeTimestamp = 0;
        private final Set<Integer> receivedOks = ConcurrentHashMap.newKeySet();
        private final Queue<Integer> waitingQueue = new ConcurrentLinkedQueue<>();

        public Node(int id, int totalNodes) {
            this.id = id;
            this.totalNodes = totalNodes;
            this.myPort = 5000 + id;
            this.ports = new int[totalNodes];
            for (int i = 0; i < totalNodes; i++) {
                ports[i] = 5000 + i;
            }
        }

        @Override
        public void run() {
            new Thread(this::receiver).start();

            Random rand = new Random(System.currentTimeMillis() + id);

            while (true) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}

                double chance = rand.nextDouble();
                if (chance <= 0.5) {
                    sendToMonitor(String.format("[Nó %d] Não quer acessar o recurso.", id));
                    continue;
                }

                requestAccess();
                waitApproval();

                enterCriticalSection(rand);
                freeResources();
            }
        }

        private void receiver() {
            try (ServerSocket serverSocket = new ServerSocket(myPort)) {
                sendToMonitor(String.format("[Receiver %d] Escutando na porta %d...", id, myPort));
                while (true) {
                    Socket sender = serverSocket.accept();
                    new Thread(() -> processMessage(sender)).start();
                }
            } catch (IOException e) {
                sendToMonitor("[Receiver] Erro: " + e.getMessage());
            }
        }

        private void processMessage(Socket sender) {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(sender.getInputStream()));
                sender
            ) {
                String message = in.readLine();
                if (message == null) return;

                synchronized (this) {
                    if (message.startsWith("REQ")) {
                        String[] parts = message.split(" ");
                        int ts = Integer.parseInt(parts[1]);
                        int shipper = Integer.parseInt(parts[2]);

                        logicalClock = Math.max(logicalClock, ts);

                        boolean shipperHigherPriority = (ts < nodeTimestamp) || (ts == nodeTimestamp && shipper < id);

                        if (inCriticalSection || (requesting && !shipperHigherPriority)) {
                            waitingQueue.add(shipper);
                        } else {
                            sendMessage("OK " + id, ports[shipper]);
                        }

                    } else if (message.startsWith("OK")) {
                        String[] parts = message.split(" ");
                        int shipper = Integer.parseInt(parts[1]);
                        receivedOks.add(shipper);
                    }
                }
            } catch (IOException e) {
                sendToMonitor("[Servidor] Erro ao processar mensagem: " + e.getMessage());
            }
        }

        private synchronized void requestAccess() {
            receivedOks.clear();
            logicalClock++;
            nodeTimestamp = logicalClock;
            requesting = true;

            String message = "REQ " + nodeTimestamp + " " + id;
            for (int i = 0; i < totalNodes; i++) {
                if (i != id) {
                    sendMessage(message, ports[i]);
                }
            }
        }

        private void waitApproval() {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {}

                if (receivedOks.size() >= totalNodes - 1) break;
            }
        }

        private synchronized void enterCriticalSection(Random rand) {
            inCriticalSection = true;
            requesting = false;

            int k = rand.nextInt(10) + 1;
            sendToMonitor(String.format("[Nó %d] --- ENTRANDO na SC --- \n[Nó %d] --- Quantidade de números a serem printados: %d ---", id, id, k));
            for (int i = 0; i < k; i++) {
                sendToMonitor(String.format("[Nó %d] Valor %d", id, nodeTimestamp + i));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {}
            }
            sendToMonitor(String.format("[Nó %d] ___ SAINDO da SC ___", id));
            inCriticalSection = false;
        }

        private synchronized void freeResources() {
            while (!waitingQueue.isEmpty()) {
                int destination = waitingQueue.poll();
                sendMessage("OK " + id, ports[destination]);
            }
        }

        private void sendMessage(String message, int port) {
            try (Socket socket = new Socket("localhost", port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println(message);
            } catch (IOException e) {
                sendToMonitor(String.format("[Nó %d] Erro ao enviar para %d: %s", id, port, e.getMessage()));
            }
        }

        private void sendToMonitor(String message) {
            try (Socket socket = new Socket("localhost", 6000);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println(message);
            } catch (IOException ignored) {}
        }
    }

    public static void main(String[] args) {
        int totalNodes = 6;

        if(args.length >= 1) {
            try {
                totalNodes = Integer.parseInt(args[0]); 

                if(totalNodes <= 0) {
                    throw new IllegalArgumentException("O número de nós precisa ser maior ou igual a um!");
                } 
            } catch(NumberFormatException e) {
                System.out.println("Uso: java DistributedSimulator <total_de_nós>");
                return;
            }
        }

		    new Monitor().start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}

        for (int i = 0; i < totalNodes; i++) {
            new Node(i, totalNodes).start();
        }
    }
}
