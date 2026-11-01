package Client;
import java.io.*;
import java.net.Socket;

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import Server.AuctionMessage; 

public class Bidder {
    private String username;
    private String token_id;
    private String sharedDirectoryPath;
    
    private static final String SERVER_IP = "127.0.0.1";
    private static final int SERVER_PORT = 5000; 
    private int myListenPort; 
    
    private Socket serverSocketConnection;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public void executePhaseOne(String user, String pass) {
        this.username = user;
        this.sharedDirectoryPath = "shared_directory_" + this.username;
        this.myListenPort = 6000 + (int)(Math.random() * 3000); 

        System.out.println("--- INITIATING PHASE 1: BOOTSTRAP & AUTH ---");

        File dir = new File(sharedDirectoryPath);
        if (!dir.exists()) {
            dir.mkdirs();
            System.out.println("[Step 1] Created local directory: " + sharedDirectoryPath);
        } else {
            System.out.println("[Step 1] Local directory verified: " + sharedDirectoryPath);
        }

        try {
            serverSocketConnection = new Socket(SERVER_IP, SERVER_PORT);
            out = new ObjectOutputStream(serverSocketConnection.getOutputStream());
            in = new ObjectInputStream(serverSocketConnection.getInputStream());

            while (this.token_id == null) {
                System.out.println("\n[Step 2] Attempting Registration as '" + this.username + "'...");
                AuctionMessage regMsg = new AuctionMessage("REGISTER", this.username + " " + pass);
                out.writeObject(regMsg);
                out.flush();

                AuctionMessage regResponse = (AuctionMessage) in.readObject();
                if (regResponse.getPayload().equals("FAILURE")) {
                    System.out.println("Registration failed: Username already taken.");
                    this.username = user + "_" + (int)(Math.random() * 1000);
                    System.out.println("Retrying autonomously with new username: " + this.username);
                    continue; 
                }
                System.out.println("Registration Successful!");

                System.out.println("\n[Step 3] Attempting Login...");
                AuctionMessage loginMsg = new AuctionMessage("LOGIN", this.username + " " + pass);
                out.writeObject(loginMsg);
                out.flush();

                AuctionMessage loginResponse = (AuctionMessage) in.readObject();
                String loginReply = (String) loginResponse.getPayload();
                
                if (loginReply.startsWith("SUCCESS")) {
                    this.token_id = loginReply.split(" ")[1];
                    System.out.println("Login Successful! Received token: " + this.token_id);
                } else {
                    System.out.println("CRITICAL: Login failed due to bad password.");
                    return; 
                }
            }

            System.out.println("\n[Step 4] Declaring inventory to server...");
            File[] listOfFiles = dir.listFiles();
            StringBuilder itemsList = new StringBuilder();
            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.getName().endsWith(".txt")) {
                        itemsList.append(file.getName()).append(",");
                    }
                }
            }
            
            String inventory = itemsList.length() > 0 ? itemsList.substring(0, itemsList.length() - 1) : "EMPTY";
            String myIp = serverSocketConnection.getLocalAddress().getHostAddress();
            String payload = this.token_id + " " + myIp + " " + myListenPort + " " + inventory;
            
            AuctionMessage reqMsg = new AuctionMessage("REQUEST_AUCTION", payload);
            out.writeObject(reqMsg);
            out.flush();
            
            AuctionMessage reqResponse = (AuctionMessage) in.readObject();
            System.out.println("Inventory Declaration: " + reqResponse.getPayload());

            System.out.println("\n--- PHASE 1 COMPLETE ---");
            
            startItemGeneratorThread(); 
            startListeningThread();
            startAuctionMonitorThread();

        } catch (Exception e) {
            System.err.println("Phase 1 Sequence Failed: " + e.getMessage());
        }
    }

    //in case of shut down
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Shutdown Hook] Intercepted shutdown signal. Executing emergency logout...");
            if (token_id != null && out != null) {
                try {
                    AuctionMessage logoutMsg = new AuctionMessage("LOGOUT", token_id);
                    out.writeObject(logoutMsg);
                    out.flush();
                    System.out.println("[Shutdown Hook] LOGOUT message sent to server.");
                    Thread.sleep(500);
                    if (serverSocketConnection != null && !serverSocketConnection.isClosed()) {
                        serverSocketConnection.close();
                    }
                } catch (Exception e) {
                    System.err.println("Error during graceful shutdown: " + e.getMessage());
                }
            }
        }));
    }

    public void sendNewItemMessage(String filename) {
        if (out != null && token_id != null) {
            try {
                String payload = this.token_id + " " + filename;
                AuctionMessage newItemMsg = new AuctionMessage("NEW_ITEM", payload);
                synchronized(out) {
                    out.writeObject(newItemMsg);
                    out.flush();
                }
            } catch (Exception e) {
                System.err.println("Failed to send NEW_ITEM message: " + e.getMessage());
            }
        }
    }

    //udp go-back-n receiver
    private void downloadFileP2P(String sellerAddr, String fileName) {
        try {
            String[] addressParts = sellerAddr.split(":");
            String ip = addressParts[0];
            int port = Integer.parseInt(addressParts[1]);

            System.out.println("[P2P Client] Starting UDP GBN download from " + sellerAddr);
            
            try (DatagramSocket p2pSocket = new DatagramSocket()) {
                //send initial request with the filename
                byte[] reqData = fileName.getBytes();
                DatagramPacket reqPacket = new DatagramPacket(reqData, reqData.length, InetAddress.getByName(ip), port);
                p2pSocket.send(reqPacket);

                int expectedSeqNum = 0;
                ByteArrayOutputStream fileBytes = new ByteArrayOutputStream();
                //5 sec timeout in case seller crashes
                p2pSocket.setSoTimeout(5000);

                //loop to receive packets
                while (true) {
                    //4 bytes for int SeqNum + 64 bytes max payload
                    byte[] rcvBuf = new byte[68];
                    DatagramPacket rcvPacket = new DatagramPacket(rcvBuf, rcvBuf.length);
                    p2pSocket.receive(rcvPacket);
                    
                    ByteBuffer bb = ByteBuffer.wrap(rcvPacket.getData(), 0, rcvPacket.getLength());
                    int seqNum = bb.getInt();
                    
                    // EOF check
                    if (seqNum == -1) {
                        System.out.println("[P2P Client] Received EOF. File transfer complete.");
                        break; 
                    }

                    //network loss simulation 20%
                    if (Math.random() < 0.20) {
                        System.out.println("   [Receiver] SIMULATED DROP of incoming packet " + seqNum);
                        // Ignore this packet
                        continue;
                    }

                    //if it's the packet we are waiting for save it
                    if (seqNum == expectedSeqNum) {
                        fileBytes.write(rcvPacket.getData(), 4, rcvPacket.getLength() - 4);
                        expectedSeqNum++;
                    }

                    //send ACK for the last successfully received packet in order
                    int ackToSend = expectedSeqNum - 1;

                    //simulate network loss 20% outgoing ACK drop
                    if (Math.random() < 0.20) {
                        System.out.println("   [Receiver] SIMULATED DROP of outgoing ACK " + ackToSend);
                        //don't actually send the ACK
                        continue;
                    }

                    byte[] ackData = ByteBuffer.allocate(4).putInt(ackToSend).array();
                    DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, rcvPacket.getAddress(), rcvPacket.getPort());
                    p2pSocket.send(ackPacket);
                }

                //save the completed file
                String finalName = fileName.startsWith("WON_") ? fileName : "WON_" + fileName;
                File downloadedFile = new File(sharedDirectoryPath + File.separator + finalName);
                
                try (FileOutputStream fos = new FileOutputStream(downloadedFile)) {
                    fileBytes.writeTo(fos);
                }
                
                System.out.println("[P2P Client] Saved as: " + downloadedFile.getName());
                
                //notify Server of success and new inventory
                sendNewItemMessage(finalName);
                
                AuctionMessage successMsg = new AuctionMessage("TRANSACTION_SUCCESS", this.token_id + " " + fileName);
                synchronized(out) {
                    out.writeObject(successMsg);
                    out.flush();
                }
                System.out.println("[P2P Client] Notified server of successful transaction.");

            }
        } catch (SocketTimeoutException ste) {
            System.err.println("[P2P Client] Transfer timed out! Alerting server for fallback.");
            try {
                AuctionMessage failMsg = new AuctionMessage("TRANSACTION_FAIL", this.token_id + " " + fileName);
                synchronized(out) { out.writeObject(failMsg); out.flush(); }
            } catch (Exception ex) {}
        } catch (Exception e) {
            System.err.println("[P2P Client] Download failed: " + e.getMessage());
            try {
                AuctionMessage failMsg = new AuctionMessage("TRANSACTION_FAIL", this.token_id + " " + fileName);
                synchronized(out) { out.writeObject(failMsg); out.flush(); }
            } catch (Exception ex) {}
        }
    }

    private void startItemGeneratorThread() {
        ItemGeneratorThread generator = new ItemGeneratorThread(this.sharedDirectoryPath, this);
        generator.start();
    }

    //udp go-back-n sender
    private void startListeningThread() {
        Thread p2pServer = new Thread(() -> {
            try (DatagramSocket p2pSocket = new DatagramSocket(myListenPort)) {
                System.out.println("[P2P Server] UDP Go-Back-N Listener active on port " + myListenPort);
                
                while (true) {
                    try {
                        //wait for a buyer to request a file
                        byte[] reqBuf = new byte[1024];
                        DatagramPacket reqPacket = new DatagramPacket(reqBuf, reqBuf.length);
                        p2pSocket.receive(reqPacket); 
                        
                        String requestedFile = new String(reqPacket.getData(), 0, reqPacket.getLength()).trim();
                        File fileToSend = new File(sharedDirectoryPath + File.separator + requestedFile);
                        
                        if (!fileToSend.exists()) {
                            System.out.println("[P2P Server] File " + requestedFile + " not found!");
                            continue;
                        }

                        System.out.println("[P2P Server] Starting GBN transfer for " + requestedFile);

                        //read file and split into 64-byte chunks
                        FileInputStream fis = new FileInputStream(fileToSend);
                        ArrayList<byte[]> chunks = new ArrayList<>();
                        byte[] buffer = new byte[64];
                        int bytesRead;
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            chunks.add(Arrays.copyOf(buffer, bytesRead));
                        }
                        fis.close();

                        //go-back-n variables
                        int base = 0;
                        int nextSeqNum = 0;
                        int windowSize = 3;
                        int totalPackets = chunks.size();
                        long lastAckTime = System.currentTimeMillis();

                        p2pSocket.setSoTimeout(10); // 10ms timeout to constantly check for ACKs without freezing

                        //the sliding window loop
                        while (base < totalPackets) {
                            //send packets up to the window size
                            while (nextSeqNum < base + windowSize && nextSeqNum < totalPackets) {
                                byte[] chunk = chunks.get(nextSeqNum);
                                //packet format: [4 bytes Sequence Number] + [Up to 64 bytes Data]
                                ByteBuffer bb = ByteBuffer.allocate(4 + chunk.length);
                                bb.putInt(nextSeqNum);
                                bb.put(chunk);
                                byte[] sendData = bb.array();
                                
                                DatagramPacket p = new DatagramPacket(sendData, sendData.length, reqPacket.getAddress(), reqPacket.getPort());
                                p2pSocket.send(p);
                                System.out.println("   [Sender] Sent packet " + nextSeqNum);
                                nextSeqNum++;
                            }

                            //try to receive an ACK
                            try {
                                byte[] ackBuf = new byte[4];
                                DatagramPacket ackPacket = new DatagramPacket(ackBuf, ackBuf.length);
                                p2pSocket.receive(ackPacket);
                                
                                int ackNum = ByteBuffer.wrap(ackPacket.getData()).getInt();
                                System.out.println("   [Sender] Received ACK for " + ackNum);
                                
                                //cumulative ACK: Slide the window forward
                                if (ackNum >= base) {
                                    base = ackNum + 1;
                                    lastAckTime = System.currentTimeMillis(); 
                                }
                            } catch (SocketTimeoutException e) {
                                //check if 2 seconds have passed since the last valid ACK
                                if (System.currentTimeMillis() - lastAckTime > 2000) {
                                    System.out.println("   [Sender] TIMEOUT (2s)! Resending window starting from " + base);
                                    //reset nextSeqNum to resend the unacknowledged packets
                                    nextSeqNum = base;
                                    lastAckTime = System.currentTimeMillis();
                                }
                            }
                        }

                        //send EOF packet (sequence number -1)
                        byte[] eofData = ByteBuffer.allocate(4).putInt(-1).array();
                        DatagramPacket eofPacket = new DatagramPacket(eofData, eofData.length, reqPacket.getAddress(), reqPacket.getPort());
                        //spam last packet in case it gets dropped
                        for (int i = 0; i < 5; i++) {
                            p2pSocket.send(eofPacket);
                        }
                        
                        //clean up
                        //reset timeout for the next buyer
                        p2pSocket.setSoTimeout(0);
                        System.out.println("[P2P Server] Transfer complete! Deleting " + requestedFile);
                        fileToSend.delete();

                    } catch (Exception e) {
                        System.err.println("Error during P2P Transfer: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("UDP Listener Error: " + e.getMessage());
            }
        });
        p2pServer.setDaemon(true);
        p2pServer.start();
    }

    private void startAuctionMonitorThread() {
        System.out.println("[Thread Started] Auction Monitor is running.");
        Thread monitorThread = new Thread(() -> {
            try {
                while (true) {
                    //ask what's on the market first
                    if (out != null && token_id != null) {
                        AuctionMessage statusReq = new AuctionMessage("GET_CURRENT_AUCTION", token_id);
                        synchronized(out) {
                            out.writeObject(statusReq);
                            out.flush();
                        }
                    }
                    
                    //sleep for just 10 seconds so we don't miss short auctions!
                    Thread.sleep(10000); 
                }
            } catch (InterruptedException e) {
                System.out.println("[Monitor] Thread interrupted.");
            } catch (Exception e) {
                System.err.println("[Monitor] Network error: " + e.getMessage());
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void startMessageReceiverThread() {
        Thread receiver = new Thread(() -> {
            System.out.println("[Receiver] Standing by for server updates...");
            try {
                while (true) {
                    AuctionMessage incomingMsg = (AuctionMessage) in.readObject();
                    handleServerMessage(incomingMsg);
                }
            } catch (Exception e) {
                System.out.println("[Receiver] Stopped. (Connection closed or logout)");
            }
        });
        receiver.setDaemon(true);
        receiver.start();
    }

    private void handleServerMessage(AuctionMessage msg) {
        String command = msg.getCommand();
        String payload = (String) msg.getPayload();

        switch (command) {
            case "AUCTION_INFO":
                System.out.println("\n[MARKET] Market Info:\n" + payload);
                //split the payload into separate lines
                String[] lines = payload.split("\n");
                
                //loop through every auction in the message
                for (String line : lines) {
                    if (line.contains("Current Bid:")) {
                        try {
                            //parse from the 'line'
                            String itemId = line.split("Item: ")[1].split(" \\|")[0].trim();
                            String sellerPart = line.split("SellerToken: ")[1].split(" \\|")[0].trim();

                            if (sellerPart.equals(this.token_id)) {
                                System.out.println("   -> [STOP] This is my own item (" + itemId + "). Ignoring.");
                                //use continue instead of break to keep reading the next lines
                                continue;
                            }

                            if (Math.random() <= 0.60) {
                                System.out.println("   -> [COIN FLIP] INTERESTED in " + itemId + "!");
                                
                                String pricePart = line.split("Current Bid: ")[1].split(" \\|")[0];
                                int currentPrice = Integer.parseInt(pricePart.trim());
                                
                                String timePart = line.split("Time Left: ")[1].replace("s", "").trim();
                                int timeLeft = Integer.parseInt(timePart);
                                
                                if (currentPrice == 0) {
                                    currentPrice = 10;
                                }
                                
                                double randFactor = Math.random() / 10.0; 
                                
                                if (timeLeft <= 6) {
                                    randFactor = Math.random() / 5.0; 
                                    System.out.println("   -> [SNIPE] " + itemId + " ending soon! Increasing bid aggression to max 20%.");
                                }
                                
                                int myNewBid = (int) Math.ceil(currentPrice * (1.0 + randFactor));
                                
                                if (myNewBid <= currentPrice) {
                                    myNewBid = currentPrice + 1;
                                }

                                System.out.println("   -> [BID] Placing bid of " + myNewBid + " on " + itemId);
                                AuctionMessage bidMsg = new AuctionMessage("PLACE_BID", this.token_id + " " + itemId + " " + myNewBid);
                                synchronized(out) {
                                    out.writeObject(bidMsg);
                                    out.flush();
                                }
                            } else {
                                System.out.println("   -> [COIN FLIP] NOT interested in " + itemId + ".");
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing auction info line: " + e.getMessage());
                        }
                    }
                }
                break;

            case "AUCTION_WON":
                String[] winData = payload.split(" ");
                String sellerAddr = winData[0];
                String fileName = winData[1]; 
                
                System.out.println("\n[MARKET] I won the auction for " + fileName + "!");

                if (Math.random() <= 0.30) {
                    System.out.println("   -> [DECISION] I am CANCELLING my purchase! (30% chance triggered)");
                    try {
                        AuctionMessage cancelMsg = new AuctionMessage("TRANSACTION_FAIL", this.token_id + " " + fileName);
                        synchronized(out) {
                            out.writeObject(cancelMsg);
                            out.flush();
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to send cancellation.");
                    }
                } else {
                    System.out.println("   -> [DECISION] Proceeding with transaction. Connecting to " + sellerAddr);
                    downloadFileP2P(sellerAddr, fileName);
                }
                break;
                
            case "BID_CONFIRMATION":
                System.out.println("[SUCCESS] Confirmed " + payload);
                break;
                
            case "BID_ERROR":
                System.out.println("[REJECTED] Bid Rejected: " + payload);
                break;

            default:
                System.out.println("\n[Server] " + command + ": " + payload);
                break;
        }
    }
    
    public static void main(String[] args) {
        Bidder peer = new Bidder();
        
        String username = args.length > 0 ? args[0] : "TestUser";
        String password = args.length > 1 ? args[1] : "password123";
        
        peer.executePhaseOne(username, password);
        peer.startMessageReceiverThread();
        peer.setupShutdownHook();
    }
}