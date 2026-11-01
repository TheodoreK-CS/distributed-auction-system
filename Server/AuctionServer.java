package Server;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AuctionServer {
    private static final int PORT = 5000;

    public static ConcurrentHashMap<String, java.io.ObjectOutputStream> clientStreams = new ConcurrentHashMap<>();

    //keep tabs on everyone
    public static ConcurrentHashMap<String, UserProfile> registeredUsers = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> activeSessions = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> connectionDetails = new ConcurrentHashMap<>();
    public static ConcurrentLinkedDeque<AuctionItem> auctionQueue = new ConcurrentLinkedDeque<>();

    //map to hold up to 2 active auctions, keyed by the objectId
    public static ConcurrentHashMap<String, AuctionState> activeAuctions = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, AuctionState> pendingTransactions = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Auction Server starting on port " + PORT + "...");
        new Thread(new AuctionEngine()).start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            //accept connections and send them to hendler
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

//user profile class
class UserProfile {
    String password;
    int num_auctions_seller = 0;
    int num_auctions_bidder = 0;

    //reputation score starts at 1
    double reputation_score = 1.0;

    public UserProfile(String password) {
        this.password = password;
    }
}

//auction item class
class AuctionItem {
    String sellerToken;
    String objectId;

    public AuctionItem(String sellerToken, String objectId) {
        this.sellerToken = sellerToken;
        this.objectId = objectId;
    }
}

//auction state class
class AuctionState {
    AuctionItem item;
    int currentHighestBid = 0;
    String highestBidderToken = null;
    long auctionEndTime = 0;
    
    //keep track of everyone's highest bid to find the 2nd place winner
    java.util.concurrent.ConcurrentHashMap<String, Integer> allBids = new java.util.concurrent.ConcurrentHashMap<>();
    //keep track of people who cancelled so they don't get picked twice
    java.util.Set<String> skippedBidders = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public AuctionState(AuctionItem item, long endTime) {
        this.item = item;
        this.auctionEndTime = endTime;
    }
}