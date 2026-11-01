package Server;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.UUID;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    
    private String currentSessionToken = null; 

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    @Override
    public void run() {
        try {
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            in = new ObjectInputStream(clientSocket.getInputStream());

            while (true) {
                AuctionMessage incomingMsg = (AuctionMessage) in.readObject();
                String command = incomingMsg.getCommand();
                String payload = (String) incomingMsg.getPayload();
                String[] parts = payload.split(" ");
                
                //check what bidder wants to do
                switch (command) {
                    case "REGISTER":
                        handleRegister(parts);
                        break;
                    case "LOGIN":
                        handleLogin(parts);
                        break;
                    case "REQUEST_AUCTION":
                        handleInitialInventory(parts);
                        break;
                    case "NEW_ITEM":
                        handleNewItem(parts);
                        break;
                    case "GET_CURRENT_AUCTION":
                        handleGetStatus();
                        break;
                    case "PLACE_BID":
                        handlePlaceBid(parts);
                        break;
                    case "TRANSACTION_SUCCESS":
                        handleTransactionSuccess(parts);
                        break;
                    case "TRANSACTION_FAIL":
                        handleTransactionFail(parts);
                        break;
                    case "LOGOUT":
                        handleLogout(payload);
                        return; 
                    default:
                        System.out.println("Unknown command: " + command);
                }
            }
        } catch (Exception e) {
            System.out.println("Client disconnected unexpectedly.");
        } finally {
            if (currentSessionToken != null) {
                AuctionServer.activeSessions.remove(currentSessionToken);
                AuctionServer.connectionDetails.remove(currentSessionToken);
                System.out.println("Cleaned up ghost session for token: " + currentSessionToken);
            }
        }
    }

    private void handleRegister(String[] parts) throws Exception {
        String user = parts[0];
        String pass = parts[1];
        //if user with this name already exists then fail
        if (AuctionServer.registeredUsers.containsKey(user)) {
            out.writeObject(new AuctionMessage("REGISTER_RESPONSE", "FAILURE"));
        }
        //else register user
        else {
            AuctionServer.registeredUsers.put(user, new UserProfile(pass));
            out.writeObject(new AuctionMessage("REGISTER_RESPONSE", "SUCCESS"));
            System.out.println("Registered new user: " + user);
        }
        out.flush();
    }

    private void handleLogin(String[] parts) throws Exception {
        String user = parts[0];
        String pass = parts[1];
        UserProfile profile = AuctionServer.registeredUsers.get(user);
        //if profile exists and passwords match then log in
        if (profile != null && profile.password.equals(pass)) {
            //give user session token
            String token = UUID.randomUUID().toString();
            
            //save token to instance variable for disconnect cleanup
            this.currentSessionToken = token; 
            
            AuctionServer.activeSessions.put(token, user);

            AuctionServer.clientStreams.put(token, out);
            out.writeObject(new AuctionMessage("LOGIN_RESPONSE", "SUCCESS " + token));
            System.out.println(user + " logged in. Token: " + token);
        } else {
            out.writeObject(new AuctionMessage("LOGIN_RESPONSE", "FAILURE"));
        }
        out.flush();
    }

    private void handleInitialInventory(String[] parts) throws Exception {
        String token = parts[0];
        String ip = parts[1];
        String port = parts[2];

        if (AuctionServer.activeSessions.containsKey(token)) {
            AuctionServer.connectionDetails.put(token, ip + ":" + port);

            if (parts.length > 3 && !parts[3].equals("EMPTY")) {
                String[] items = parts[3].split(",");
                //add items to auction queue
                for (String itemName : items) {
                    AuctionServer.auctionQueue.add(new AuctionItem(token, itemName));
                }
            }
            out.writeObject(new AuctionMessage("REQUEST_RESPONSE", "SUCCESS"));
        } else {
            out.writeObject(new AuctionMessage("REQUEST_RESPONSE", "FAILURE"));
        }
        out.flush();
    }

    //add new item to queue
    private void handleNewItem(String[] parts) {
        String token = parts[0];
        String itemName = parts[1];
        AuctionServer.auctionQueue.add(new AuctionItem(token, itemName));
        System.out.println("Queue updated (+1 item): " + itemName);
    }

    private void handleGetStatus() throws Exception {
        //if no auction active
        if (AuctionServer.activeAuctions.isEmpty()) {
            synchronized(out) {
                out.writeObject(new AuctionMessage("AUCTION_INFO", "No active auctions."));
                out.flush();
            }
        }
        //if there are ongoing auctions
        else {
            StringBuilder allStatuses = new StringBuilder();

            // Loop through all active auctions in the map
            for (String objectId : AuctionServer.activeAuctions.keySet()) {
                AuctionState state = AuctionServer.activeAuctions.get(objectId);
                if (state == null) continue;

                String sellerToken = state.item.sellerToken;

                //if the seller is no longer in the active sessions map cancel the auction
                if (!AuctionServer.activeSessions.containsKey(sellerToken)) {
                    System.out.println("Seller dropped! Cancelling auction for " + objectId);
                    
                    //remove from active auctions
                    AuctionServer.activeAuctions.remove(objectId);
                    
                    //BROADCAST the cancellation to the entire network
                    for (ObjectOutputStream peerOut : AuctionServer.clientStreams.values()) {
                        try {
                            synchronized(peerOut) {
                                peerOut.writeObject(new AuctionMessage("AUCTION_INFO", "ALERT: Auction for " + objectId + " cancelled due to Seller disconnect."));
                                peerOut.flush();
                            }
                        } catch (Exception e) {
                            //ignore dropped peers
                        }
                    }
                }
                //if everything is fine
                else {
                    long timeLeft = (state.auctionEndTime - System.currentTimeMillis()) / 1000;
                    //give auction information
                    String status = String.format("Item: %s | SellerToken: %s | Current Bid: %d | Time Left: %ds", 
                                    state.item.objectId, 
                                    sellerToken,
                                    state.currentHighestBid, 
                                    Math.max(0, timeLeft));
                    
                    //add a new line for each auction
                    allStatuses.append(status).append("\n");
                }
            }

            //send the combined string of all active auctions back to the bidder
            if (allStatuses.length() > 0) {
                synchronized(out) {
                    out.writeObject(new AuctionMessage("AUCTION_INFO", allStatuses.toString().trim()));
                    out.flush();
                }
            }
        }
    }

    private void handlePlaceBid(String[] parts) throws Exception {
        //safety check to prevent crash before Bidder is updated
        if (parts.length < 3) {
            out.writeObject(new AuctionMessage("BID_ERROR", "Protocol Update: Client must send objectId to bid on simultaneous auctions. Update Bidder.java."));
            out.flush();
            return;
        }

        //format: [Token] [ObjectID] [Amount]
        String token = parts[0];
        String objectId = parts[1];
        int amount = Integer.parseInt(parts[2]);

        //get the specific auction state from the map
        AuctionState state = AuctionServer.activeAuctions.get(objectId);

        if (state == null) {
            out.writeObject(new AuctionMessage("BID_ERROR", "Auction ended or does not exist."));
        } else if (token.equals(state.item.sellerToken)) {
            out.writeObject(new AuctionMessage("BID_ERROR", "You cannot bid on your own item."));
        } else if (token.equals(state.highestBidderToken)) {
            out.writeObject(new AuctionMessage("BID_ERROR", "You are already the highest bidder!"));    
        } else if (amount <= state.currentHighestBid) {
            out.writeObject(new AuctionMessage("BID_ERROR", "Bid too low."));
        }

        //if no problem with bid update bid information
        else {
            state.currentHighestBid = amount;
            state.highestBidderToken = token;

            //save their bid to the history map
            state.allBids.put(token, amount);
            
            //confirm to the person who made the bid
            out.writeObject(new AuctionMessage("BID_CONFIRMATION", "You are the highest bidder for " + objectId + "!"));
            System.out.println("New High Bid on " + objectId + ": " + amount + " by " + AuctionServer.activeSessions.get(token));
            
            //BROADCAST to everyone else on the network
            long timeLeft = (state.auctionEndTime - System.currentTimeMillis()) / 1000;

            String broadcastStatus = String.format("Item: %s | SellerToken: %s | Current Bid: %d | Time Left: %ds", 
                                                    state.item.objectId, state.item.sellerToken, amount, Math.max(0, timeLeft));

            for (ObjectOutputStream peerOut : AuctionServer.clientStreams.values()) {
                if (peerOut != this.out) {
                    //don't send the broadcast to the guy who just bid
                    try {
                        synchronized(peerOut) {
                            peerOut.writeObject(new AuctionMessage("AUCTION_INFO", broadcastStatus));
                            peerOut.flush();
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to broadcast to a peer.");
                    }
                }
            }
        }
        out.flush();
    }

    private void handleTransactionSuccess(String[] parts) {
        String token = parts[0];
        String objectId = parts[1]; 
        
        String buyerUser = AuctionServer.activeSessions.get(token);
        if (buyerUser != null) {
            UserProfile buyerProfile = AuctionServer.registeredUsers.get(buyerUser);
            buyerProfile.num_auctions_bidder++;
            
            double oldRep = buyerProfile.reputation_score;
            buyerProfile.reputation_score = (1 - 0.25) * oldRep + (0.25 * 1); 
            System.out.println("[Reputation] " + buyerUser + " completed purchase! Score increased to: " + buyerProfile.reputation_score);
        }
        
        //transaction is done, remove it from the waiting room
        AuctionServer.pendingTransactions.remove(objectId);
    }

    private void handleTransactionFail(String[] parts) {
        String token = parts[0];
        String objectId = parts[1]; 
        
        //penalty to the user who cancelled
        String buyerUser = AuctionServer.activeSessions.get(token);
        if (buyerUser != null) {
            UserProfile buyerProfile = AuctionServer.registeredUsers.get(buyerUser);
            double oldRep = buyerProfile.reputation_score;
            buyerProfile.reputation_score = (1 - 0.25) * oldRep + (0.25 * 0); 
            System.out.println("[Reputation] " + buyerUser + " CANCELLED! Score dropped to: " + buyerProfile.reputation_score);
        }

        //find the next highest bidder
        AuctionState state = AuctionServer.pendingTransactions.get(objectId);
        if (state != null) {
            //mark this bidder as skipped
            state.skippedBidders.add(token);
            
            String nextWinnerToken = null;
            int nextHighestBid = -1;
            
            //search through all bids to find the highest one that hasn't been skipped
            for (String bidderToken : state.allBids.keySet()) {
                if (!state.skippedBidders.contains(bidderToken) && AuctionServer.activeSessions.containsKey(bidderToken)) {
                    int theirBid = state.allBids.get(bidderToken);
                    if (theirBid > nextHighestBid) {
                        nextHighestBid = theirBid;
                        nextWinnerToken = bidderToken;
                    }
                }
            }
            
            //if found next valid bidder
            if (nextWinnerToken != null) {
                System.out.println("[Fallback] Awarding " + objectId + " to 2nd highest bidder: " + AuctionServer.activeSessions.get(nextWinnerToken));
                
                //update the state so the new winner is official
                state.highestBidderToken = nextWinnerToken;
                state.currentHighestBid = nextHighestBid;
                
                //push the AUCTION_WON message to the new winner
                try {
                    ObjectOutputStream newWinnerOut = AuctionServer.clientStreams.get(nextWinnerToken);
                    String sellerAddr = AuctionServer.connectionDetails.get(state.item.sellerToken);
                    if (newWinnerOut != null && sellerAddr != null) {
                        AuctionMessage winMsg = new AuctionMessage("AUCTION_WON", sellerAddr + " " + objectId);
                        synchronized(newWinnerOut) {
                            newWinnerOut.writeObject(winMsg);
                            newWinnerOut.flush();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to notify fallback winner.");
                }
            //if no other bidders
            } else {
                System.out.println("[Fallback] No valid fallback bidders left for " + objectId + ". Auction totally failed.");
                AuctionServer.pendingTransactions.remove(objectId);
            }
        }
    }

    //if user logs out
    private void handleLogout(String token) throws Exception {
        String user = AuctionServer.activeSessions.remove(token);
        if (user != null) {
            AuctionServer.connectionDetails.remove(token);
            //clear so the finally block ignores it
            this.currentSessionToken = null;
            System.out.println("User logged out: " + user);
            out.writeObject(new AuctionMessage("LOGOUT_SUCCESS", "Goodbye!"));
            out.flush();
        }
    }
}