package Server;

import java.io.ObjectOutputStream;

public class AuctionEngine implements Runnable {

    @Override
    public void run() {
        System.out.println("[Auction Engine] Active and monitoring the queue...");

        while (true) {
            try {
                //see if any ongoing auctions have ended
                for (String objectId : AuctionServer.activeAuctions.keySet()) {
                    AuctionState state = AuctionServer.activeAuctions.get(objectId);
                    if (state != null && System.currentTimeMillis() >= state.auctionEndTime) {
                        resolveAuction(state);
                        
                        //move it to the waiting room before removing it
                        AuctionServer.pendingTransactions.put(objectId, state);

                        //remove finished auction to free up a slot
                        AuctionServer.activeAuctions.remove(objectId); 
                    }
                }

                //if we have less than 2 running start a new one
                while (AuctionServer.activeAuctions.size() < 2 && !AuctionServer.auctionQueue.isEmpty()) {
                    
                    //reputation checking starts
                    AuctionItem nextItem = null;
                    
                    //pull the first two items
                    AuctionItem firstItem = AuctionServer.auctionQueue.pollFirst();
                    AuctionItem secondItem = AuctionServer.auctionQueue.pollFirst();
                    
                    //if only one item in queue, use it
                    if (secondItem == null) {
                        nextItem = firstItem;

                    }
                    //if we have two items, compare reputations
                    else {
                        String seller1 = AuctionServer.activeSessions.get(firstItem.sellerToken);
                        String seller2 = AuctionServer.activeSessions.get(secondItem.sellerToken);
                        
                        //get reputations (default to 0 if something goes wrong)
                        double rep1 = (seller1 != null && AuctionServer.registeredUsers.containsKey(seller1)) 
                                        ? AuctionServer.registeredUsers.get(seller1).reputation_score : 0;
                        double rep2 = (seller2 != null && AuctionServer.registeredUsers.containsKey(seller2)) 
                                        ? AuctionServer.registeredUsers.get(seller2).reputation_score : 0;
                        
                        //if first item seller has lower reputation
                        if (rep1 < rep2) {
                            nextItem = secondItem;
                            //put the first item back at the front of the queue
                            AuctionServer.auctionQueue.addFirst(firstItem); 
                            System.out.println("[Priority Swap] Selected 2nd item due to higher reputation.");
                        }
                        //if second item seller has lower reputation
                        else {
                            nextItem = firstItem;
                            //put the second item back at the front of the queue
                            AuctionServer.auctionQueue.addFirst(secondItem); 
                        }
                    }
                    //reputation checking ended
                    
                    //create an AuctionState
                    int duration = 60; 
                    long endTime = System.currentTimeMillis() + (duration * 1000);
                    AuctionState newState = new AuctionState(nextItem, endTime);
                    
                    //put it in the map of active auctions
                    AuctionServer.activeAuctions.put(nextItem.objectId, newState);

                    System.out.println("\n--- NEW AUCTION STARTED ---");
                    System.out.println("Item: " + nextItem.objectId);
                    System.out.println("Seller: " + AuctionServer.activeSessions.get(nextItem.sellerToken));
                    System.out.println("---------------------------\n");
                } 

                //sleep for a second so the while(true) loop doesn't max out the CPU
                Thread.sleep(1000);
                
            } catch (InterruptedException e) {
                System.out.println("Auction Engine shut down.");
                break;
            } catch (Exception e) {
                System.err.println("Engine error: " + e.getMessage());
            }
        }
    }

    private void resolveAuction(AuctionState state) {
        System.out.println("\n--- AUCTION ENDED: " + state.item.objectId + " ---");
        
        //if highest bidder exists
        if (state.highestBidderToken != null) {
            String winnerToken = state.highestBidderToken;
            int winningAmount = state.currentHighestBid;
            String sellerToken = state.item.sellerToken;
            
            System.out.println("WINNER: " + AuctionServer.activeSessions.get(winnerToken) + " at " + winningAmount);

            //update statistics
            String sellerUser = AuctionServer.activeSessions.get(sellerToken);
            if (sellerUser != null) {
                AuctionServer.registeredUsers.get(sellerUser).num_auctions_seller++;
            }

            //send message to winner
            String sellerAddr = AuctionServer.connectionDetails.get(sellerToken);
            //if seller address exists
            if (sellerAddr != null) {
                ObjectOutputStream winnerOut = AuctionServer.clientStreams.get(winnerToken);
                if (winnerOut != null) {
                    try {
                        //send seller address to winner
                        String payload = sellerAddr + " " + state.item.objectId;
                        AuctionMessage winMsg = new AuctionMessage("AUCTION_WON", payload);
                        synchronized(winnerOut) {
                            winnerOut.writeObject(winMsg);
                            winnerOut.flush();
                        }
                        System.out.println("Notified winner via push message.");
                    } catch (Exception e) {
                        System.err.println("Failed to push notification to winner.");
                    }
                }

                ObjectOutputStream sellerOut = AuctionServer.clientStreams.get(sellerToken);
                if (sellerOut != null) {
                    try {
                        String sellerPayload = "Your item " + state.item.objectId + " sold to " + winnerToken + " for " + winningAmount;
                        AuctionMessage soldMsg = new AuctionMessage("AUCTION_SOLD", sellerPayload);
                        synchronized(sellerOut) {
                            sellerOut.writeObject(soldMsg);
                            sellerOut.flush();
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to notify seller.");
                    }
                }
            }
        } else {
            System.out.println("RESULT: No bids received.");
        }
        System.out.println("-----------------------------------\n");
    }
}