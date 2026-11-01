package Client;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class ItemGeneratorThread extends Thread {
    private String sharedDirectoryPath;
    private Bidder bidder; 
    private int itemCount = 1;
    private Random rand = new Random();

    public ItemGeneratorThread(String sharedDirectoryPath, Bidder peer) {
        this.sharedDirectoryPath = sharedDirectoryPath;
        this.bidder = peer;
    }

    @Override
    public void run() {
        System.out.println("[Item Generator] Background thread started.");
        try {
            while (true) {
                //wait between 0 and 120 seconds to create new item
                double randomFactor = rand.nextDouble();
                long sleepTimeMillis = (long) (randomFactor * 120 * 1000); 
                
                System.out.println("   [Item Generator] Next item in " + (sleepTimeMillis / 1000) + "s...");
                //sleed till item generation
                Thread.sleep(sleepTimeMillis);
                
                //make item with name based on time and count of items
                String objectId = "Object_" + System.currentTimeMillis() + "_" + String.format("%02d", itemCount);
                String description = "Randomly generated item " + itemCount;
                int startBid = 10 + rand.nextInt(90); 
                int duration = 60; 
                
                String fileContent = String.format("[object_id: %s; description: \"%s\"; start_bid: \"%d\"; auction_duration: \"%d\"]", 
                                                    objectId, description, startBid, duration);

                File newItemFile = new File(sharedDirectoryPath + File.separator + objectId + ".txt");
                try (FileWriter writer = new FileWriter(newItemFile)) {
                    writer.write(fileContent);
                    System.out.println("   [Item Generator] Created new file: " + objectId + ".txt");
                }

                bidder.sendNewItemMessage(objectId + ".txt");
                itemCount++;
            }
        } catch (InterruptedException e) {
            System.out.println("[Item Generator] Interrupted and stopping.");
        } catch (IOException e) {
            System.err.println("[Item Generator] Error writing item file: " + e.getMessage());
        }
    }
}