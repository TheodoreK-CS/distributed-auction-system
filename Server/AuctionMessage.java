package Server;

import java.io.Serializable;

public class AuctionMessage implements Serializable {
    //this ID ensures the Server and Bidder are using the exact same version of the class
    private static final long serialVersionUID = 1L; 
    
    private String command;
    private Object payload;

    public AuctionMessage(String command, Object payload) {
        this.command = command;
        this.payload = payload;
    }

    public String getCommand() {
        return command;
    }

    public Object getPayload() {
        return payload;
    }
}