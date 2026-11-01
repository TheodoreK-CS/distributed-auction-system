# Distributed Real-Time Auction & P2P File Transfer System

A multithreaded Client-Server and Peer-to-Peer (P2P) auction system written in Java. It uses a centralized TCP server to manage live auctions and user sessions, while file transfers between the seller and the winning bidder happen directly P2P over UDP using a custom Go-Back-N implementation.

## How to Build & Run

The server accepts any number of concurrent client connections. To test a live market locally (`127.0.0.1`), run one server terminal alongside multiple client terminals.

### 1. Compile
From the root directory (containing both `Server` and `Client`), run:

`javac Server/*.java Client/*.java`

### 2. Start the Server
In your first terminal, start the auction server (listens on TCP port `5000`):

`java Server.AuctionServer`

### 3. Launch Autonomous Bidders
Open as many separate terminal windows as you want in the same root directory and start the peers with `<Username> <Password>`:

Ex.
- **Terminal 2:** `java Client.Bidder Auser pass123`
- **Terminal 3:** `java Client.Bidder Buser pass123`
- **Terminal 4:** `java Client.Bidder Cuser pass123`
- **Terminal 5:** `java Client.Bidder Duser pass123`
- **Terminal 6:** `java Client.Bidder Euser pass123`

*(If you run `java Client.Bidder` without arguments, it defaults to `TestUser` / `password123`. If a username is already taken, the peer appends a random number and retries automatically).*

## How It Works

Once the server and peers are running, everything happens autonomously:

1. **Setup & Item Generation:** Each peer creates a local `shared_directory_<username>/` folder, logs in to get a `UUID` session token, opens a random UDP port (`6000-8999`), and starts `ItemGeneratorThread` to create new `.txt` items every 0 to 120 seconds.
2. **Concurrent Auctions & Sniping:** The server runs up to 2 auctions at the same time (60 seconds each). Peers check active auctions every 10 seconds (`GET_CURRENT_AUCTION`). If an item doesn't belong to them, they have a 60% chance to bid (+1% to +10% above current price). In the last 6 seconds, peers switch to `[SNIPE]` mode and bid more aggressively (up to +20%).
3. **P2P File Transfer (UDP Go-Back-N):** When an auction ends, the winner connects directly to the seller's UDP port (70% chance to proceed). The file is sent in chunks with a simulated 20% packet/ACK drop rate, 2-second timeout retransmissions, and saved locally as `WON_<ObjectID>.txt`.
4. **Cancellations & 2nd-Bidder Fallback:** Winners have a 30% chance to cancel the purchase. When that happens, the server lowers their reputation score and automatically awards the item to the 2nd-highest active bidder (`[Fallback]`).
5. **Disconnect Handling:** Pressing `Ctrl + C` on a client terminal triggers a shutdown hook that sends an emergency `LOGOUT` message to the server, cleaning up the session and cancelling any active auctions from that seller.

## Implementation Details

### TCP Auction Server (`Server`)
- `AuctionEngine` checks active auctions every 1 second without blocking, closes expired ones, and pulls new items from the queue.
- Shared state (`registeredUsers`, `activeSessions`, `activeAuctions`, `pendingTransactions`) is stored in `ConcurrentHashMap` and `ConcurrentLinkedDeque` structures so `ClientHandler` threads don't run into race conditions.
- Seller reputation is updated after each transaction using `newRep = (1 - 0.25) * oldRep + (0.25 * result)`. When starting a new auction, the engine checks the first two items in the queue and gives priority to the seller with the higher reputation (`[Priority Swap]`).

### UDP Go-Back-N Protocol (`Client`)
- **Sliding Window (`N = 3`):** Files are split into 64-byte chunks with a 4-byte sequence number header using `ByteBuffer`.
- **Simulated Packet Loss:** To test reliability, the receiver intentionally drops 20% of incoming data packets and 20% of outgoing ACKs. The sender tracks cumulative ACKs and resends the current window if 2 seconds pass without an update.
- **EOF Handling:** To prevent the receiver from getting stuck at 99% if the final packet is dropped, the sender transmits the EOF packet (`seqNum = -1`) 5 times in a row before closing.

## Project Structure

- `Server/AuctionServer.java`: Main server entry point (Port 5000), concurrent maps, and `UserProfile` / `AuctionState` classes.
- `Server/AuctionEngine.java`: 1s auction loop, timer resolution, and reputation queue priority.
- `Server/ClientHandler.java`: Per-client thread handling login, bid validation, broadcasts, and 2nd-bidder fallback.
- `Server/AuctionMessage.java`: `Serializable` message object (`command` and `payload`).
- `Client/Bidder.java`: Peer client logic, automated bidding/sniping, and UDP Go-Back-N sender/receiver sockets.
- `Client/ItemGeneratorThread.java`: Background thread that generates random `.txt` item files every 0-120 seconds.
