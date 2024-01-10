import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;

public class ArcadeDB {

    private ArcadeDBServer server;

    public ArcadeDB() {

        //use default settings
        ContextConfiguration config = new ContextConfiguration();
        ArcadeDBServer server = new ArcadeDBServer(config);

        server.start();
        this.server = server;
    }

    public ArcadeDBServer getServer(){
        return this.server;

    }

}
