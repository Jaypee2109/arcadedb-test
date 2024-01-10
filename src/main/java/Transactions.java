import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.schema.Schema;

import java.util.*;

public class Transactions {


    public static void main(String[] args) {

        // Starting embedded server
        ArcadeDB arcade = new ArcadeDB();
        ArcadeDBServer server = arcade.getServer();


        // Connect to database
        Database myDatabase = server.getOrCreateDatabase("mydb");


        // Define schema
        myDatabase.transaction(() -> {
            myDatabase.getSchema().getOrCreateVertexType("Sensor").getOrCreateProperty("id", Type.STRING);
            myDatabase.getSchema().getOrCreateVertexType("Sensor").getOrCreateProperty("timeseries", Type.MAP);
            myDatabase.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Sensor", "id");

            myDatabase.getSchema().getOrCreateDocumentType("Record").getOrCreateProperty("timestamp", Type.LONG);
            myDatabase.getSchema().getOrCreateDocumentType("Record").getOrCreateProperty("sensorid", Type.STRING);
            myDatabase.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Record", new String[] { "sensorid", "timestamp" });

            myDatabase.getSchema().getOrCreateVertexType("Year");
            myDatabase.getSchema().getOrCreateVertexType("Month");
            myDatabase.getSchema().getOrCreateVertexType("Day");
            myDatabase.getSchema().getOrCreateVertexType("Day").getOrCreateProperty("timeseries", Type.LIST);

            myDatabase.getSchema().getOrCreateEdgeType("HAS_YEAR");
            myDatabase.getSchema().getOrCreateEdgeType("HAS_MONTH");
            myDatabase.getSchema().getOrCreateEdgeType("HAS_DAY");

            myDatabase.getSchema().getOrCreateEdgeType("AFFECTS");
        });


        // Clear database
        myDatabase.command("sql", "delete from Sensor");
        myDatabase.command("sql", "delete from Record");
        myDatabase.command("sql", "delete from Year");
        myDatabase.command("sql", "delete from Month");
        myDatabase.command("sql", "delete from Day");
        myDatabase.command("sql", "delete from HAS_YEAR");
        myDatabase.command("sql", "delete from HAS_MONTH");
        myDatabase.command("sql", "delete from HAS_DAY");
        myDatabase.command("sql", "delete from AFFECTS");



        String method = "index";
        int numOfVertices = 10;
        int numOfRecordsPerVertex = 1000;
        int numOfVertexEdges = 3;

        HashMap<String, Object> propertiesOfVertex = new HashMap<>();
        propertiesOfVertex.put("location", "indoor");
        propertiesOfVertex.put("method", method);

        int maxValueSensorID = new Random().nextInt(numOfVertices) + 1;
        int maxValuePosition = new Random().nextInt(numOfRecordsPerVertex);
        System.out.println("Sensor with max value: " + maxValueSensorID);
        System.out.println("Position of max value: " + maxValuePosition);

        for(int k = 1; k <= numOfVertices; k++){

            int finalID = k;
            createVertices(myDatabase, "Sensor", String.valueOf(k), propertiesOfVertex);


            System.out.println("Time " + k + ": " + measureExecutionTime(() -> {

                for(int i = 0; i < numOfRecordsPerVertex; i++){
                    int value = 0;

                    if(finalID == maxValueSensorID && i == maxValuePosition)
                        value = 123;

                    addTimeseriesRecord(myDatabase, method, String.valueOf(finalID), i, value);
                }
            }));


        }

        createRandomEdges(myDatabase, "Sensor", "AFFECTS", 1, numOfVertices, numOfVertexEdges);
        System.out.println("finished");


    }

    public static void createRandomEdges(Database db, String vertexTypeName, String edgeTypeName, int lowerBound, int upperBound, int numberOfEdges){
        db.transaction(() -> {

            ArrayList<Integer> indices = new ArrayList<>();

            for(int k = lowerBound; k <= upperBound; k++) {
                indices.add(k);
            }

            for(int i = lowerBound; i <= upperBound; i++){

                Random rand = new Random();
                ArrayList<Integer> possibleRelations = new ArrayList<>(indices);
                possibleRelations.remove(Integer.valueOf(i));
                MutableVertex fromVertex = db.lookupByKey(vertexTypeName, "id", i).next().getRecord().asVertex().modify();


                for(int k = 0; k < numberOfEdges; k++){
                    Integer index = possibleRelations.get(rand.nextInt(possibleRelations.size()));

                    MutableVertex toVertex = db.lookupByKey(vertexTypeName, "id", index)
                            .next()
                            .getRecord()
                            .asVertex()
                            .modify();

                    fromVertex.newEdge(edgeTypeName, toVertex, false);

                    possibleRelations.remove(index);
                }

            }


        });
    }

    public static double measureExecutionTime(Runnable task){

        long startTime = System.nanoTime();
        task.run();
        long stopTime = System.nanoTime();

        return (stopTime - startTime) / 1_000_000_000.0;


    }


    public static void createVertices(Database db, String typeName, String sensorID, Map<String, Object> properties){
        // Create Sensor
        db.transaction(() -> {
            MutableVertex sensor = db.newVertex(typeName);

            sensor.set("id", sensorID);
            sensor.set("timeseries", Collections.emptyMap());
            sensor.set(properties);

            sensor.save();

        });
    }

    public static void addTimeseriesRecord(Database db, String method, String sensorID, long timestamp, int value){
        switch(method){
            case "embed":
                saveEmbedRecord(db, sensorID, timestamp, value);

                // Example sql query: select timeseries from (select from Sensor unwind timeseries) where timeseries.timestamp > 2 and timeseries.timestamp <= 5

                break;

            case "graph":
                saveGraphRecord(db, sensorID, timestamp, value);

                break;

            case "reference":
                saveReferenceRecord(db, sensorID, timestamp, value);

                break;

            default:
                saveIndexRecord(db, sensorID, timestamp, value);

        }
    }

    private static void saveGraphRecord(Database db, String ID, long timestamp, int value){

        Date date = new Date(timestamp);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int yearDate = calendar.get(Calendar.YEAR);
        int monthDate = calendar.get(Calendar.MONTH) + 1; // Calendar.MONTH is zero-based (0 for January)
        int dayDate = calendar.get(Calendar.DAY_OF_MONTH);

        db.transaction(() -> {

            MutableVertex sensor = db.lookupByKey("Sensor", "id", ID).next().getRecord().asVertex().modify();

            MutableVertex yearVertex = getOrCreateDateVertex(db, sensor, "Year", yearDate);
            MutableVertex monthVertex = getOrCreateDateVertex(db, yearVertex, "Month", monthDate);
            MutableVertex dayVertex = getOrCreateDateVertex(db, monthVertex, "Day", dayDate);

            MutableDocument record = dayVertex.newEmbeddedDocument("Record", "timeseries");
            record.set("timestamp", timestamp, "value", value);

            record.save();
            dayVertex.save();

        });
    }

    private static void saveReferenceRecord(Database db, String ID, long timestamp, int value){

        db.transaction(() -> {
            MutableVertex sensor = db.lookupByKey("Sensor", "id", ID).next().getRecord().asVertex().modify();

            MutableDocument record = db.newDocument("Record");
            record.set("sensorid", ID, "timestamp", timestamp, "value", value);

            record.save();

            sensor.set("timeseries", record);
            sensor.save();
        });

    }

    private static void saveEmbedRecord(Database db, String ID, long timestamp, int value){
        db.transaction(() -> {
            //MutableVertex sensor = db.lookupByKey("Sensor", "id", ID).next().getRecord().asVertex().modify();

            //MutableDocument record = sensor.newEmbeddedDocument("Record", "timeseries", String.valueOf(timestamp));
            //record.set("value", value);

            //record.save();

            db.command("sql", String.format("update Sensor set timeseries[%s] = {'@type':'Record','value':%s,'@cat':'d'} where id = %s", timestamp, value, ID));

        });
    }

    private static void saveIndexRecord(Database db, String ID, long timestamp, int value){
        db.transaction(() -> {
            //MutableVertex sensor = db.lookupByKey("Sensor", "id", ID).next().getRecord().asVertex().modify();

            MutableDocument record = db.newDocument("Record");
            record.set("sensorid", ID, "timestamp", timestamp, "value", value);

            record.save();


        });
    }


    private static MutableVertex getOrCreateDateVertex(Database database, MutableVertex sourceVertex, String destVertexType, int date){

        String edgeType = "HAS_" + destVertexType.toUpperCase();
        Iterator<Vertex> neighbours = sourceVertex.getVertices(Vertex.DIRECTION.OUT, edgeType).iterator();

        boolean destFound = false;
        MutableVertex destVertex = null;

        while(!destFound && neighbours.hasNext()){
            Vertex neighbour = neighbours.next();

            if(neighbour.get("date").equals(date)){
                destFound = true;
                destVertex = neighbour.modify();
            }
        }

        if(destVertex == null){
            destVertex = database.newVertex(destVertexType);
            destVertex.set("date", date);
            destVertex.save();

            sourceVertex.newEdge(edgeType, destVertex, true);
        }

        return destVertex;


    }

}
