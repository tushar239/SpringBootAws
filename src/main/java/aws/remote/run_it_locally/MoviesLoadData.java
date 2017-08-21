package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.util.Iterator;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */

/*
    NOTE:
    There are so many records to put and when you run this program, at some point it will definitely a lot more data (> provisioned write throughput) to write, and so you will see ProvisionedThroughputExceededException.
 */
public class MoviesLoadData extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        JsonParser parser = new JsonFactory().createParser(new File("./SpringBootAws/moviedata.json"));

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iter = rootNode.iterator();

        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int year = currentNode.path(PARTITION_KEY_YEAR).asInt();
            String title = currentNode.path(SORT_KEY_TITLE).asText();

            try {

                table.putItem(
                        new Item()
                                .withPrimaryKey(PARTITION_KEY_YEAR, year, SORT_KEY_TITLE, title)
                                //.withList() // list attribute
                                .withJSON(INFO_MAP_ATTRIBUTE, currentNode.path(INFO_MAP_ATTRIBUTE).toString())
                );
                System.out.println("PutItem succeeded: " + year + " " + title);

            } catch (Exception e) {
                System.err.println("Unable to add movie: " + year + " " + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }

}
