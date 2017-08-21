package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */
public class MoviesAddNewItem extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        int year = 2015;
        String title = "The Big New Movie";

        final Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table
                    .putItem(
                            new Item()
                                    .withPrimaryKey(PARTITION_KEY_YEAR, year, SORT_KEY_TITLE, title)
                                    .withMap(INFO_MAP_ATTRIBUTE, infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        } catch (Exception e) {
            System.err.println("Unable to add item: " + year + " " + title);
            System.err.println(e.getMessage());
        }

    }
}
