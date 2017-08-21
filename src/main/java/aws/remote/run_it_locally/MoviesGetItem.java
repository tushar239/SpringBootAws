package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */

// Just like GetItemSpec, there is a BatchGetItemSpec also available for retrieving items in batch.

public class MoviesGetItem extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey(PARTITION_KEY_YEAR, year, SORT_KEY_TITLE, title);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome); // GetItem succeeded: { Item: {year=2015, title=The Big New Movie, info={plot=Nothing happens at all., rating=0}} }
        } catch (Exception e) {
            System.err.println("Unable to read item: " + year + " " + title);
            System.err.println(e.getMessage());
        }

    }
}
