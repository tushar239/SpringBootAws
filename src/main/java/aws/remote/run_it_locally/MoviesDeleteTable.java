package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */
public class MoviesDeleteTable extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        try {
            System.out.println("Attempting to delete table; please wait...");
            table.delete();
            table.waitForDelete();
            System.out.print("Success.");
        }
        catch (Exception e) {
            System.err.println("Unable to delete table: ");
            System.err.println(e.getMessage());
        }
    }
}
