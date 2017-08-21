package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */
/*
DynamoDB supports atomic counters, where you use the UpdateItem method to increment or decrement the value of an existing attribute without interfering with other write requests. (All write requests are applied in the order in which they were received.)

If update request is received by DynamoDB, but response has not received for some reason (may be network issue). If you retry, then for the retried UpdateItem request will increment the value.
This kind of behavior may not be expected in some business cases. In that case, you should put conditional check also during retry to check the incremented value before incrementing it.

The following program shows how to increment the rating for a movie.
Each time you run it, the program increments this attribute by one.
 */
public class MoviesIncrementAtomicCounter extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(PARTITION_KEY_YEAR, year, SORT_KEY_TITLE, title)
                // incrementing info.rating by 1 in each UpdateItem
                .withUpdateExpression("set info.rating = info.rating + :val")
                .withValueMap(
                        new ValueMap()
                                .withNumber(":val", 1)
                )
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Incrementing an atomic counter...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            System.err.println("Unable to update item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }
}
