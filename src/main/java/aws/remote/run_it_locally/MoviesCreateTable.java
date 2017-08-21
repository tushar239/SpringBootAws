package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.util.Arrays;

// creating Movies table on AWS DynamoDB using access keys
// Examples are taken from
// http://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Java.html
// in a link above, it shows how to connect to local DynamoDB, but I couldn't make it work for some reason. So, connected to AWS DynamoDB using Credentials.
public class MoviesCreateTable extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(MOVIES_TABLE,
                    Arrays.asList(new KeySchemaElement(PARTITION_KEY_YEAR, KeyType.HASH), // Partition key  (KeyType.HASH)
                            new KeySchemaElement(SORT_KEY_TITLE, KeyType.RANGE)), // Sort key (KeayType.RANGE)
                    Arrays.asList(new AttributeDefinition(PARTITION_KEY_YEAR, ScalarAttributeType.N),
                            new AttributeDefinition(SORT_KEY_TITLE, ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));

            table.waitForActive();

            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }

    }
}
