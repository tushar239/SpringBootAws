package aws.remote.run_it_locally;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */
public abstract class AbstractMoviesTableOperations {

    protected static String MOVIES_TABLE = "Movies";
    protected static String PARTITION_KEY_YEAR = "year";
    protected static String SORT_KEY_TITLE = "title";
    protected static String INFO_MAP_ATTRIBUTE = "info";

    protected static DynamoDB getDynamoDbHandler() {
        AWSCredentials awsCredentials = new BasicAWSCredentials(Credentials.access_key_id, Credentials.secret_access_key);

        AmazonDynamoDB client = new AmazonDynamoDBClient(awsCredentials);
        client.setRegion(Region.getRegion(Regions.US_WEST_2));

        DynamoDB dynamoDB = new DynamoDB(client);

        return dynamoDB;

    }
}
