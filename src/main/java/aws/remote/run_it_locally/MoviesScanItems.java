package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import java.util.Iterator;

/**
 * @author Tushar Chokshi @ 8/21/17.
 */
/*
The following program scans the entire Movies table, which contains approximately 5,000 items.
The scan specifies the optional filter to retrieve only the movies from the 1950s (approximately 100 items), and discard all of the others.

In Scan,
Even though, you can filter the records only by Partition Key (and optionally Sort Key),
entire table is scanned and then filter is applied on all records as mentioned in filtering (where) condition.

So, normally, you should try to avoid using Scan. Use Query.

 */
public class MoviesScanItems extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression("#yr, title, info.rating") // it is like "select year,title,info.rating from Movies where year >= 1950 and year <= 1959
                .withFilterExpression("#yr between :start_yr and :end_yr")
                .withNameMap(new NameMap().with("#yr", PARTITION_KEY_YEAR))
                .withValueMap(new ValueMap().withNumber(":start_yr", 1950).withNumber(":end_yr", 1959));

        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);

            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                System.out.println(item.toString());
            }

        } catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    }
}
