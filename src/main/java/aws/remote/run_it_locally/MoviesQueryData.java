package aws.remote.run_it_locally;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import java.util.HashMap;
import java.util.Iterator;

/*
    Query in DynamoDB must have Partition Key (optionally Sort Key) and optionally other filtering criteria based on other attributes.
    In Query, using KeyConditionExpression, you can add Partition Key (optionally Sort Key) as filtering criteria (where condition) in the query.

    In Scan,
    Even though, you can filter the records only by Partition Key (and optionally Sort Key),
    entire table is scanned and then filter is applied on all records as mentioned in filtering (where) condition.

    So, normally, you should try to avoid using Scan. Use Query.


    nameMap :
    provides name substitution.
    We use this because year is a reserved word in DynamoDB—you cannot use it directly in any expression, including KeyConditionExpression.
    We use the expression attribute name #yr to address this.

    valueMap :
    provides value substitution.
    We use this because you cannot use literals in any expression, including KeyConditionExpression.
    We use the expression attribute value :yyyy to address this.

    ProjectionExpression :
    specifies the attributes you want in the scan result.

    FilterExpression :
    specifies a condition that returns only items that satisfy the condition. All other items are discarded.

    Querying Indexes:
    This program shows how to query a table by its primary key attributes.
    In DynamoDB, you can optionally create one or more secondary indexes on a table, and query those indexes in the same way that you query a table.
    Secondary indexes give your applications additional flexibility by allowing queries on non-key attributes.
 */

public class MoviesQueryData extends AbstractMoviesTableOperations {
    public static void main(String[] args) throws Exception {

        DynamoDB dynamoDB = getDynamoDbHandler();

        Table table = dynamoDB.getTable(MOVIES_TABLE);

        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("#yr", PARTITION_KEY_YEAR);

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put(":yyyy", 1985);

        // Retrieve all movies release in year 1985.
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withProjectionExpression("#yr, title, info.rating") // it is like "select year,title,info.rating from Movies where year >= 1950 and year <= 1959
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Movies from 1985");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber(PARTITION_KEY_YEAR) + ": " + item.getString(SORT_KEY_TITLE));
            }

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }

        valueMap.put(":yyyy", 1992);
        valueMap.put(":letter1", "A");
        valueMap.put(":letter2", "L");

        // Retrieve all movies released in year 1992, with title beginning with the letter "A" through the letter "L".
        querySpec.withProjectionExpression("#yr, title, info.genres, info.actors[0]")
                .withKeyConditionExpression("#yr = :yyyy and title between :letter1 and :letter2")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        try {
            System.out.println("Movies from 1992 - titles A-L, with genres and lead actor");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": " + item.getString("title") + " " + item.getMap("info"));
            }

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1992:");
            System.err.println(e.getMessage());
        }
    }
}
