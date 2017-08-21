package aws.remote.rest.access_it_using_beanstalk_url;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 http://localhost:8080/dynamoDbTables/sample/health

 Using beanstalk url - http://springbootaws-dev.us-west-2.elasticbeanstalk.com/dynamoDbTables/list
 You need to get beanstalk url from aws console

 This code runs only on EC2. You cannot run it from local because it does not use user's access keys to connect to DynamoDB.
 It assumes that your app is deployed on EC2 and that EC2 has right Role assigned to talk to DynamoDB and Beanstalk(if you are using Beanstalk to deploy this app).
 */
@RestController
@RequestMapping("/dynamoDbTables/sample")
public class DynamoDbTableOnAwsController {

    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public @ResponseBody
    String index() {
        return "UP";
    }


    private AmazonDynamoDB getAmazonDynamoDB() {
        // To connect to DynamoDB from your local, you need access key, but after deploying to EC2 using BeanStalk, if you have correct role assigned to EC2 that can access DynamoDB, then you don't need access keys while running this code from EC2.
        //AWSCredentials awsCredentials = new BasicAWSCredentials(Credentials.access_key_id, Credentials.secret_access_key);
        //AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(awsCredentials);

        //AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        //amazonDynamoDB.setEndpoint("localhost:8000"); // only for local dynamodb, for aws dynamodb, by seeing set region, it will create endpoint automatically (http://dynamodb.us-west-2.amazonaws.com)
        return AmazonDynamoDBClientBuilder.standard()
                //.setEndpoint("localhost:8000") // only for local dynamodb, for aws dynamodb, by seeing set region, it will create endpoint automatically (http://dynamodb.us-west-2.amazonaws.com)
                .withRegion(Regions.US_WEST_2.getName()) // not for local dynamodb, only for aws dynamodb
                .build();
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public @ResponseBody
    List<String> listDynamoDBTables() {
        System.out.println("Your DynamoDB tables:\n");
        AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDB();


        List<String> allTables = new ArrayList<>();

        boolean hasMoreTables = true;
        while (hasMoreTables) {
            String lastName = null;
            try {
                ListTablesResult tableList = null;
                if (lastName == null) {
                    tableList = amazonDynamoDB.listTables();
                }

                List<String> tableNames = tableList.getTableNames();
                allTables.addAll(tableNames);

                if (tableNames.size() > 0) {
                    for (String cur_name : tableNames) {
                        System.out.format("* %s\n", cur_name);
                    }
                } else {
                    System.out.println("No tables found!");
                    System.exit(0);
                }

                lastName = tableList.getLastEvaluatedTableName();
                if (lastName == null) {
                    hasMoreTables = false;
                }
            } catch (AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                //System.exit(1);
            }
        }
        System.out.println("\nDone!");

        return allTables;
    }

    @RequestMapping(value = "/create", method = RequestMethod.POST)
    public @ResponseBody
    String createTable() {
        String table_name = "sample";

        System.out.format(
                "Creating table \"%s\" with a simple primary key: \"Name\".\n",
                table_name);

        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(
                        "Name", ScalarAttributeType.S))// create column named "Name" of type String
                .withKeySchema(new KeySchemaElement("Name", KeyType.HASH)) // primary key is column "Name"
                .withProvisionedThroughput(new ProvisionedThroughput(
                        new Long(10), new Long(10)))
                .withTableName(table_name);

        AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDB();

        String createdTableName = null;
        try {
            CreateTableResult result = amazonDynamoDB.createTable(request);
            createdTableName = result.getTableDescription().getTableName();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            //System.exit(1);
        }
        System.out.println("Done!");

        return createdTableName;

    }

    @RequestMapping(value = "/put", method = RequestMethod.PUT)
    public void putItem() {
        String table_name = "sample";

        AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDB();

        Map<String, AttributeValue> fieldValue = new HashMap<>();
        fieldValue.put("Name", new AttributeValue("Tushar " + System.currentTimeMillis()));
        fieldValue.put("Address", new AttributeValue("{\"city\":\"Sacramento\", \"state\":\"CA\", \"zip\":\"98051\"}"));

        try {
            amazonDynamoDB.putItem(table_name, fieldValue);
        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The table \"%s\" can't be found.\n", table_name);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
        } catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
        }
        System.out.println("Done!");
    }


    // practice scenarios
    // http://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Java.03.html#GettingStarted.Java.03.04



}