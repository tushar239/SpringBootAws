package aws.rest;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/*
 http://localhost:8080/dynamoDbTables/health

 Using beanstalk url - http://springbootaws-dev.us-west-2.elasticbeanstalk.com/dynamoDbTables/list
 You need to get beanstalk url from aws console
 */
@RestController
@RequestMapping("/dynamoDbTables")
public class DynamoDbController {

    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public @ResponseBody
    String index() {
        return "UP";
    }


    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public @ResponseBody
    List<String> listDynamoDBTables() {
        System.out.println("Your DynamoDB tables:\n");

        // To connect to DynamoDB from your local, you need access key, but after deploying to EC2 using BeanStalk, if you have correct role assigned to EC2 that can access DynamoDB, then you don't need access keys while running this code from EC2.
        //AWSCredentials awsCredentials = new BasicAWSCredentials(Credentials.access_key_id, Credentials.secret_access_key);
        //AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(awsCredentials);

        //AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                //.setEndpoint("localhost:8000") // only for local dynamodb, for aws dynamodb, by seeing set region, it will create endpoint automatically (http://dynamodb.us-west-2.amazonaws.com)
                .withRegion(Regions.US_WEST_2.getName()) // not for local dynamodb, only for aws dynamodb
                .build();
        //amazonDynamoDB.setEndpoint("localhost:8000"); // only for local dynamodb, for aws dynamodb, by seeing set region, it will create endpoint automatically (http://dynamodb.us-west-2.amazonaws.com)

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
                System.exit(1);
            }
        }
        System.out.println("\nDone!");

        return allTables;
    }

}