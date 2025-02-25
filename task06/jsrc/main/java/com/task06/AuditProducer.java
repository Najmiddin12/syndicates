package com.task06;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 10)
@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, Map<String, Object>> {
	private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
			.region(Region.of(System.getenv("region")))
			.build();


	public Map<String, Object> handleRequest(DynamodbEvent request, Context context) {
		for(DynamodbEvent.DynamodbStreamRecord record : request.getRecords()) {
			context.getLogger().log("records: " + record.toString());

			var oldImage = record.getDynamodb().getOldImage();
			var newImage = record.getDynamodb().getNewImage();

			if (oldImage != null) {
				context.getLogger().log("Old Image: " + oldImage);
			}
			if (newImage != null) {
				context.getLogger().log("New Image: " + newImage);
			}

			String itemKey = newImage.get("key").getS();

			Map<String, AttributeValue> audit;
			if (oldImage != null) {
				context.getLogger().log("Updating...");
				audit = Map.of(
						"id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
						"itemKey", AttributeValue.builder().s(itemKey).build(),
						"modificationTime", AttributeValue.builder().s(Instant.now().toString()).build(),
						"updatedAttribute", AttributeValue.builder().s("value").build(),
						"oldValue", AttributeValue.builder().n(oldImage.get("value").getN()).build(),
						"newValue", AttributeValue.builder().n(newImage.get("value").getN()).build()
				);
			} else {
				context.getLogger().log("Inserting...");
				Map<String, AttributeValue> newItemValue = Map.of(
						"key", AttributeValue.builder().s(newImage.get("key").getS()).build(),
						"value", AttributeValue.builder().n(newImage.get("value").getN()).build()
				);
				audit = Map.of(
						"id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
						"itemKey", AttributeValue.builder().s(itemKey).build(),
						"modificationTime", AttributeValue.builder().s(Instant.now().toString()).build(),
						"newValue", AttributeValue.builder().m(newItemValue).build()
				);
			}
			try {
				context.getLogger().log("Putting the item: " + audit);
				putItem(audit, context);
				context.getLogger().log("Finishing....");
			} catch (Exception e) {
				context.getLogger().log("Error inserting item: " + e.getMessage());
				throw new RuntimeException(e);
			}
		}
		System.out.println("Hello from lambda");
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from Lambda");
		return resultMap;
	}


	private void putItem(Map<String, AttributeValue> audit, Context context) throws Exception {
		PutItemRequest putItemRequest = PutItemRequest.builder()
				.tableName(System.getenv("target_table"))
				.item(audit).build();

		PutItemResponse response = dynamoDbClient.putItem(putItemRequest);

		context.getLogger().log("PutItemResponse: " + response.toString());

	}
}
