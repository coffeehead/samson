package org.coffeehead.aws.samson;

import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;


/**
 * This is a wrapper for the Amazon SQS client
 *
 */
public class SQS {
	
	static final Logger LOG = LoggerFactory.getLogger(SQS.class);

	public static String createQueue(AmazonSQS client, String queueName) {
		CreateQueueRequest cqr = new CreateQueueRequest(queueName);
		CreateQueueResult result = client.createQueue(cqr);
		return result.getQueueUrl();
	}

	public static SendMessageResult sendMessage(AmazonSQS client, String queueUrl, String messageText) {
		SendMessageRequest smr = new SendMessageRequest(queueUrl, messageText);
		return client.sendMessage(smr);
	}

	public static List<Message> receiveMessage(AmazonSQS client, String queueUrl) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		return client.receiveMessage(receiveMessageRequest).getMessages();
	}

	public static DeleteMessageResult deleteMessage(AmazonSQS client, String queueUrl, List<Message> messages, int index) {
		String messageRecieptHandle = messages.get(index).getReceiptHandle();
		DeleteMessageRequest dmr = new DeleteMessageRequest(queueUrl, messageRecieptHandle);
		LOG.debug("Deleted message: " + messageRecieptHandle + " from queue: " + queueUrl);
		return client.deleteMessage(dmr);
	}	
	
	public static DeleteQueueResult deleteQueue(AmazonSQS client, String queueUrl) {
		DeleteQueueRequest dqr = new DeleteQueueRequest(queueUrl);
		LOG.debug("Deleted queue: " + queueUrl);
		return client.deleteQueue(dqr);
	}
	
	public static void printQueueNames(AmazonSQS client) {
		for (String queueUrl : client.listQueues().getQueueUrls()) {
			LOG.debug("QueueUrl: " + queueUrl);
		}
	}
	public static void prettyPrint(List<Message> messages) {
		for (Message message : messages) {
			LOG.debug("  Message");
			LOG.debug("    MessageId:     " + message.getMessageId());
			LOG.debug("    ReceiptHandle: " + message.getReceiptHandle());
			LOG.debug("    MD5OfBody:     " + message.getMD5OfBody());
			LOG.debug("    Body:          " + message.getBody());
			for (Entry<String, String> entry : message.getAttributes().entrySet()) {
				LOG.debug("  Attribute");
				LOG.debug("    Name:  " + entry.getKey());
				LOG.debug("    Value: " + entry.getValue());
			}
		}
		LOG.debug("\n");
	}

}
