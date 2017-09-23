package org.coffeehead.aws.samson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class SQSTest extends SQS {
	
	private static final String HOSTNAME="localhost";
	private static final int PORT=9324;
	private static final Regions DEFAULT_REGION = Regions.US_EAST_1;
	private static AmazonSQS client = null;
	private static SQSRestServer sqsServer = null;
	
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    	sqsServer = SQSRestServerBuilder
                .withInterface(HOSTNAME)
                .withPort(PORT)
                .start();
    	sqsServer.waitUntilStarted();
    	
		// initialize client
		client = getClient();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if(sqsServer != null)
        	sqsServer.stopAndWait();
        client = null;
    }	

	@Test(expected = AmazonSQSException.class)
	public void testCreateQueueWithSpaces() {
		String queueNameWithSpaces = "Sample Queue";
		createQueue(client, queueNameWithSpaces);
	}
    
	@Test
	public void testCreateQueue() {
		String queueName = "SQSSampleQueue";
		String queueUrl = createQueue(client, queueName);
		String expectedQueueUrl = "http://localhost:9324/queue/SQSSampleQueue";
		assertEquals("Queue URL doesn't match expected", expectedQueueUrl,queueUrl);
	}

	@Test
	public void testSendMessage() {
		String queueName = "SQSSampleQueue";
		String messageText = "Sample Message";
		String queueUrl = createQueue(client, queueName);
		SendMessageRequest smrq = new SendMessageRequest(queueUrl, messageText);
		assertEquals("Queue URL doesn't match expected", queueUrl, smrq.getQueueUrl());
		assertEquals("Message body doesn't match expected", messageText, smrq.getMessageBody());
		SendMessageResult smrt = client.sendMessage(smrq);
		assertNotNull(smrt);
	}

	@Test
	public void testReceiveMessage() {
		String queueName = "SQSSampleQueue";
		String messageText = "Sample Message";
		String queueUrl = createQueue(client, queueName);
		SendMessageRequest smrq = new SendMessageRequest(queueUrl, messageText);
		client.sendMessage(smrq);
		List<Message> messages = receiveMessage(client, queueUrl);
		assertEquals("Expected 1 messages", 1, messages.size());
		Message message = messages.get(0);
		assertEquals("Message body does not match expected", messageText, message.getBody());
	}

	@Test
	public void testDeleteMessage() {
		String queueName = "SQSSampleQueue";
		String messageText = "Sample Message";
		String queueUrl = createQueue(client, queueName);
		SendMessageRequest smrq = new SendMessageRequest(queueUrl, messageText);
		client.sendMessage(smrq);
		List<Message> messages = receiveMessage(client, queueUrl);
		// delete the first message
		DeleteMessageResult dmr = deleteMessage(client, queueUrl, messages, 0);	
		assertNotNull(dmr);
	}

	@Test
	public void testDeleteQueue() {
		String queueName = "SQSSampleQueue";
		String queueUrl = createQueue(client, queueName);
		DeleteQueueResult dqr = deleteQueue(client, queueUrl);
		assertNotNull(dqr);
	}

	private static AmazonSQS getClient() {
		final String endpoint = String.format("http://%s:%d", HOSTNAME, PORT);
		AmazonSQS client = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("fakeAccessId", "fakeSecretKey")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, DEFAULT_REGION.getName()))
                .build();
		return client;
	}
}
