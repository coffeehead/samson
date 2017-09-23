package org.coffeehead.aws.samson;

/*
 * This is my own version of the sample at
 * http://docs.aws.amazon.com/AWSToolkitEclipse/latest/GettingStartedGuide/tke_java_apps.html
 * 
 * It has some utility methods and explains what's going on under the covers.
 * 
 * If you run this sample application repeatedly, you should wait at least 60 seconds between subsequent runs. 
 * Amazon SQS requires that at least 60 seconds elapse after deleting a queue before creating a queue with the 
 * same name.
 */

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer account, and be signed up to use Amazon SQS. For more information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * WANRNING:</b> To avoid accidental leakage of your credentials, DO NOT keep the credentials file in your source directory.
 */

public class SQSRunner {

	private static final String AWS_DEFAULT_USER_PROFILE = "default";
	private static final Regions DEFAULT_REGION = Regions.US_EAST_1;
	private static final String QUEUE_NAME = "SQSSampleQueue";
	private static final String SAMPLE_MESSAGE = "Sample Message";
	private static final int FIRST = 0;

	static final Logger LOG = LoggerFactory.getLogger(SQSRunner.class);

	public static void main(String[] args) throws Exception {
		/*
			The old constructors are deprecated 
			see: https://aws.amazon.com/blogs/developer/client-constructors-now-deprecated/
			AmazonSQS client = new AmazonSQSClient(getCredentials(AWS_DEFAULT_USER_PROFILE));
			client.setRegion(Region.getRegion(DEFAULT_REGION));
		*/
		
		// The default client uses the config and credential files in the local AWS profile
		// see: http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
		// This is the same as AmazonSQSClientBuilder.standard().build()
		// AmazonSQS client = AmazonSQSClientBuilder.defaultClient();
		
		// setup
		AmazonSQS client = getClient();
		
		try {
			String sampleQueue = SQS.createQueue(client, QUEUE_NAME);
			SQS.printQueueNames(client);
			SQS.sendMessage(client, sampleQueue, SAMPLE_MESSAGE);
			List<Message> messages = SQS.receiveMessage(client, sampleQueue);
			SQS.prettyPrint(messages);
			SQS.deleteMessage(client, sampleQueue, messages, FIRST);
			SQS.deleteQueue(client, sampleQueue);
		} catch (AmazonServiceException ase) {
			LOG.error("Caught an AmazonServiceException, which means your request made it " 
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			LOG.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not "
					+ "being able to access the network.");
			LOG.error("Error Message: " + ace.getMessage());
		}
	}

	private static AmazonSQS getClient() {
		AmazonSQS client = AmazonSQSClientBuilder.standard()
                .withRegion(DEFAULT_REGION)
                .withCredentials(getCredentials(AWS_DEFAULT_USER_PROFILE))
                .build();
		return client;
	}

	private static AWSCredentialsProvider getCredentials(String profileName) {
		// The ProfileCredentialsProvider will look in your default location for your credentials
		// http://java.awsblog.com/post/TxRE9V31UFN860/Secure-Local-Development-with-the-ProfileCredentialsProvider

		AWSCredentialsProvider credentials = null;
		try {
			credentials = new ProfileCredentialsProvider(profileName);
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file.", e);
		}
		return credentials;
	}
}
