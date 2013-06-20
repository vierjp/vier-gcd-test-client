import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.datastore.DatastoreV1.BlindWriteRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreOptions;

public class ClientTest4 {
	private static final Logger logger = Logger.getLogger(ClientTest4.class.getName());
	public static final String[] SCOPES = new String[] { "https://www.googleapis.com/auth/datastore",
			"https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/bigquery" };

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: ClientTest <DATASET_ID>");
			System.exit(1);
		}

		logger.info("DATASTORE_DATASET:" + System.getenv("DATASTORE_DATASET"));
		logger.info("DATASTORE_HOST:" + System.getenv("DATASTORE_HOST"));
		logger.info("DATASTORE_SERVICE_ACCOUNT:" + System.getenv("DATASTORE_SERVICE_ACCOUNT"));
		logger.info("DATASTORE_PRIVATE_KEY_FILE:" + System.getenv("DATASTORE_PRIVATE_KEY_FILE"));

		// Set the dataset from the command line parameters.
		String datasetId = args[0];
		Datastore datastore = null;
		try {
			// DatastoreOptions.Builder builder =
			// DatastoreHelper.getOptionsfromEnv();
			DatastoreOptions.Builder builder = getOptionsfromEnv();

			DatastoreOptions options = builder.dataset(datasetId).build();
			logger.info("options.getHost():" + options.getHost());
			logger.info("options.getDataset():" + options.getDataset());

			datastore = DatastoreFactory.get().create(options);

			Bigquery bigquery = new Bigquery.Builder(new NetHttpTransport(), new JacksonFactory(),
					options.getCredential()).setApplicationName("BigQuery-Service-Accounts/0.1")
					.setHttpRequestInitializer(options.getCredential()).build();
			Datasets.List datasetRequest = bigquery.datasets().list("publicdata");
			DatasetList datasetList = datasetRequest.execute();
			System.out.format("%s\n", datasetList.toPrettyString());

		} catch (GeneralSecurityException exception) {
			System.err.println("Security error connecting to the datastore: " + exception.getMessage());
			System.exit(1);
		} catch (IOException exception) {
			System.err.println("I/O error connecting to the datastore: " + exception.getMessage());
			System.exit(1);
		}

		try {
			// Create an RPC request to write mutations outside of a
			// transaction.
			BlindWriteRequest.Builder req = BlindWriteRequest.newBuilder();
			// Create a new entity.
			Entity.Builder entity = Entity.newBuilder();
			// Set the entity key with only one `path_element`: no parent.
			Key.Builder key = Key.newBuilder().addPathElement(
					Key.PathElement.newBuilder().setKind("Trivia").setName("hgtg"));
			entity.setKey(key);
			// Add two entity properties:
			// - a utf-8 string: `question`
			entity.addProperty(Property.newBuilder().setName("question")
					.addValue(Value.newBuilder().setStringValue("Meaning of Life?")));
			// - a 64bit integer: `answer`
			entity.addProperty(Property.newBuilder().setName("answer").addValue(Value.newBuilder().setIntegerValue(42)));
			// Add mutation to the request that update or insert this entity.
			req.getMutationBuilder().addUpsert(entity);
			// Execute the RPC synchronously and ignore the response.
			datastore.blindWrite(req.build());
			// Create an RPC request to get entities by key.
			LookupRequest.Builder lreq = LookupRequest.newBuilder();
			// Add one key to lookup the same entity.
			lreq.addKey(key);
			// Execute the RPC and get the response.
			LookupResponse lresp = datastore.lookup(lreq.build());
			// Found one entity result.
			Entity entityFound = lresp.getFound(0).getEntity();
			// Get `question` property value.
			String question = entityFound.getProperty(0).getValue(0).getStringValue();
			// Get `answer` property value.
			Long answer = entityFound.getProperty(1).getValue(0).getIntegerValue();
			System.out.println(question);
			// String result = System.console().readLine("> ");
			String result = "11";
			if (result.equals(answer.toString())) {
				System.out.println("fascinating, extraordinary and,"
						+ "when you think hard about it, completely obvious.");
			} else {
				System.out.println("Don't Panic!");
			}
		} catch (DatastoreException exception) {
			// Catch all Datastore rpc errors.
			System.err.println("Error while doing datastore operation");
			// Log the exception, the name of the method called and the error
			// code.
			System.err.println(String.format("DatastoreException(%s): %s %s", exception.getMessage(),
					exception.methodName, exception.code));

			logger.log(Level.SEVERE, "error", exception);
			System.exit(1);
		}
	}

	public static DatastoreOptions.Builder getOptionsfromEnv() throws GeneralSecurityException, IOException {
		logger.info("DATASTORE_DATASET:" + System.getenv("DATASTORE_DATASET"));
		logger.info("DATASTORE_HOST:" + System.getenv("DATASTORE_HOST"));
		logger.info("DATASTORE_SERVICE_ACCOUNT:" + System.getenv("DATASTORE_SERVICE_ACCOUNT"));
		logger.info("DATASTORE_PRIVATE_KEY_FILE:" + System.getenv("DATASTORE_PRIVATE_KEY_FILE"));
		DatastoreOptions.Builder options = new DatastoreOptions.Builder();
		options.dataset(System.getenv("DATASTORE_DATASET"));
		options.host(System.getenv("DATASTORE_HOST"));

		if (System.getenv("DATASTORE_SERVICE_ACCOUNT") != null && System.getenv("DATASTORE_PRIVATE_KEY_FILE") != null) {
			Credential credential = getServiceAccountCredential(System.getenv("DATASTORE_SERVICE_ACCOUNT"),
					System.getenv("DATASTORE_PRIVATE_KEY_FILE"));
			logger.info("Using JWT Service Account credential.");
			options.credential(credential);
		}

		return options;
	}

	public static Credential getServiceAccountCredential(String account, String privateKeyFile)
			throws GeneralSecurityException, IOException {
		NetHttpTransport transport = new NetHttpTransport();
		JacksonFactory jsonFactory = new JacksonFactory();
		return new GoogleCredential.Builder().setTransport(transport).setJsonFactory(jsonFactory)
				.setServiceAccountId(account).setServiceAccountScopes(SCOPES)
				.setServiceAccountPrivateKeyFromP12File(new File(privateKeyFile)).build();
	}
}
