import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
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
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;

/**
 * @see https://developers.google.com/datastore/docs/getstarted/start_java/
 * 
 */
public class ClientTest2 {
	private static final Logger logger = Logger.getLogger(ClientTest2.class.getName());

	// Load Client ID/secret from client_secrets.json file
	private static final String CLIENTSECRETS_LOCATION = "client_secrets.json";

	// Load Client ID/secret from client_secrets.json file
	private static final String REFLESH_TOKEN_FILE_NAME = "token.dat";

	private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
	public static final String[] SCOPES = new String[] { "https://www.googleapis.com/auth/datastore",
			"https://www.googleapis.com/auth/userinfo.email" };

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
			// Setup the connection to Google Cloud Datastore and infer
			// credentials
			// from the environment.
			// DatastoreOptions.Builder builder =
			// DatastoreHelper.getOptionsfromEnv();
			DatastoreOptions.Builder builder = getOptionsfromEnv();

			DatastoreOptions options = builder.dataset(datasetId).build();
			logger.info("options.getHost():" + options.getHost());
			logger.info("options.getDataset():" + options.getDataset());

			datastore = DatastoreFactory.get().create(options);

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
		Credential credential = DatastoreHelper.getComputeEngineCredential();
		if (credential != null) {
			logger.info("Using Compute Engine credential.");
		} else if (System.getenv("DATASTORE_SERVICE_ACCOUNT") != null
				&& System.getenv("DATASTORE_PRIVATE_KEY_FILE") != null) {
			// credential =
			// getServiceAccountCredential(System.getenv("DATASTORE_SERVICE_ACCOUNT"),
			// System.getenv("DATASTORE_PRIVATE_KEY_FILE"));
			// logger.info("Using JWT Service Account credential.");
		}
		if (credential == null) {
			credential = getCredential();
			logger.info("My credential.");
		}
		options.credential(credential);
		return options;
	}

	public static Credential getServiceAccountCredential(String account, String privateKeyFile)
			throws GeneralSecurityException, IOException {
		NetHttpTransport transport = new NetHttpTransport();
		JacksonFactory jsonFactory = new JacksonFactory();
		return new GoogleCredential.Builder().setTransport(transport).setJsonFactory(jsonFactory)
				.setServiceAccountId(account).setServiceAccountScopes(Arrays.asList(SCOPES))
				.setServiceAccountPrivateKeyFromP12File(new File(privateKeyFile)).build();
	}

	/**
	 * 
	 * @param clientSecrets
	 * @return
	 * @throws IOException
	 */
	public static Credential getCredential() throws IOException {

		// Sample通りに「getResourceAsStream」の返り値のInputStreamをそのまま使ったところ、
		// 「GoogleClientSecrets.load」でエラーになってしましました。
		// そのためちょっと変なコードになってますが、とりあえずこれで動きました。
		InputStream resourceInputStream = ClientTest2.class.getResourceAsStream(CLIENTSECRETS_LOCATION);
		if (resourceInputStream == null) {
			System.err.println("「src/main/resources/jp/vier/sample/bigquery」以下に「" + CLIENTSECRETS_LOCATION
					+ "」を配置する必要があります。");
			System.exit(1);
		}
		String jsonString = IOUtils.toString(resourceInputStream);
		InputStream stream = new ByteArrayInputStream(jsonString.getBytes("UTF-8"));

		// ClientSecretを取得する
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(new JacksonFactory(),
				new InputStreamReader(stream));

		// 既存のリフレッシュトークンの取得を試みる
		String storedRefreshToken = loadRefleshToken(REFLESH_TOKEN_FILE_NAME);

		// リフレッシュトークンの有無をチェックして、取得できなければOAuthの認可フローを開始する
		if (storedRefreshToken == null) {
			// Create a URL to request that the user provide access to the
			// BigQuery API
			String authorizeUrl = new GoogleAuthorizationCodeRequestUrl(clientSecrets, REDIRECT_URI,
					Collections.singleton(DatastoreOptions.SCOPE)).build();

			// Prompt the user to visit the authorization URL, and retrieve the
			// provided authorization code
			System.out.println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl);
			System.out.println("... and paste the code you received here: ");
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			// ユーザーが入力した認可コード
			String authorizationCode = in.readLine();

			// Create a Authorization flow object
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(new NetHttpTransport(),
					new JacksonFactory(), clientSecrets, Arrays.asList(DatastoreOptions.SCOPE))
					.setAccessType("offline").setApprovalPrompt("force").build();
			// Exchange the auth code for an access token and refesh token
			GoogleTokenResponse response = flow.newTokenRequest(authorizationCode).setRedirectUri(REDIRECT_URI)
					.execute();
			Credential credential = flow.createAndStoreCredential(response, null);

			// リフレッシュトークンをファイルに保存
			saveRefleshToken(REFLESH_TOKEN_FILE_NAME, credential.getRefreshToken());

			return credential;

			// リフレッシュトークンを取得できた場合
		} else {
			// リフレッシュトークンを使って新しいアクセストークンを取得する
			GoogleCredential credential = new GoogleCredential.Builder().setTransport(new NetHttpTransport())
					.setJsonFactory(new JacksonFactory()).setClientSecrets(clientSecrets).build()
					.setFromTokenResponse(new TokenResponse().setRefreshToken(storedRefreshToken));
			credential.refreshToken();

			return credential;
		}
	}

	/**
	 * RefleshTokenを取得する
	 * 
	 * @param path
	 * @return
	 */
	public static String loadRefleshToken(String path) {
		try {
			String refleshToken = FileUtils.readFileToString(new File(path), "UTF-8");
			return refleshToken;
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * RefleshTokenを保存する
	 * 
	 * @param path
	 * @param refleshToken
	 * @return
	 */
	public static void saveRefleshToken(String path, String refleshToken) throws IOException {
		FileUtils.writeStringToFile(new File(path), refleshToken, "UTF-8");
	}

}
