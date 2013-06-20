import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.PropertyOrder;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;

/**
 * @see https://developers.google.com/datastore/docs/getstarted/start_java/
 * 
 */
public class ClientTest5 {
	private static final Logger logger = Logger.getLogger(ClientTest3.class.getName());

	// Load Client ID/secret from client_secrets.json file
	private static final String CLIENTSECRETS_LOCATION = "client_secrets.json";

	// Load Client ID/secret from client_secrets.json file
	private static final String REFLESH_TOKEN_FILE_NAME = "token.dat";

	private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: ClientTest <DATASET_ID>");
			System.exit(1);
		}

		logger.info("DATASTORE_DATASET:" + System.getenv("DATASTORE_DATASET"));
		logger.info("DATASTORE_HOST:" + System.getenv("DATASTORE_HOST"));
		logger.info("DATASTORE_SERVICE_ACCOUNT:" + System.getenv("DATASTORE_SERVICE_ACCOUNT"));
		logger.info("DATASTORE_PRIVATE_KEY_FILE:" + System.getenv("DATASTORE_PRIVATE_KEY_FILE"));

		String datasetId = args[0];
		Datastore datastore = null;
		try {
			DatastoreOptions.Builder builder = DatastoreHelper.getOptionsfromEnv();
			// DatastoreOptions.Builder builder = getOptionsfromEnv();

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

		StopWatch sw = new StopWatch();

		sw.start();
		for (int i = 1; i <= 50; i++) {
			try {
				// クエリする
				RunQueryRequest.Builder req = RunQueryRequest.newBuilder();
				Query.Builder queryBuilder = req.getQueryBuilder();
				queryBuilder.addKindBuilder().setName("ClientTest3");
				// 作成時刻の新しい順
				queryBuilder.addOrder(DatastoreHelper.makeOrder("createDate", PropertyOrder.Direction.DESCENDING));
				// limit 500件
				queryBuilder.setLimit(500);
				// クエリ実行
				RunQueryResponse res = datastore.runQuery(req.build());

				List<EntityResult> results = res.getBatch().getEntityResultList();
				for (EntityResult result : results) {
					Entity entity = result.getEntity();

					Map<String, Object> propertyMap = DatastoreHelper.getPropertyMap(entity);
					logger.info("Entity: keyName:" + entity.getKey().getPathElement(0).getName() + " str:"
							+ propertyMap.get("str") + " number:" + propertyMap.get("number") + " createDate:"
							+ propertyMap.get("createDate"));
				}

			} catch (DatastoreException exception) {
				logger.log(Level.SEVERE, "error", exception);
				System.exit(1);
			}
		}
		sw.stop();
		logger.info("query entities " + sw.getTime() + " milliseconds.");

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
		} else {
			credential = getAuthorizationCodeCredential();
			logger.info("Using AuthorizationCode Credential");
		}
		options.credential(credential);
		return options;
	}

	private static Credential getAuthorizationCodeCredential() throws IOException {

		InputStream resourceInputStream = ClientTest3.class.getResourceAsStream(CLIENTSECRETS_LOCATION);
		if (resourceInputStream == null) {
			System.err.println("「src/main/resources」に「" + CLIENTSECRETS_LOCATION + "」を配置する必要があります。");
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
	private static String loadRefleshToken(String path) {
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
	private static void saveRefleshToken(String path, String refleshToken) throws IOException {
		FileUtils.writeStringToFile(new File(path), refleshToken, "UTF-8");
	}

}
