import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class ClientTest {
	private static final Logger logger = Logger.getLogger(ClientTest.class.getName());

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: ClientTest <DATASET_ID>");
			System.exit(1);
		}

		// 環境変数をログ出力
		logger.info("DATASTORE_DATASET:" + System.getenv("DATASTORE_DATASET"));
		logger.info("DATASTORE_HOST:" + System.getenv("DATASTORE_HOST"));
		logger.info("DATASTORE_SERVICE_ACCOUNT:" + System.getenv("DATASTORE_SERVICE_ACCOUNT"));
		logger.info("DATASTORE_PRIVATE_KEY_FILE:" + System.getenv("DATASTORE_PRIVATE_KEY_FILE"));

		// DatasetID(ProjectID) を実行時引数から取得する
		String datasetId = args[0];
		Datastore datastore = null;
		try {
			// 生成したCredentialをセットした「DatastoreOptions」を取得する。
			// Compute Engine上て動作している場合はComputeEngine用のCredentialを、
			// それ以外の環境で、かつ環境変数が適切に設定されている場合は
			// Serivce Account FlowのCredentialを生成する。
			DatastoreOptions.Builder builder = DatastoreHelper.getOptionsfromEnv();

			DatastoreOptions options = builder.dataset(datasetId).build();
			logger.info("options.getHost():" + options.getHost());
			logger.info("options.getDataset():" + options.getDataset());

			datastore = DatastoreFactory.get().create(options);

		} catch (GeneralSecurityException exception) {
			logger.severe("Security error connecting to the datastore: " + exception.getMessage());
			System.exit(1);
		} catch (IOException exception) {
			logger.severe("I/O error connecting to the datastore: " + exception.getMessage());
			System.exit(1);
		}

		try {
			// トランザクション外で更新するためのRPC requestを作成する
			BlindWriteRequest.Builder req = BlindWriteRequest.newBuilder();
			// 新規Entityを作成する
			Entity.Builder entity = Entity.newBuilder();
			// 一つのPathElementでKeyを生成する (親Keyなし)
			Key.Builder key = Key.newBuilder().addPathElement(
					Key.PathElement.newBuilder().setKind("Trivia").setName("hgtg"));
			entity.setKey(key);
			// Entityに2つのPropertyを追加する
			// utf-8文字列の「question」
			entity.addProperty(Property.newBuilder().setName("question")
					.addValue(Value.newBuilder().setStringValue("Meaning of Life?")));
			// 64bit integerの「answer」を追加する
			entity.addProperty(Property.newBuilder().setName("answer").addValue(Value.newBuilder().setIntegerValue(42)));
			// update or insertする「mutation」として「upsert」を指定する
			// 他にも「update」「insert」「insertAutoId」「delete」「force」がある
			// (「force」はユーザーが設定したread-onlyを無視する)
			req.getMutationBuilder().addUpsert(entity);
			// 同期的にRPCを実行して結果を無視する
			// (「返り値を無視する」の意味であって、システムエラー時には例外が発生する)
			datastore.blindWrite(req.build());

			// KeyでEntityを「get」するための RPC リクエストを作成する
			LookupRequest.Builder lreq = LookupRequest.newBuilder();
			// 登録したEntityを「get」するためにKeyを一つ指定する
			// (おそらく複数指定するとbatch get)
			lreq.addKey(key);
			// RPCを実行して結果を取得する
			LookupResponse lresp = datastore.lookup(lreq.build());
			// 結果として一つのEntityを取得する.
			Entity entityFound = lresp.getFound(0).getEntity();
			// 「question」 propertyの値を取得する。
			String question = entityFound.getProperty(0).getValue(0).getStringValue();
			// 「answer」 propertyの値を取得する。
			Long answer = entityFound.getProperty(1).getValue(0).getIntegerValue();
			logger.info(question);
			// コンソールからの値入力待ち
			String result = System.console().readLine("> ");
			// 入力値とEntityから取得した「answer」(=42)が一致した場合
			if (result.equals(answer.toString())) {
				logger.info("fascinating, extraordinary and,when you think hard about it, completely obvious.");

				// 一致しなかった場合
			} else {
				logger.info("Don't Panic!");
			}
		} catch (DatastoreException exception) {
			// DatastoreAPI実行時の例外をcatchする.
			logger.severe("Error while doing datastore operation");
			logger.log(Level.SEVERE, "error", exception);
			System.exit(1);
		}
	}
}
