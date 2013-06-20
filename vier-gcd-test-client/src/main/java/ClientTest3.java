import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.time.StopWatch;

import com.google.api.services.datastore.DatastoreV1.BlindWriteRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.PropertyOrder;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
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
public class ClientTest3 {
	private static final Logger logger = Logger.getLogger(ClientTest3.class.getName());

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
			//
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
		for (int i = 1; i <= 1000; i++) {
			try {
				// トランザクション外で更新するためのRPC requestを作成する
				BlindWriteRequest.Builder req = BlindWriteRequest.newBuilder();
				// 新規Entityを作成する
				Entity.Builder entity = Entity.newBuilder();
				// 一つのPathElementでKeyを生成する (親Keyなし)
				Key.Builder key = Key.newBuilder().addPathElement(
						Key.PathElement.newBuilder().setKind("ClientTest3").setName("keyName" + i));
				entity.setKey(key);
				// 文字列
				entity.addProperty(Property.newBuilder().setName("str")
						.addValue(Value.newBuilder().setStringValue("string" + i)));
				// 数値
				entity.addProperty(Property.newBuilder().setName("number")
						.addValue(Value.newBuilder().setIntegerValue(i)));
				// 作成時刻
				entity.addProperty(Property.newBuilder().setName("createDate")
						.addValue(Value.newBuilder().setTimestampMicrosecondsValue(new Date().getTime() * 1000)));
				req.getMutationBuilder().addUpsert(entity);
				// putする
				datastore.blindWrite(req.build());

				logger.info("put done count:" + i);

			} catch (DatastoreException exception) {
				logger.log(Level.SEVERE, "error", exception);
			}
		}
		sw.stop();
		logger.info("put entities " + sw.getTime() + " milliseconds.");

		sw.reset();
		sw.start();

		try {
			// クエリする
			RunQueryRequest.Builder req = RunQueryRequest.newBuilder();
			Query.Builder queryBuilder = req.getQueryBuilder();
			queryBuilder.addKindBuilder().setName("ClientTest3");
			// 作成時刻の新しい順
			queryBuilder.addOrder(DatastoreHelper.makeOrder("createDate", PropertyOrder.Direction.DESCENDING));
			// limit 5件
			queryBuilder.setLimit(5);
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
		sw.stop();
		logger.info("query entities " + sw.getTime() + " milliseconds.");

	}
}
