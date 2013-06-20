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
public class ClientTest6 {
	private static final Logger logger = Logger.getLogger(ClientTest6.class.getName());

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: ClientTest <DATASET_ID>");
			System.exit(1);
		}

		String datasetId = args[0];
		Datastore datastore = null;
		try {
			DatastoreOptions.Builder builder = DatastoreHelper.getOptionsfromEnv();
			DatastoreOptions options = builder.dataset(datasetId).build();
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
				// Create an RPC request to write mutations outside of a
				// transaction.
				BlindWriteRequest.Builder req = BlindWriteRequest.newBuilder();
				// Create a new entity.
				Entity.Builder entity = Entity.newBuilder();
				// Set the entity key with only one `path_element`: no parent.
				Key.Builder key = Key.newBuilder().addPathElement(
						Key.PathElement.newBuilder().setKind("ClientTest3").setName("keyName" + i));
				entity.setKey(key);
				// Add three entity properties:
				// - a utf-8 string: `str`
				entity.addProperty(Property.newBuilder().setName("str")
						.addValue(Value.newBuilder().setStringValue("string" + i)));
				// - a 64bit integer: `number`
				entity.addProperty(Property.newBuilder().setName("number")
						.addValue(Value.newBuilder().setIntegerValue(i)));
				// - a date: `createDate`
				entity.addProperty(Property.newBuilder().setName("createDate")
						.addValue(Value.newBuilder().setTimestampMicrosecondsValue(new Date().getTime() * 1000)));
				req.getMutationBuilder().addUpsert(entity);
				// Execute the RPC synchronously and ignore the response.
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
			// Create an RPC request to query
			RunQueryRequest.Builder req = RunQueryRequest.newBuilder();
			Query.Builder queryBuilder = req.getQueryBuilder();
			queryBuilder.addKindBuilder().setName("ClientTest3");
			// order by createDate
			queryBuilder.addOrder(DatastoreHelper.makeOrder("createDate", PropertyOrder.Direction.DESCENDING));
			// limit 10
			queryBuilder.setLimit(10);
			// run query
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
