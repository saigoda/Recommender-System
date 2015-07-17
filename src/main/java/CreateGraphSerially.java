import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class CreateGraphSerially {

	/**
	 * @param args
	 */
	private static String _dbName = "";
	private static String _host = "";
	private static long _userCounter = 1;
	private static long _itemCounter = 1;
	private static long _duplicateCounter = 1;
	private static long _edgeCounter = 1;

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Fourth is the database name\n"
							+ "Fifth is the name of the host that we are storing the data");
		} else {
			if (args[1].trim().equalsIgnoreCase("local")) {
				_host = "localhost";
			} else {
				_host = "theia,titan01,titan02,titan04,titan05,titan06,titan07,titan08,titan09,titan10";
			}
			_dbName = args[0].trim();
			System.out.println("Host is: " + _host);
			TitanGraph g = createGraph();
			System.out.println("About to add the user vertices");
			addVertices("/homes/saibharath/TitanTestData/users", g);
			System.out.println("About to add the item vertices");
			addVertices("/homes/saibharath/TitanTestData/items", g);
			System.out
					.println("finished adding vertices. Starting to add edges");
			addEdges("/homes/saibharath/TitanTestData/labelDist", g);
			System.out
					.println("Finished adding edges as well!! Printing vertices now");
			printVertices(g);
			System.out.println("Done!Phew");
			System.out.println("I have successfully added "
					+ (_userCounter + _itemCounter) + " vertices and "
					+ _edgeCounter + " edges and there were "
					+ _duplicateCounter + " duplicates");
			g.shutdown();
		}
	}

	private static void printVertices(final TitanGraph g) {
		try {
			/*
			 * org.apache.commons.configuration.Configuration titanConf = new
			 * BaseConfiguration(); titanConf.setProperty("storage.backend",
			 * "hbase"); titanConf.setProperty("storage.tablename", _dbName);
			 * titanConf.setProperty("storage.hostname", _host);
			 */

			// g = TitanFactory.open(titanConf);

			/* Printing the newly added vertex */
			Iterator<Vertex> it = g.getVertices().iterator();

			while (it.hasNext()) {
				Vertex v1 = it.next();
				System.out.println("type: " + v1.getProperty("type"));
				System.out.println("myID: " + v1.getProperty("MyID"));
			}
			g.shutdown();
		} catch (Exception e) {
			g.shutdown();

			e.printStackTrace();
		}

	}

	private static void addEdges(String path, final TitanGraph g)
			throws Exception {
		File folder = new File(path);
		for (File userFile : folder.listFiles()) {
			if (!userFile.isDirectory()) {
				BufferedReader br = new BufferedReader(new FileReader(userFile));
				String str = "";
				while ((str = br.readLine()) != null) {
					System.out.println(str);
					String[] splits = str.split(",");
					String sourceUser = splits[0].trim() + "_user";
					String destItem = splits[1].trim() + "_item";
					Vertex source = g.getVertices("MyID", sourceUser)
							.iterator().next();
					Vertex dest = g.getVertices("MyID", destItem).iterator()
							.next();
					double edgeCost = Double.parseDouble(splits[2].trim());
					Edge edge = g.addEdge(null, source, dest, "click_injLabel");
					edge.setProperty("labelDist_inj", edgeCost);
					_edgeCounter++;
					if (_edgeCounter % 10000 == 0) {
						System.out.println("Commiting :" + _edgeCounter
								+ " edges");
						g.commit();
					}
				}
				br.close();
			}
		}
		System.out.println("Commiting :" + _edgeCounter + " edges");
		g.commit();
	}

	private static void addVertices(String path, final TitanGraph g)
			throws Exception {
		File folder = new File(path);
		for (File userFile : folder.listFiles()) {
			if (!userFile.isDirectory()) {
				BufferedReader br = new BufferedReader(new FileReader(userFile));
				String str = "";
				while ((str = br.readLine()) != null) {
					System.out.println(str);
					String typeString = str.substring(str.lastIndexOf("_") + 1);
					Iterator<Vertex> it = g.getVertices("MyID", str).iterator();
					if (!it.hasNext()) {
						Vertex v = g.addVertex(str);
						v.setProperty("MyID", str);
						v.setProperty("type", typeString);
						if (typeString.equalsIgnoreCase("user")) {
							_userCounter++;
						} else {
							_itemCounter++;
						}
						if (_userCounter % 5000 == 0
								|| _itemCounter % 5000 == 0) {
							System.out.println("Comminting :" + _userCounter
									+ " users and " + _itemCounter + " items");
							g.commit();
						}
					} else {
						System.out.println("Found a duplicate entry!!" + str);
						_duplicateCounter++;
					}
				}
				br.close();
			}

		}
		System.out.println("Comminting :" + _userCounter + " users and "
				+ _itemCounter + " items");
		g.commit();
	}

	private static TitanGraph createGraph() {
		TitanGraph g = null;
		try {
			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();

			/* Set the properties for this configuration */
			int bufferSize = 10000;
			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", _dbName);
			titanConf.setProperty("storage.hostname", _host);
			titanConf.setProperty("storage.batch-loading", true);
			titanConf.setProperty("ids.block-size", 1500000);
			titanConf.setProperty("persist-wait-time", 300000);
			titanConf.setProperty("persist-attempts", 100);
			titanConf.setProperty("storage.idauthority-block-size", 1500000);
			titanConf.setProperty("storage.buffer-size", bufferSize);
			titanConf.setProperty("storage.lock-wait-time", 20000);
			titanConf.setProperty("storage.idauthority-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-retries", "10");
			titanConf.setProperty("ids.renew-timeout", "2000000");

			g = TitanFactory.open(titanConf);

			/*
			 * Set the Vertex Properties. 1. MyID - Primary - long 2. Type -
			 * String 3. distSum - double
			 */
			/*g.makeKey("MyID").dataType(String.class).indexed(Vertex.class)
					.unique().make();
			g.makeKey("type").dataType(String.class).make();
			 Set the edge labels and properties 
			g.makeKey("labelDist_inj").dataType(FullDouble.class)
					.indexed(Edge.class).make();
			g.makeKey("labelDist_est").dataType(FullDouble.class).make();
			g.makeLabel("click_injLabel").make();
			g.makeLabel("click_estLabel").make();
			System.out.println("Made keys and lables");*/
			/* Adding Dummy Vertex */

			// String dummyID = "__DUMMY__";
			/*
			 * Vertex v = g.addVertex(dummyID); v.setProperty("type", "dummy");
			 * v.setProperty("MyID", dummyID);
			 * 
			 * /* Printing the newly added vertex Vertex v1 =
			 * g.getVertices("MyID", dummyID).iterator().next();
			 * System.out.println("type: " + v1.getProperty("type"));
			 * System.out.println("myID: " + v1.getProperty("MyID"));
			 */
			g.commit();
		} catch (Exception e) {
			e.printStackTrace();
			g.shutdown();
		}
		return g;
	}

}
