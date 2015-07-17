package CreateGraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class PriorityTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		/*
		 * String s = "a:10,b:20,c:5,d:1,e:2,f:3";
		 * 
		 * TreeSet<String> queue = new TreeSet<String>(new Comparator<String>()
		 * {
		 * 
		 * @Override public int compare(String s1, String s2) { double d1 =
		 * Double.parseDouble(s1.split(":")[1]); double d2 =
		 * Double.parseDouble(s2.split(":")[1]); return d1 > d2 ? -1 : d1 == d2
		 * ? 0 : 1; } });
		 * 
		 * String[] splits = s.split(",");
		 * 
		 * for (String s1 : splits) { queue.add(s1); } Iterator<String> it =
		 * queue.iterator(); while(it.hasNext()){ System.out.println(it.next());
		 * } /*String y = ",";
		 * System.out.println("y's length is "+y.split(",").length);
		 * StringBuilder sb = new StringBuilder(); String[] keys = new
		 * String[10]; keys = queue.toArray(keys); String[] splits1 =
		 * s.split("\\t"); System.out.println(splits1[1].split("\\|").length);
		 * /*for(int i=0;i<10&&i<splits.length;i++)
		 * sb.append(queue.remove()+","); System.out.println(sb.toString());
		 */
		/*
		 * HashMap<String, Double> orderedPairs = new HashMap<String,Double>();
		 * HashMap<String, Double> weights = new HashMap<String,Double>();
		 * String[] ingredients = { "a", "b", "c", "d", "e" }; double i1 = 1;
		 * for (String ingredient : ingredients) {
		 * 
		 * weights.put(ingredient, i1); i1++; } for (String ingredient :
		 * ingredients) { String ingNum = ingredient; for (int i = 0; i <
		 * ingredients.length; i++) { String ing = ingredients[i]; if
		 * (ing.equals(ingredient)) continue; else {
		 * 
		 * String samePair = ingNum + "," + ing; String reversePair = ing + ","
		 * + ingNum; if (orderedPairs.containsKey(samePair) ||
		 * orderedPairs.containsKey(reversePair)) continue; else {
		 * orderedPairs.put(samePair, (weights.get(ingNum) * weights
		 * .get(ing))); }
		 * 
		 * } } } HashMap<String, Double> docLength = new HashMap<String,
		 * Double>(); double k=10.2345; char ch='a'; for(int i=0;i<5;i++){
		 * docLength.put(ch+"", k); k++; ch++; } Iterator<String> it =
		 * orderedPairs.keySet().iterator(); int i=1; while (it.hasNext()) {
		 * String key1 = it.next(); String[] splits1 = key1.split(","); String
		 * val1 = splits1[0] + ":" + docLength.get(splits1[0]); String val2 =
		 * splits1[1] + ":" + docLength.get(splits1[1]); String updatedKey =
		 * val1 + "," + val2;
		 * System.out.println(i+".\t"+updatedKey+"\t"+orderedPairs.get(key1));
		 * i++; } List<String> list = new ArrayList<String>(); list.add(0,
		 * "Hi"); //list.remove(0); list.set(0,"helo");
		 * System.out.println(list.toString()); String[] splits =
		 * ",hi".split(","); System.out.println(splits[0].length());
		 */
		/*org.apache.http.client.HttpClient client = new DefaultHttpClient();
		
		try {
			HttpGet request = new HttpGet(
					"http://allrecipes.com/cook/10292860/profile.aspx");
			HttpResponse response = client.execute(request);
			BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			StringBuilder sb = new StringBuilder();
			String st = "";
			while((st = br.readLine()) != null){
				sb.append(st+"\n");
			}
			System.out.println(sb.toString());
		} catch (Exception e) {

		}*/
		char[] c={ 'A','n','d','r','&','e','a','c','u','t','e',';'};
		String s = new String(c,0,c.length);
		s= StringEscapeUtils.unescapeHtml4(s);
		String k = s;
		//BufferedWriter br = new BufferedWriter(new FileWriter(new File("hi.csv")));
		//br.write(s);
		//br.close();
		System.out.println(k);
	}

}
