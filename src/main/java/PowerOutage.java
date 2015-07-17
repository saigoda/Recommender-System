import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

public class PowerOutage {

    public static HashMap<Integer, HashMap<Integer, Integer>> adjacencyList = new HashMap<Integer, HashMap<Integer, Integer>>();

    public static void buildList(int[] from, int[] to, int[] len) {
        for (int i = 0; i < from.length; i++) {
            if (from[i] < 0 && from[i] > 49)
                System.exit(0);
            HashMap<Integer, Integer> edgeVal = null;
            if (!adjacencyList.containsKey(from[i])) {
                edgeVal = new HashMap<Integer, Integer>();
                edgeVal.put(to[i], len[i]);
            } else {
                edgeVal = adjacencyList.get(from[i]);
                edgeVal.put(to[i], len[i]);
            }
            adjacencyList.put(from[i], edgeVal);
        }
    }

    public static int estimateTimeOut(int[] fromJunction, int[] toJunction,
            int[] ductLength) {
        if (fromJunction.length == 0 || fromJunction.length > 50
                || toJunction.length == 0 || toJunction.length > 50
                || ductLength.length == 0 || ductLength.length > 50
                || fromJunction.length != toJunction.length
                || fromJunction.length != ductLength.length) {
            System.exit(0);
        }
        buildList(fromJunction, toJunction, ductLength);
        // int[] min_max = findMinMax();
        Stack<Integer> dfs = new Stack<Integer>();
        String path = "";
        dfs.push(0);
        int parent = 0;
        int minutesTaken = 0;
        while (!dfs.isEmpty()) {
            int element = dfs.pop();
            path += (element + " ");
            HashMap<Integer, Integer> edges = adjacencyList.get(element);
            if (edges != null) {
                parent = element;
                Iterator<Integer> keys = edges.keySet().iterator();
                while (keys.hasNext()) {
                    int edge = keys.next();
                    int length = edges.get(edge);
                    dfs.push(edge);
                    minutesTaken += length;
                }
            } else {
                if (dfs.size() >= 1)
                    minutesTaken += adjacencyList.get(parent).get(element);
            }
        }
        System.out.println(path);
        return minutesTaken;
    }

    private static int[] findMinMax() {
        Iterator<Integer> keys = adjacencyList.keySet().iterator();
        int[] max_min = new int[2];
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        while (keys.hasNext()) {
            int key = keys.next();
            if (min >= key)
                min = key;
            if (max <= key)
                max = key;
            HashMap<Integer, Integer> edges = adjacencyList.get(key);
            Iterator<Integer> edgeKeys = edges.keySet().iterator();
            while (edgeKeys.hasNext()) {
                int edgeKey = edgeKeys.next();
                if (min >= edgeKey)
                    min = edgeKey;
                if (max <= edgeKey)
                    max = edgeKey;
            }
        }
        max_min[0] = min;
        max_min[1] = max;
        System.out.println(min);
        System.out.println(max);
        return max_min;
    }

    public static void main(String[] args) {
        int[] from = { 0, 0, 0, 1, 4 };
        int[] to = { 1, 3, 4, 2, 5 };
        int[] len = { 10, 10, 100, 10, 5 };
        System.out.println(estimateTimeOut(from, to, len));
    }
}