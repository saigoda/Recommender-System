package org.graph;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Node {

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    /**
     * @param args
     */
    public static enum Color {
        WHITE, GRAY, BLACK
    }

    private String id;
    private List<String> edges = new ArrayList<String>();
    private String distance;

    private String color = "WHITE";
    private String parent;

    private String score;

    public Node() {// By default we set the color of the node as WHITE and edges
                   // empty
        edges = new ArrayList<String>();
        color = "WHITE";
        distance = "Integer.MAX_VALUE";
        parent = null;
    }

    /*
     * 
     * Input format fo this constructor
     * 
     * <KEY>\t<neighbor1>,<neighbor1>,<neighbor1>,<neighbor1>,|distance|Color|Parent
     */
    public Node(String nodeInfo) {
        // System.out.println(nodeInfo);
        String[] input = nodeInfo.trim().split("\\t");
        String key = "", value = "";
        try {
            key = input[0];
            value = input[1];
        } catch (Exception e) {
            e.printStackTrace();
        }
        String[] tokens = value.split("\\|");
        this.id = key;
        for (String s : tokens[0].split(",")) {
            if (s.length() > 0) {
                edges.add(s);
            }
        }
        if (tokens[1].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE + "";
        } else {
            this.distance = tokens[1];
        }
        this.color = tokens[2];
        this.parent = tokens[3];
        this.score = tokens[4];
    }

    /*
     * 
     * Prints all the node information except the key
     */
    public Text getNodeInfo() {
        StringBuffer buffer = new StringBuffer();
        for (String edge : this.edges)
            buffer.append(edge + ",");
        buffer.append("|");
        if (!distance.equals("Integer.MAX_VALUE")) {
            buffer.append(this.distance).append("|");
        } else {
            buffer.append("Integer.MAX_VALUE").append("|");
        }
        buffer.append(color.toString() + "|");
        buffer.append(parent + "|");
        buffer.append(score);
        return new Text(buffer.toString());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getEdges() {
        return edges;
    }

    public void setEdges(List<String> edges) {
        this.edges = edges;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

}
