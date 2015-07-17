package org.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Node {

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    /**
     * @param args
     */
    public static enum Color {
        WHITE, GRAY, BLACK
    }

    private String id;
    private List<String> edges = new ArrayList<String>();
    private int distance;
    private double score;

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    private Color color = Color.WHITE;
    private String parent;

    public Node() {
        edges = new ArrayList<String>();
        color = Color.WHITE;
        distance = Integer.MAX_VALUE;
        parent = null;
    }

    public Node(String nodeInfo) {
        System.out.println(nodeInfo);
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
        System.out.println("tokens[0]=" + tokens[0]);
        for (String s : tokens[0].split(",")) {
            if (s.length() > 0) {
                edges.add(s);
            }
        }
        if (tokens[1].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            this.distance = Integer.parseInt(tokens[1]);
        }
        this.color = Color.valueOf(tokens[2]);
        this.parent = tokens[3];
    }

    public Text getNodeInfo() {
        StringBuffer buffer = new StringBuffer();
        for (String edge : edges)
            buffer.append(edge + ",");
        buffer.append("|");
        if (this.distance < Integer.MAX_VALUE) {
            buffer.append(this.distance).append("|");
        } else {
            buffer.append("Integer.MAX_VALUE").append("|");
        }
        buffer.append(color.toString() + "|");
        buffer.append(parent);
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

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

}
