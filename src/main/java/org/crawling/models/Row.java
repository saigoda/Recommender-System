package org.crawling.models;

import java.util.ArrayList;
import java.util.List;

public class Row {
	
	private List<Element> elements;
	

	public List<Element> getElements() {
		return elements;
	}


	public void setElements(List<Element> elements) {
		this.elements = elements;
	}


	public Row() {
		elements = new ArrayList<Element>();
	}

}
