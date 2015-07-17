package org.crawling.models;

import java.util.ArrayList;
import java.util.List;

public class MapResult {
	
	private List<String> destination_addresses;
	
	private List<String> origin_addresses;
	
	private Row rows;
	
	public Row getRows() {
		return rows;
	}

	public void setRows(Row rows) {
		this.rows = rows;
	}

	public MapResult(){
		rows = new Row();
		origin_addresses = new ArrayList<String>();
		destination_addresses = new ArrayList<String>();
	}

	public List<String> getDestination_addresses() {
		return destination_addresses;
	}

	public void setDestination_addresses(List<String> destination_addresses) {
		this.destination_addresses = destination_addresses;
	}

	public List<String> getOrigin_addresses() {
		return origin_addresses;
	}

	public void setOrigin_addresses(List<String> origin_addresses) {
		this.origin_addresses = origin_addresses;
	}

	

}
