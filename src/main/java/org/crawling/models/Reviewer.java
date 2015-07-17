package org.crawling.models;

public class Reviewer {
	
	private String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getExpertise() {
		return expertise;
	}

	public void setExpertise(String expertise) {
		this.expertise = expertise;
	}


	private String expertise;
	
	private Location homeTown;
	
	private Location currentTown;

	public Location getHomeTown() {
		return homeTown;
	}

	public void setHomeTown(Location homeTown) {
		this.homeTown = homeTown;
	}

	public Location getCurrentTown() {
		return currentTown;
	}

	public void setCurrentTown(Location currentTown) {
		this.currentTown = currentTown;
	}

}
