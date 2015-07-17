package org.crawling.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import com.google.gson.Gson;

public class Cook implements WritableComparable<Cook> {

	private String name;

	private String expertise;
	
	private List<String> cookingInterests;
	
	private List<String> hobbies;
	
	private int numberOfRecipesInRecipeBox;
	
	private List<String> namesOfRecipesInRecipeBox;
	
	private List<String> typesOfRecipesInRecipeBox;
	
	private List<Float> overallRatingToTheRecipe;
	
	private List<Integer> ratingGivenByUserToRecipe;
	
	private List<String> postedRecipe;

	public List<String> getPostedRecipe() {
		return postedRecipe;
	}

	public void setPostedRecipe(List<String> postedRecipe) {
		this.postedRecipe = postedRecipe;
	}

	public int compareTo(Cook o) {
		if (this.hashCode() == o.hashCode())
			return 0;
		else
			return -1;
	}

	public List<String> getCookingInterests() {
		return cookingInterests;
	}

	public String getExpertise() {
		return expertise;
	}

	public List<String> getHobbies() {
		return hobbies;
	}
	
	public String getName() {
		return name;
	}

	public List<String> getNamesOfRecipesInRecipeBox() {
		return namesOfRecipesInRecipeBox;
	}

	public int getNumberOfRecipesInRecipeBox() {
		return numberOfRecipesInRecipeBox;
	}

	public List<Float> getOverallRatingToTheRecipe() {
		return overallRatingToTheRecipe;
	}

	public List<Integer> getRatingGivenByUserToRecipe() {
		return ratingGivenByUserToRecipe;
	}

	public List<String> getTypesOfRecipesInRecipeBox() {
		return typesOfRecipesInRecipeBox;
	}

	public void readFields(DataInput in) throws IOException {
		String text = in.readLine();
		try {
			Cook cook = new Gson().fromJson(text, Cook.class);
			if (cook != null) {
				name = cook.getName();
				expertise = cook.getExpertise();
				cookingInterests = cook.getCookingInterests();
				hobbies = cook.getHobbies();
				numberOfRecipesInRecipeBox = cook.getNumberOfRecipesInRecipeBox();
				namesOfRecipesInRecipeBox = cook.getNamesOfRecipesInRecipeBox();
				typesOfRecipesInRecipeBox = cook.getTypesOfRecipesInRecipeBox();
				overallRatingToTheRecipe = cook.getOverallRatingToTheRecipe();
				ratingGivenByUserToRecipe = cook.getRatingGivenByUserToRecipe();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public void setCookingInterests(List<String> cookingInterests) {
		this.cookingInterests = cookingInterests;
	}

	public void setExpertise(String expertise) {
		this.expertise = expertise;
	}

	public void setHobbies(List<String> hobbies) {
		this.hobbies = hobbies;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setNamesOfRecipesInRecipeBox(List<String> namesOfRecipesInRecipeBox) {
		this.namesOfRecipesInRecipeBox = namesOfRecipesInRecipeBox;
	}

	public void setNumberOfRecipesInRecipeBox(int numberOfRecipesInRecipeBox) {
		this.numberOfRecipesInRecipeBox = numberOfRecipesInRecipeBox;
	}

	public void setOverallRatingToTheRecipe(List<Float> overallRatingToTheRecipe) {
		this.overallRatingToTheRecipe = overallRatingToTheRecipe;
	}

	public void setRatingGivenByUserToRecipe(List<Integer> ratingGivenByUserToRecipe) {
		this.ratingGivenByUserToRecipe = ratingGivenByUserToRecipe;
	}

	public void setTypesOfRecipesInRecipeBox(List<String> typesOfRecipesInRecipeBox) {
		this.typesOfRecipesInRecipeBox = typesOfRecipesInRecipeBox;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBytes(new Gson().toJson(this, Cook.class) + "\n");
		
	}
}
