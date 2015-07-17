package org.crawling.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import com.google.gson.Gson;

public class ReviewDish implements WritableComparable<ReviewDish> {

	private String name;

	private String description;

	private String paraDescription;

	private Reviewer postedBy;

	private int numberOfReviews;

	private int servings;

	private List<String> ingredientsName;

	private List<String> ingredientsAmount;

	private List<Review> reviews;

	private List<String> directions;

	public int compareTo(ReviewDish o) {
		if (this.getPostedBy().getName().equals(o.getPostedBy().getName()))
			return 0;
		else
			return this.getPostedBy().getName().compareTo(o.getPostedBy().getName())<0?-1:1;
	}

	public String getDescription() {
		return description;
	}

	public List<String> getDirections() {
		return directions;
	}

	public List<String> getIngredientsAmount() {
		return ingredientsAmount;
	}

	public List<String> getIngredientsName() {
		return ingredientsName;
	}

	public String getName() {
		return name;
	}

	public int getNumberOfReviews() {
		return numberOfReviews;
	}

	public String getParaDescription() {
		return paraDescription;
	}

	public Reviewer getPostedBy() {
		return postedBy;
	}

	public List<Review> getReviews() {
		return reviews;
	}

	public int getServings() {
		return servings;
	}

	public void readFields(DataInput in) throws IOException {
		String text = in.readLine();
		try {
			ReviewDish dish = new Gson().fromJson(text, ReviewDish.class);
			if (dish != null) {
				name = dish.getName();
				reviews = dish.getReviews();
				ingredientsName = dish.getIngredientsName();
				ingredientsAmount = dish.getIngredientsAmount();
				description = dish.getDescription();
				paraDescription = dish.getParaDescription();
				postedBy = dish.getPostedBy();
				numberOfReviews = dish.getNumberOfReviews();
				servings = dish.getServings();
				directions = dish.getDirections();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setDirections(List<String> directions) {
		this.directions = directions;
	}

	public void setIngredientsAmount(List<String> ingredientsAmount) {
		this.ingredientsAmount = ingredientsAmount;
	}

	public void setIngredientsName(List<String> ingredientsName) {
		this.ingredientsName = ingredientsName;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setNumberOfReviews(int numberOfReviews) {
		this.numberOfReviews = numberOfReviews;
	}

	public void setParaDescription(String paraDescription) {
		this.paraDescription = paraDescription;
	}

	public void setPostedBy(Reviewer postedBy) {
		this.postedBy = postedBy;
	}

	public void setReviews(List<Review> reviews) {
		this.reviews = reviews;
	}

	public void setServings(int servings) {
		this.servings = servings;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBytes(new Gson().toJson(this, ReviewDish.class) + "\n");

	}

}
