package org.crawling.models;

import java.util.Date;

public class Review {

	/**
	 * @param args
	 */
	private int rating;
	
	private Date reviewDate;
	
	private String review;
	
	private Reviewer reviewer;
	
	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public Date getReviewDate() {
		return reviewDate;
	}

	public void setReviewDate(Date reviewDate) {
		this.reviewDate = reviewDate;
	}

	public String getReview() {
		return review;
	}

	public void setReview(String review) {
		this.review = review;
	}

	public Reviewer getReviewer() {
		return reviewer;
	}

	public void setReviewer(Reviewer reviewer) {
		this.reviewer = reviewer;
	}

	public ReviewDish getReviewDish() {
		return reviewDish;
	}

	public void setReviewDish(ReviewDish reviewDish) {
		this.reviewDish = reviewDish;
	}

	private ReviewDish reviewDish;
	

}
