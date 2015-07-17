package org.dblp.models;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Article {

	public static void main(String[] args) throws Exception {
		// File f = new File("/homes/saibharath/dblp.xml");
		SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
		ArticleHandler handler = new ArticleHandler();
		parser.parse("/homes/saibharath/dblp/dblp.xml", handler);

		/*
		 * List<Element> list = document.getElementsByAttribute("key"); for
		 * (Element article : list) { boolean yearExists = false; boolean
		 * monthExists = false; List<Element> citations =
		 * article.getElementsByTag("cite"); String paperName =
		 * article.attr("key"); Date d = new Date(); List<Element> yearElements
		 * = article.getElementsByTag("year"); int year =
		 * Integer.parseInt(yearElements.size() > 0 ? yearElements
		 * .get(0).text() : "0"); List<Element> monthElements =
		 * article.getElementsByTag("month"); int month = Integer
		 * .parseInt(monthElements.size() > 0 ? monthElements.get(0) .text() :
		 * "0"); if (year != 0) { yearExists = true; } for (Element citation :
		 * citations) { if (!citation.text().equals("...")) { if (yearExists) {
		 * d.setYear(year); if (month != 0) { d.setMonth(month); monthExists =
		 * true; } }
		 */
	}

}

class ArticleHandler extends DefaultHandler {
	private String articleName;
	private List<String> citation;
	private String year;
	private String month;
	private File outputFile;
	private String bookTitle;
	private BufferedWriter bw;
	private List<String> authors;
	private String temp;
	private String type;

	public ArticleHandler() throws Exception {
		authors = new ArrayList<String>();
		citation = new ArrayList<String>();
		outputFile = new File("/homes/saibharath/dblp/paper-citations.txt");
		bw = new BufferedWriter(new FileWriter(outputFile));
		temp = type = year = month = bookTitle = "";
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("article")) {
			articleName = attributes.getValue("key");
			type = "article";
		} else if (qName.equalsIgnoreCase("inproceedings")) {
			articleName = attributes.getValue("key");
			type = "inproceedings";
		} else if (qName.equalsIgnoreCase("proceedings")) {
			articleName = attributes.getValue("key");
			type = "proceedings";
		} else if (qName.equalsIgnoreCase("book")) {
			articleName = attributes.getValue("key");
			type = "book";
		} else if (qName.equalsIgnoreCase("incollection")) {
			articleName = attributes.getValue("key");
			type = "incollection";
		} else if (qName.equalsIgnoreCase("phdthesis")) {
			articleName = attributes.getValue("key");
			type = "phdthesis";
		} else if (qName.equalsIgnoreCase("mastersthesis")) {
			articleName = attributes.getValue("key");
			type = "mastersthesis";
		} else if (qName.equalsIgnoreCase("www")) {
			articleName = attributes.getValue("key");
			type = "www";
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("year")) {
			year = temp;
		} else if (qName.equalsIgnoreCase("month")) {
			month = temp;
		} else if (qName.equalsIgnoreCase("author")) {
			authors.add(temp);
		} else if (qName.equalsIgnoreCase("editor")) {
			authors.add(temp);
		} else if (qName.equalsIgnoreCase("cite")) {
			citation.add(temp);
		} else if (qName.equalsIgnoreCase("booktitle")) {
			bookTitle = temp;
		} else if (qName.equalsIgnoreCase("book")
				|| qName.equalsIgnoreCase("inproceedings")
				|| qName.equalsIgnoreCase("proceedings")
				|| qName.equalsIgnoreCase("article")
				|| qName.equalsIgnoreCase("incollection")
				|| qName.equalsIgnoreCase("phdthesis")
				|| qName.equalsIgnoreCase("mastersthesis")
				|| qName.equalsIgnoreCase("www")) {
			try {
				bw.write(articleName + "\t");
				bw.write(type + "|" + month + "|" + year + "|" + authors.size()
						+ "|");
				for (String author : authors) {
					bw.write(author + "|");
				}
				bw.write(citation.size() + "|");
				for (String cite : citation) {
					bw.write(cite + "|");
				}
				bw.write(bookTitle + "|" + "\n");
				citation = new ArrayList<String>();
				authors = new ArrayList<String>();
				articleName = type = temp = year = month = bookTitle = "";
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// To take specific actions for each chunk of character data (such as
	// adding the data to a node or buffer, or printing it to a file).
	@Override
	public void characters(char ch[], int start, int length)
			throws SAXException {
		temp = new String(ch, start, length);
		temp = StringEscapeUtils.unescapeHtml4(temp);
	}
	public String[] decode(){
		return new String[]{"",""};
	}

}