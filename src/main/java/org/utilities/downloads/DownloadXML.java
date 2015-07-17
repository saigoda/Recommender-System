package org.utilities.downloads;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class DownloadXML {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		HttpClient client = new DefaultHttpClient();
		String url = "http://allrecipes.com/recipedetail.xml";
		HttpGet request = null;
		try {
			request = new HttpGet(url);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (request != null) {
			HttpResponse response = null;
			try {
				response = client.execute(request);
			} catch (HttpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (response != null) {
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				} catch (IllegalStateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				File file = new File("recipeDetails.xml");
				BufferedWriter fis = null;
				try {
					fis = new BufferedWriter(new FileWriter(file));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String line = "";
				try {
					while((line = br.readLine())!=null){
						fis.write(line);
					}
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
