package org.crawling.analytics;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToolBar;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;

/**
 * A simple demonstration application showing how to create a bar chart.
 * 
 */
public class UserAnalytics extends ApplicationFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final HashMap<String, String> map;
	private JTextField textField;

	/**
	 * Creates a new demo instance.
	 * 
	 * @param title
	 *            the frame title.
	 */
	public static HashMap<String, String> readFile() {
		HashMap<String, String> map = new HashMap<String, String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(
					"xmlDataFiles/part-00000")));
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] splits = line.split("\t");
				if (splits.length > 0) {
					String value = "";
					for (int i = 1; i < splits.length; i++)
						value += splits[i] + "\t";
					map.put(splits[0], value);
				}
			}
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	public UserAnalytics() {

		super("User Analytics");
		map = readFile();

		final JPanel chartPanel = new JPanel();
		chartPanel.setLayout(new BorderLayout(0, 0));

		setContentPane(chartPanel);

		final JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		chartPanel.add(tabbedPane, BorderLayout.NORTH);

		final JToolBar toolBar = new JToolBar();
		final ChartPanel panel = new ChartPanel(null);
		panel.setVisible(false);
		tabbedPane.addTab("Input", null, toolBar, null);

		JLabel lblNewLabel = new JLabel("Reviewer");
		toolBar.add(lblNewLabel);

		textField = new JTextField();
		toolBar.add(textField);
		textField.setColumns(10);

		JButton btnNewButton = new JButton("Find Analysis");
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if (textField.getText().length() > 0) {

					final CategoryDataset dataset = createDataset(map
							.containsKey(textField.getText()) ? map
							.get(textField.getText()) : "");

					final JFreeChart chart = createChart(dataset);
					panel.setChart(chart);
					panel.setPreferredSize(new Dimension(800, 800));
					panel.setLayout(new FormLayout(
							new ColumnSpec[] { ColumnSpec.decode("735px"), },
							new RowSpec[] { RowSpec.decode("23px"), }));
					chartPanel.add(panel);
					panel.setVisible(true);
				} else {
					panel.setVisible(false);
				}
			}
		});
		toolBar.add(btnNewButton);

		tabbedPane.addTab("Graph", null, panel, null);

	}

	/**
	 * Returns a sample dataset.
	 * 
	 * @param value
	 * 
	 * @return The dataset.
	 */
	private CategoryDataset createDataset(String value) {
		final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		// row keys...
		if (value.length() > 0) {
			System.out.println(value);
			int numberOfCategories = (value.split("\\t").length) >= 2 ? (value
					.split("\\t").length) / 2 : 1;
			String[] ratings = value.split("\\t");
			System.out.println(ratings.length - 1);
			// column keys...
			final String[] category = new String[numberOfCategories];
			int categoryTemp = 0, ratingTemp = 0;
			final int[] ratingValues = new int[numberOfCategories];
			for (int i = 0; i < ratings.length; i++) {
				if (i % 2 == 0) {
					ratingValues[ratingTemp] = Integer.parseInt(ratings[i]);
					ratingTemp++;
				} else {
					category[categoryTemp] = ratings[i];
					categoryTemp++;

				}
			}
			for (int i = 0; i < numberOfCategories; i++) {
				dataset.addValue(ratingValues[i], "Dishes ", category[i]);
			}
		}
		return dataset;

	}

	/**
	 * Creates a sample chart.
	 * 
	 * @param dataset
	 *            the dataset.
	 * @return The chart.
	 */
	private JFreeChart createChart(final CategoryDataset dataset) {

		// create the chart...
		final JFreeChart chart = ChartFactory.createBarChart("Bar Chart Demo", // chart
																				// title
				"Category", // domain axis label
				"Value", // range axis label
				dataset, // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				true, // tooltips?
				false // URLs?
				);

		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...

		// set the background color for the chart...
		chart.setBackgroundPaint(Color.white);

		// get a reference to the plot for further customisation...
		final CategoryPlot plot = chart.getCategoryPlot();
		plot.setBackgroundPaint(Color.lightGray);
		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.white);

		// set the range axis to display integers only...
		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		// disable bar outlines...
		final BarRenderer renderer = (BarRenderer) plot.getRenderer();
		renderer.setDrawBarOutline(false);

		// set up gradient paints for series...
		final GradientPaint gp0 = new GradientPaint(0.0f, 0.0f, Color.cyan,
				0.0f, 0.0f, Color.lightGray);
		renderer.setSeriesPaint(0, gp0);
		final CategoryAxis domainAxis = plot.getDomainAxis();
		domainAxis.setCategoryLabelPositions(CategoryLabelPositions
				.createUpRotationLabelPositions(Math.PI / 6.0));
		// OPTIONAL CUSTOMISATION COMPLETED.
		return chart;

	}

	// ****************************************************************************
	// * JFREECHART DEVELOPER GUIDE *
	// * The JFreeChart Developer Guide, written by David Gilbert, is available
	// *
	// * to purchase from Object Refinery Limited: *
	// * *
	// * http://www.object-refinery.com/jfreechart/guide.html *
	// * *
	// * Sales are used to provide funding for the JFreeChart project - please *
	// * support us so that we can continue developing free software. *
	// ****************************************************************************

	/**
	 * Starting point for the demonstration application.
	 * 
	 * @param args
	 *            ignored.
	 */
	public static void main(final String[] args) {

		final UserAnalytics demo = new UserAnalytics();
		demo.pack();
		RefineryUtilities.centerFrameOnScreen(demo);
		demo.setVisible(true);

	}

}
