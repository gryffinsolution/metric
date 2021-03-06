package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Conf {
	private static final Logger LOG = LogManager.getLogger(Conf.class);
	private static String fileName = null;

	@SuppressWarnings("static-access")
	public void setConfFile(String fileName) {
		File f = new File(fileName);
		if (!f.isFile()) {
			LOG.error("There is no configulation xml file");
			System.exit(0);
		}
		this.fileName = fileName;
	}

	@SuppressWarnings("static-access")
	public String getConfFile() {
		return this.fileName;
	}

	public int getSingleValue(String key) {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/" + key;
			int value = Integer.parseInt((xpath.compile(expression)
					.evaluate(document)));
			LOG.trace(key + ":" + value);
			return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			LOG.error(key);
			e.printStackTrace();
		}
		LOG.fatal("No config value about " + key);
		return 0;
	}

	public String getSingleString(String key) {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/" + key;
			String value = (xpath.compile(expression).evaluate(document));
			// LOG.trace(key + ":" + value);
			return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		LOG.fatal("No config value about " + key);
		return null;
	}

	public String getDbURL() {
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/main_db_url";
			String hostNmae = xpath.compile(expression).evaluate(document);
			LOG.trace("DBhostName:" + hostNmae);
			return hostNmae;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		return "localhost";
	}

	public long getLongValue(String key) {
		LOG.info(key);
		try {
			InputSource is = new InputSource(new FileReader(fileName));
			Document document = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			XPath xpath = XPathFactory.newInstance().newXPath();
			String expression = "/flog/" + key;
			long value = Long.parseLong((xpath.compile(expression)
					.evaluate(document)));
			LOG.trace(key + ":" + value);
			return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			LOG.error(key);
			e.printStackTrace();
		}
		LOG.fatal("No config value about " + key);
		return 0L;
	}
}