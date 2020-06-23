package edu.upenn.cis455.xpathengine;

import java.io.InputStream;
import org.w3c.dom.Document;
import org.xml.sax.helpers.DefaultHandler;

/** (MS2) Implements XPathEngine to handle XPaths.
  */
public class XPathEngineImpl implements XPathEngine {


  public XPathEngineImpl() {
    // Do NOT add arguments to the constructor!!
  }
	
  public void setXPaths(String[] s) {
    /* TODO: Store the XPath expressions that are given to this method */
  }

  public boolean isValid(int i) {
    /* TODO: Check which of the XPath expressions are valid */
    return false;
  }
	
  public boolean[] evaluate(Document d) { 
    /* TODO: Check whether the document matches the XPath expressions */
    return null; 
  }

@Override
public boolean isSAX() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
	// TODO Auto-generated method stub
	return null;
}
        
}
