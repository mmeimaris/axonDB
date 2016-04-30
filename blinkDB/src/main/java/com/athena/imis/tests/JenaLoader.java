package com.athena.imis.tests;

import java.io.File;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;

public class JenaLoader {

	public static void main(String[] args) {
		Dataset dataset2 = TDBFactory.createDataset(args[0]);
		//args[0] = "/data1/mmeimaris/TDB1000"
		Model model = dataset2.getDefaultModel();
		//args[1] = "/data1/mmeimaris/LUBM"
		File dir = new File(args[1]);
		
		File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		      
		    	if(child.getName().contains(".rdf.xml")){
			    	
			    	  TDBLoader.loadModel(model, child.getName());
		    		
		    		
			      }
		    }
		  } 
		
		dataset2.close();

	}

}
