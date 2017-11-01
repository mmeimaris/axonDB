package com.athena.imis.tests;

import java.io.File;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;

public class JenaLoader {

	public static void main(String[] args) {
		Dataset dataset2 = TDBFactory.createDataset("C:/temp/alexiou");
		
		//args[0] = "/data1/mmeimaris/TDB1000"
		Model model = dataset2.getDefaultModel();
		System.out.println(model.size());
		if(true) return;
		//args[1] = "/data1/mmeimaris/LUBM"
		File dir = new File("C:/temp");
		
		File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		      
		    	if(child.getName().contains("output000026.nq")){
			    	
			    	  TDBLoader.loadModel(model, child.toString());
		    				    		
			      }
		    }
		  } 
		
		dataset2.close();

	}

}
