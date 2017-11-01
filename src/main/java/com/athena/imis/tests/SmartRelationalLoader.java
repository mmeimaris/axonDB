package com.athena.imis.tests;

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.athena.imis.models.BigCharacteristicSet;
import com.athena.imis.models.BigExtendedCharacteristicSet;
import com.athena.imis.models.MinHash;
import com.athena.imis.models.NewCS;

public class SmartRelationalLoader {

	public static Map<String, Integer> propertiesSet = new THashMap<String, Integer>();
	public static Map<Integer, String> revPropertiesSet = new THashMap<Integer, String>();
	public static Map<String, Integer> intMap = new THashMap<String, Integer>();
	public static Map<Integer, String> revIntMap = new THashMap<Integer, String>();
	public static int meanMultiplier = 5;
	
	public static void main(String[] args) {
		
		
		//195.251.63.129
		/*localhost
		C:/temp/lubm1.nt
		testbatch
		100
		postgres
		postgres*/
				System.out.println("Starting time: " + new Date().toString());
				int batchSize = Integer.parseInt(args[3]);
				Connection conn = null;
			      Statement stmt = null;
			      try {
			         Class.forName("org.postgresql.Driver");
			         conn = DriverManager
			        		 .getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2], args[4], args[5]);
			         System.out.println("Opened database successfully");

			         
			      } catch ( Exception e ) {
			         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			         System.exit(0);
			      }	      
			     
				int next = 0;
				
				
				int propIndex = 0, nextInd = 0;
				long start = System.nanoTime();
				
				int triplesParsed2 = 0;
				
				FileInputStream is;
				try {
					is = new FileInputStream(args[1]);
					NxParser nxp = new NxParser();
					//RdfXmlParser nxp = new RdfXmlParser(); 
					//nxp.parse(is, "http://ex");
					nxp.parse(is);
					for (Node[] nx : nxp){
						triplesParsed2++;
						if(triplesParsed2 == 1000000) break;
					  // prints the subject, eg. <http://example.org/>
					  //System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
					}
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
				
			    System.out.println("triplesParsed: " + triplesParsed2);
			    System.out.println(System.nanoTime()-start);
			    final int[][] array = new int[triplesParsed2][4];
			  
			    String s, p, o;
			    	   			    
			    try {
					is = new FileInputStream(args[1]);
					NxParser nxp = new NxParser();
					//RdfXmlParser nxp = new RdfXmlParser(); 
					//nxp.parse(is, "http://ex");
					nxp.parse(is);
					for (Node[] nx : nxp){
						if(triplesParsed2==0) break;
						triplesParsed2--;
					  // prints the subject, eg. <http://example.org/>
					  //System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
						s = nx[0].toString();
						p = nx[1].toString();
						o = nx[2].toString();
						if(!propertiesSet.containsKey(p)){							
						    
				    		revPropertiesSet.put(propIndex, p);
				    		propertiesSet.put(p, propIndex++);	    		
					    	
				    	}
						
				    	if(!intMap.containsKey(s)){		    				    
				    	
				    		//revIntMap.put(nextInd, s);
				    		intMap.put(s, nextInd++);
				    		
				    	}
						
				    	if(!intMap.containsKey(o)){		   			   
				    	
				    		//if(triple.getObject().isURI())
				    		//revIntMap.put(nextInd, o);
				    		intMap.put(o, nextInd++);
				    		//else
				    			//intMap.put(o, Integer.MAX_VALUE);
				    		
				    	}
				            
				    	int[] ar = new int[4];
						ar[0] = intMap.get(s);//spLong;
						ar[1] = propertiesSet.get(p);//spLong;
						ar[2] = intMap.get(o);//spLong;			
						ar[3] = -1;
						//array.add(next, ar);
						array[next++] = ar;
						//next++;
					}
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			 
			    long end = System.nanoTime();
				
				System.out.println("piped: " + (end-start));
				
				StringBuilder sb2 = new StringBuilder();
				CopyManager cpManager2;
				System.out.println("Adding keys to dictionary. " + new Date().toString());
				try {
					stmt = conn.createStatement();
				    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dictionary (id INT, label INT); ");
				    stmt.close();		       
				        
					
					cpManager2 = ((PGConnection)conn).getCopyAPI();
					PushbackReader reader2 = new PushbackReader( new StringReader(""), 10000 );
					Iterator<Map.Entry<String, Integer>> keyIt = intMap.entrySet().iterator();
					int iter = 0;
					while(keyIt.hasNext())
					{
						Entry<String, Integer> nextEntry = keyIt.next();
					    sb2.append(nextEntry.getValue()).append(",")		      
					      .append(nextEntry.getKey().hashCode()).append("\n");
					    if (iter++ % batchSize == 0)
					    {
					      reader2.unread( sb2.toString().toCharArray() );
					      cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
					      sb2.delete(0,sb2.length());
					    }
					}
					reader2.unread( sb2.toString().toCharArray() );
					cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
					reader2.close();
				} catch (SQLException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Done adding keys to dictionary. " + new Date().toString());
				
				Arrays.sort(array, new Comparator<int[]>() {
				    public int compare(int[] s1, int[] s2) {
				        if (s1[0] > s2[0])
				            return 1;    // tells Arrays.sort() that s1 comes after s2
				        else if (s1[0] < s2[0])
				            return -1;   // tells Arrays.sort() that s1 comes before s2
				        else {
				            return 0;
				        }
				    }
				});
				end = System.nanoTime();
				System.out.println("sorting: " + (end-start));
				
				int previousSubject = Integer.MIN_VALUE;
												
				Map<NewCS, Integer> csMap = new HashMap<>();

				int csIndex = 0;
				/*for(int i = 0; i < l.size(); i++){
					long t = l.apply((long)i);*/
				int previousStart = 0;
				NewCS cs = null;
				int[] t ;
				int subject ;
				int prop ;
				
				Map<Integer, Integer> dbECSMap = new THashMap<Integer, Integer>();
				Map<Integer, NewCS> rucs = new HashMap<Integer, NewCS>();
				
				List<Integer> propList = new ArrayList<Integer>();;
				
				for(int i = 0; i < array.length; i++){
					t = array[i];
					subject = t[0];
					prop = t[1];
					
					if(i > 0 && previousSubject != subject){
											
						cs = new NewCS(propList);					
						if(!csMap.containsKey(cs)){
							
							dbECSMap.put(previousSubject, csIndex);
							rucs.put(csIndex, cs);
							for(int j = previousStart; j < i; j++)
								array[j][3] = csIndex;
							csMap.put(cs, csIndex++);
							
							
						}
						else{
							dbECSMap.put(previousSubject, csMap.get(cs));
							//array[i-1][3] = ucs.get(cs);
							for(int j = previousStart; j < i; j++)
								array[j][3] = csMap.get(cs);
						}
						previousStart = i;
						propList = new ArrayList<Integer>();
					}
					if(!propList.contains(prop))
						propList.add(prop);
					previousSubject = subject;
				}
				
				
				if(!propList.isEmpty()){
					cs = new NewCS(propList);
					if(!csMap.containsKey(cs)){
						//array[array.length-1][3] = csIndex; 
						for(int j = previousStart; j < array.length; j++)
							array[j][3] = csIndex;
						dbECSMap.put(previousSubject, csIndex);
						rucs.put(csIndex, cs);
						csMap.put(cs, csIndex);
						
					}
					else{
						for(int j = previousStart; j < array.length; j++)
							array[j][3] = csMap.get(cs);
						//array[array.length-1][3] = ucs.get(cs);
						dbECSMap.put(previousSubject, csMap.get(cs));
					}
					
				}
				end = System.nanoTime();
				System.out.println("ucs time: " + (end-start));
				start = System.nanoTime();
				Arrays.sort(array, new Comparator<int[]>() {
				    public int compare(int[] s1, int[] s2) {
				        if (s1[3] > s2[3])
				            return 1;    // s1 comes after s2
				        else if (s1[3] < s2[3])
				            return -1;   // s1 comes before s2
				        else {			          
				            return 0;
				        }
				    }
				});
								
				ArrayList<int[]> tripleListFull = new ArrayList<int[]>();
								
				Map<Integer, int[][]> csMapFull = new HashMap<Integer, int[][]>();
								
				csIndex = array[0][3];				
				
				for(int i = 0; i < array.length; i++){
					
					t = array[i];
					
					if(csIndex != t[3]){
						
						int[][] resultFull = new int[tripleListFull.size()][3];
						for(int ir = 0; ir < tripleListFull.size(); ir++){
							resultFull[ir] = tripleListFull.get(ir);
						}
						
						csMapFull.put(csIndex, resultFull);
						
						tripleListFull = new ArrayList<int[]>();
					}
					csIndex = t[3];
						
					tripleListFull.add(t);
					
				}		
			
				int[][] resultFull = new int[tripleListFull.size()][3];
				for(int i = 0; i < tripleListFull.size(); i++){
					resultFull[i] = tripleListFull.get(i);
				}
					
				
				csMapFull.put(csIndex, resultFull);
				
				end = System.nanoTime();
				System.out.println("ucs2 time: " + (end-start));
				
				System.out.println("csMapFull size: " + csMapFull.size());
								
				HashSet<String> pathPairs = new HashSet<String>();
				HashMap<String, Set<Integer>> pathPairProperties = new HashMap<String, Set<Integer>>();
				HashMap<Integer, int[]> csProps = new HashMap<Integer, int[]>(); 
				
				
				
				CopyManager cpManager;
				try {
					cpManager = ((PGConnection)conn).getCopyAPI();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return;
				}
				
				//init refs
				int idx, min, total = 0;
				PushbackReader reader ;
				StringBuilder createTableQuery ;
				HashSet<Integer> propertiesMap ;
				HashMap<Integer, HashMap<Integer, HashSet<Integer>>> spoValues ;
						
				//do the merging stuff
				//System.out.println("csMap: " + csMap.toString());
				Map<NewCS, Set<NewCS>> ancestors = new HashMap<NewCS, Set<NewCS>>();
				Map<NewCS, Integer> csSizes = new HashMap<NewCS, Integer>();
				//init ancestor map and size map
				for(NewCS n : csMap.keySet()){
					ancestors.put(n, new HashSet<NewCS>());
					int size = csMapFull.get(csMap.get(n)).length;
					csSizes.put(n, size);
				}				
				//discover ancestry
				
				for(NewCS parent : csMap.keySet()){
					
					for(NewCS child: csMap.keySet()){
						
						if(parent.equals(child)) continue;
					
						if(child.getAsList().containsAll(parent.getAsList())){
							Set<NewCS> children = ancestors.getOrDefault(parent, new HashSet<NewCS>());
							children.add(child);
							ancestors.put(parent, children);
						}
						else{
							//check for set inclusion?
							if(true) continue;
							if(jaccardSimilarity(child.getAsList(), parent.getAsList())>0.8){
								Set<NewCS> children = ancestors.getOrDefault(parent, new HashSet<NewCS>());
								children.add(child);
								ancestors.put(parent, children);
							}
						}
						
					}
					
				}
				System.out.println("Ancestor listing complete.");
				Map<NewCS, Set<NewCS>> immediateAncestors = getImmediateAncestors(ancestors);
				Map<NewCS, Set<NewCS>> reverseImmediateAncestors = new HashMap<NewCS, Set<NewCS>>();
				for(NewCS f : csMap.keySet()){
					reverseImmediateAncestors.put(f, new HashSet<NewCS>());
				}
				
				for(NewCS nextCS : immediateAncestors.keySet()){
					
					for(NewCS child : immediateAncestors.get(nextCS)){
						Set<NewCS> set = reverseImmediateAncestors.getOrDefault(child, new HashSet<NewCS>());
						set.add(nextCS);
						reverseImmediateAncestors.put(child, set);
					}
				}
				//System.out.println("\n\n No children: ") ;
				for(NewCS nextCS : csMap.keySet()) {
					//if(immediateAncestors.containsKey(nextCS))
					//	continue;
					//System.out.println(nextCS.toString() + ": " + csSizes.get(nextCS));
					total+=csSizes.get(nextCS);
					
				}
				System.out.println("total estimate size: " + total) ;
				System.out.println("mean size: " + total/csMap.size()) ;
				int denseCS = 0, totalDenseRows = 0;;
				Set<NewCS> denseCSs = new HashSet<NewCS>();
				for(NewCS nextCS : csMap.keySet()) {
					//if(immediateAncestors.containsKey(nextCS))
					//	continue;
					if(csSizes.get(nextCS) >= total/csMap.size()*meanMultiplier*2){
						denseCS++;
						denseCSs.add(nextCS);
						totalDenseRows += csSizes.get(nextCS);
					}
					
				}
				System.out.println("Total CSs: " + csMap.size());
				System.out.println("Dense CSs: " + denseCS);
//				for(NewCS nextDense : denseCSs){
//					System.out.println("\t"+nextDense.toString());
//				}
				System.out.println("Dense CS Coverage: " + totalDenseRows);						
				
				//System.out.println("\n\n\n\n");
				
				HashMap<List<NewCS>, Integer> pathCosts = new HashMap<List<NewCS>, Integer>();
				
				Set<List<NewCS>> foundPaths = findPaths(denseCSs, pathCosts, csSizes, reverseImmediateAncestors, true, false);
				
				List<NewCS> cur ;
				
				Set<List<NewCS>> clonedPaths = new HashSet<List<NewCS>>();
				for(List<NewCS> n : foundPaths){
					clonedPaths.add(new ArrayList<NewCS>(n));
				}
				Iterator<List<NewCS>> keyIt = foundPaths.iterator();			
				while(keyIt.hasNext())
				{
					
					List<NewCS> outerPath = keyIt.next();

					boolean isContained = false;

					for(List<NewCS> innerPath : clonedPaths){
						if(outerPath.equals(innerPath)) continue;					
						if(innerPath.containsAll(outerPath)){
							isContained = true;
							break;
						}					
					}
					if(isContained)
						keyIt.remove();
				}
//				for(List<NewCS> nextPath : foundPaths){
//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				//System.out.println("\n\n\n\n");
				
				List<List<NewCS>> orderedPaths = new ArrayList<List<NewCS>>(foundPaths);			
				Collections.sort(orderedPaths, new Comparator<List<NewCS>>() {

			        public int compare(List<NewCS> o1, List<NewCS> o2) {
			            if (pathCosts.get(o1) > pathCosts.get(o2)) return -11;
			            else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1;
			            else return 0;
			            		
			        }
			    });
				
//				for(List<NewCS> nextPath : orderedPaths){
//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				List<List<NewCS>> finalList = new ArrayList<List<NewCS>>();

				int totalIterations = 0;					
				
				for(int i = 0; i < orderedPaths.size(); i++){
					
					cur = orderedPaths.get(i);
													
					for(int k = i+1; k < orderedPaths.size(); k++){
						
						List<NewCS> nextCS = orderedPaths.get(k);
						
						nextCS.removeAll(cur);
						updateCardinality(nextCS, csSizes, pathCosts);	
						
					}				
				
					
					Collections.sort(orderedPaths.subList(i+1, orderedPaths.size()), new Comparator<List<NewCS>>() {

				        public int compare(List<NewCS> o1, List<NewCS> o2) {
				            if (pathCosts.get(o1) > pathCosts.get(o2)) return -1;
				            else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1;
				            else return 0;
				            		
				        }
				    });
					
				}
							
				Iterator<List<NewCS>> finalIt = orderedPaths.iterator();
				while(finalIt.hasNext()){
					List<NewCS> n = finalIt.next();
					if(n.isEmpty()){
						finalIt.remove();
						pathCosts.remove(n);
					}
					else{
						updateCardinality(n, csSizes, pathCosts);
						finalList.add(n);
					}
				}
				Set<NewCS> finalUnique = new HashSet<NewCS>();
				for(List<NewCS> finalCS : finalList){
					
					finalUnique.addAll(finalCS);
				}
				//System.out.println("\n\n\n\n\n");
//				for(List<NewCS> nextPath : finalList){
//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				System.out.println(finalUnique.size());
				System.out.println(finalList.size() + ": " + finalList.toString());
				
				int notCovered = 0;
				Set<NewCS> notContained = new HashSet<NewCS>();
				for(NewCS nextCS : csMap.keySet()){
					if(!finalUnique.contains(nextCS)){
						//System.out.println("Not contained: ("+csSizes.get(nextCS)+")" + nextCS.toString()) ;
						notCovered += csSizes.get(nextCS);
						notContained.add(nextCS);
					}
				}
				System.out.println("Not covered : " + notCovered); 
				
				Map<List<NewCS>, Integer> remainingCosts = new HashMap<List<NewCS>, Integer>();
				Map<NewCS, Set<NewCS>> remainingAncestors = new HashMap<NewCS, Set<NewCS>>();
//				MinHash minhash = new MinHash(3, propertiesSet.size());
//				Map<NewCS, int[]> signatures = new HashMap<NewCS, int[]>();
//				for(NewCS parent : notContained){
//					signatures.put(parent, minhash.signature(new HashSet<Integer>(parent.getAsList())));
//				}
				for(NewCS parent : notContained){
					
					for(NewCS child: notContained){
						
						if(parent.equals(child)) continue;
					
						if(child.getAsList().containsAll(parent.getAsList())){
							Set<NewCS> children = remainingAncestors.getOrDefault(parent, new HashSet<NewCS>());
							children.add(child);
							remainingAncestors.put(parent, children);
						}
						else{							
//							double sim = minhash.similarity(signatures.get(child), signatures.get(parent));
//							if(sim>0.9){
//								Set<NewCS> children = remainingAncestors.getOrDefault(parent, new HashSet<NewCS>());
//								children.add(child);
//								remainingAncestors.put(parent, children);
//							}
							if(jaccardSimilarity(child.getAsList(), parent.getAsList())>0.9){
								Set<NewCS> children = ancestors.getOrDefault(parent, new HashSet<NewCS>());
								children.add(child);
								remainingAncestors.put(parent, children);
							}
						}
						
					}
					
				}
				
				System.out.println("Remaining ancestor listing complete.");
				Map<NewCS, Set<NewCS>> remainingImmediateAncestors = getImmediateAncestors(remainingAncestors);
				Map<NewCS, Set<NewCS>> reverseImmediateAncestorsNotContained = new HashMap<NewCS, Set<NewCS>>();
				for(NewCS f : notContained){
					reverseImmediateAncestorsNotContained.put(f, new HashSet<NewCS>());
				}
				for(NewCS nextCS : remainingImmediateAncestors.keySet()){
					
					for(NewCS child : remainingImmediateAncestors.get(nextCS)){
						Set<NewCS> set = reverseImmediateAncestorsNotContained.getOrDefault(child, new HashSet<NewCS>());
						set.add(nextCS);
						reverseImmediateAncestorsNotContained.put(child, set);
					}
				}
				//Now for the remaining paths.
				Set<NewCS> remainingDenseCSs = new HashSet<NewCS>();
				for(NewCS nextCS : notContained) {
					//if(immediateAncestors.containsKey(nextCS))
					//	continue;
					if(csSizes.get(nextCS) >= (notCovered)/notContained.size()*meanMultiplier){
						//denseCS++;
						remainingDenseCSs.add(nextCS);
						//totalDenseRows += csSizes.get(nextCS);
					}
					
				}
				Set<List<NewCS>> remainingPaths = findPaths(remainingDenseCSs, remainingCosts, csSizes, reverseImmediateAncestorsNotContained, true, true);
				
				int totalCovered = 0;
				
				for(List<NewCS> nextPath : finalList){
					
					//System.out.println("next path: " + nextPath );
					totalCovered += pathCosts.get(nextPath);
					
				}
				System.out.println("Total coverage: " + totalCovered) ;
				
				int totalRemaining = 0;
				
				for(List<NewCS> nextPath : remainingPaths){
					
					//System.out.println("next path: " + nextPath );
					totalRemaining  += remainingCosts.get(nextPath);
					
				}
				System.out.println("Total remaining:" + totalRemaining) ;
				
				
				//start remaining cleanup
				Set<List<NewCS>> remainingClonedPaths = new HashSet<List<NewCS>>();
				for(List<NewCS> n : remainingPaths){
					remainingClonedPaths.add(new ArrayList<NewCS>(n));
				}
				System.out.println("Size of remaining Paths: " + remainingPaths.size());
				Iterator<List<NewCS>> keyRIt = remainingPaths.iterator();		
				List<NewCS> outerPath ;
				boolean isContained ;
				while(keyRIt.hasNext())
				{
					
					outerPath = keyRIt.next();

					isContained = false;

					for(List<NewCS> innerPath : remainingClonedPaths){
						if(outerPath.equals(innerPath)) continue;					
						if(innerPath.containsAll(outerPath)){
							isContained = true;
							break;
						}
					}
					
					if(isContained){
						keyRIt.remove();
						remainingClonedPaths.remove(outerPath);
					}
				}
				System.out.println("Removed contained.");
//				for(List<NewCS> nextPath : foundPaths){
//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				//System.out.println("\n\n\n\n");
				
				System.out.println("Sorting...");
				List<List<NewCS>> remainingOrderedPaths = new ArrayList<List<NewCS>>(remainingPaths);			
				Collections.sort(remainingOrderedPaths, new Comparator<List<NewCS>>() {

			        public int compare(List<NewCS> o1, List<NewCS> o2) {
			            if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -11;
			            else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
			            else return 0;
			            		
			        }
			    });
				System.out.println("Done.");
//				for(List<NewCS> nextPath : orderedPaths){
//					System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				List<List<NewCS>> remainingFinalList = new ArrayList<List<NewCS>>();

				totalIterations = 0;					
				
				System.out.println("Pruning...");
				for(int i = 0; i < remainingOrderedPaths.size(); i++){
					
					cur = remainingOrderedPaths.get(i);
													
					for(int k = i+1; k < remainingOrderedPaths.size(); k++){
						
						List<NewCS> nextCS = remainingOrderedPaths.get(k);
						
						nextCS.removeAll(cur);
						
						updateCardinality(nextCS, csSizes, remainingCosts);	
						
					}				
									
					Collections.sort(remainingOrderedPaths.subList(i+1, remainingOrderedPaths.size()), new Comparator<List<NewCS>>() {

				        public int compare(List<NewCS> o1, List<NewCS> o2) {
				            if (remainingCosts.get(o1) > remainingCosts.get(o2)) return -1;
				            else if (remainingCosts.get(o1) < remainingCosts.get(o2)) return 1;
				            else return 0;
				            		
				        }
				    });
					
				}
				System.out.println("Done.");
				finalIt = remainingOrderedPaths.iterator();
				while(finalIt.hasNext()){
					List<NewCS> n = finalIt.next();
					if(n.isEmpty()){
						finalIt.remove();
						remainingCosts.remove(n);
					}
					else{
						updateCardinality(n, csSizes, remainingCosts);
						remainingFinalList.add(n);
					}
				}
				Set<NewCS> remainingFinalUnique = new HashSet<NewCS>();
				for(List<NewCS> finalCS : remainingFinalList){
					
					remainingFinalUnique.addAll(finalCS);
				}
				
//				for(List<NewCS> nextPath : remainingFinalList){
//					System.out.println(remainingCosts.get(nextPath)+": " + nextPath.toString()) ;
//				}
				totalRemaining = 0;
				for(List<NewCS> nextPath : remainingFinalList){
					
					//System.out.println("next path: " + nextPath );
					totalRemaining  += remainingCosts.get(nextPath);
					
				}
				System.out.println("Total remaining:" + totalRemaining) ;
				System.out.println("Remaining Unique: " + remainingFinalUnique.size());
				//end remaining cleanup
				
				int coveredSoFar = totalCovered + totalRemaining;
				
				System.out.println("Dataset coverage: "  +  ((double)coveredSoFar/(double)total));
				
				Map<List<NewCS>, Integer> pathMap = new HashMap<List<NewCS>, Integer>();
				int pathIndex = 0;
				Map<Integer, int[][]> mergedMapFull = new HashMap<Integer, int[][]>();
				Map<NewCS, List<NewCS>> csToPathMap = new HashMap<NewCS, List<NewCS>>();
				finalList.addAll(remainingFinalList);
				//also compute new density factor
				int totalCSInPaths = 0;
				for(List<NewCS> pathP : finalList){
					pathMap.put(pathP, pathIndex++);
					int[][] triples = csMapFull.get(csMap.get(pathP.get(0)));
					int[][] concat = triples;
					csToPathMap.put(pathP.get(0), pathP);
					totalCSInPaths += pathP.size();
					for(int i = 1; i < pathP.size(); i++){
						concat = ArrayUtils.addAll(concat, csMapFull.get(csMap.get(pathP.get(i)))) ;
						csToPathMap.put(pathP.get(i), pathP);						
					}
					mergedMapFull.put(pathMap.get(pathP), concat);
					
				}
								
				double density = (double) coveredSoFar / totalCSInPaths ;  
				System.out.println("Density: " + density);
				Map<Integer, List<NewCS>> reversePathMap = new HashMap<Integer, List<NewCS>>();
				for(List<NewCS> pathP : pathMap.keySet())
					reversePathMap.put(pathMap.get(pathP), pathP);
				//if(true) return ;
				System.out.println("merged map full: " + mergedMapFull.toString());
				Iterator<Map.Entry<Integer, int[][]>> it = mergedMapFull.entrySet().iterator();											
				
				int nextPathIndex;
				while(it.hasNext()){
					Entry<Integer, int[][]> nextEntry = it.next();
					nextPathIndex = nextEntry.getKey();
					int[][] triplesArray = nextEntry.getValue();
					//System.out.println("Next CS: " + nextPathIndex + ", " + reversePathMap.get(nextPathIndex));
					//if(triplesArray == null || triplesArray.length == 0)
						//System.out.println("Empty triples set???");
					createTableQuery = new StringBuilder();
					createTableQuery.append("CREATE TABLE IF NOT EXISTS cs_" + nextPathIndex + " (s INT, ");
					
					//propertiesMap = new HashSet<Integer>();
					List<Integer> propsList = new ArrayList<Integer>();
					
					for(NewCS n : reversePathMap.get(nextPathIndex)){
						for(Integer np : n.getAsList()){
							if(!propsList.contains(np))
								propsList.add(np);
						}
					}
//					if(reversePathMap.get(nextPathIndex).size() > 1){
//						System.out.println("NOT ONE: " + reversePathMap.get(nextPathIndex).toString());
//						System.out.println("props: " + propsList.toString());
//						for(NewCS n : reversePathMap.get(nextPathIndex)){
//							System.out.println("\t" + n.toString());							
//						}
//					}
					Collections.sort(propsList);
						
					
//					Set<Integer> propSet = new HashSet<Integer>(); 
//					for(int[] tripleNext : triplesArray){
//						propSet.add(tripleNext[1]);
//					}
//					if(!propSet.containsAll(propsList) || !propsList.containsAll(propSet)){
//						System.out.println("Mismatch!!");
//						System.out.println("Propset: " + propSet.toString());
//						System.out.println("PropsList: " + propsList.toString());
//					}
					String cs_properties_query = "CREATE TABLE IF NOT EXISTS cs_schema (id INT, properties integer[]); INSERT INTO cs_schema (id, properties) VALUES ";
					cs_properties_query += "( " + nextPathIndex + ", "; 
					
					int[] props ;
					
					
					props = new int[propsList.size()];
					int propIdx = 0;
//					ArrayList<Integer> sortedProperties = new ArrayList<Integer>(propertiesMap);
//					Collections.sort(sortedProperties);
					for(int property : propsList){
						createTableQuery.append("p_"+property + " INT, ");
						props[propIdx++] = property;
					}
					csProps.put(nextPathIndex, props);
					cs_properties_query += "ARRAY" + Arrays.toString(props) + ") ";
					createTableQuery.deleteCharAt(createTableQuery.length()-2);
					createTableQuery.append(')');
					createTableQuery.append(';');
					
					
					
					try{				
						//c.setAutoCommit(false);
						stmt = conn.createStatement();
				        stmt.executeUpdate(createTableQuery.toString());
				        stmt.close();		       
				        
					} catch (Exception e){
						e.printStackTrace();
						return ;
					}
															
					StringBuilder sb = new StringBuilder();					
					
					try{											
						
						HashMap<Integer, HashSet<Integer>> poValues ;
						HashSet<Integer> oValues ;
						int[] valueArray ;
						spoValues = new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();
						//System.out.println("# of Triples: " + triplesArray.length);
						
						//let's try sorting to speed things up
						Arrays.sort(triplesArray, new Comparator<int[]>() {
						    public int compare(int[] s1, int[] s2) {
						        if (s1[0] > s2[0])
						            return 1;    // s1 comes after s2
						        else if (s1[0] < s2[0])
						            return -1;   // s1 comes before s2
						        else {	
						        		return 0;
						        }
						    }
						});
						int prevSubject = triplesArray[0][0];
						sb.append(prevSubject).append(',');
						poValues = new HashMap<Integer, HashSet<Integer>>();
						int rowBatch = 0;
						
						for(int[] tripleNext : triplesArray){
												
							oValues = poValues.getOrDefault(tripleNext[1], new HashSet<Integer>());
							oValues.add(tripleNext[2]);			
							if(dbECSMap.containsKey(tripleNext[2])){
								//System.out.println("1"  + rucs.get(dbECSMap.get(tripleNext[2])));
								//System.out.println("2" + csToPathMap.get(rucs.get(dbECSMap.get(tripleNext[2]))));
								if(csToPathMap.containsKey(rucs.get(dbECSMap.get(tripleNext[2])))){
									int pairedPathIndex = pathMap.get(csToPathMap.get(rucs.get(dbECSMap.get(tripleNext[2]))));
									pathPairs.add(""+nextPathIndex +"_"+pairedPathIndex);
									Set<Integer> ecsProp = pathPairProperties.getOrDefault(""+nextPathIndex +"_"+pairedPathIndex, new HashSet<Integer>());
									ecsProp.add(tripleNext[1]) ;
									pathPairProperties.put(""+nextPathIndex +"_"+pairedPathIndex, ecsProp) ;
								}
								
							}
							poValues.put(tripleNext[1], oValues);
										
							if(prevSubject != tripleNext[0]){
								//wrap up and go to next subject
								for(int nextProperty : propsList){
									if(poValues.containsKey(nextProperty)){
										if(poValues.get(nextProperty).size() > 1){
											valueArray = new int[poValues.get(nextProperty).size()];
											idx = 0;
											min = Integer.MAX_VALUE;
											for(Integer nextObject : poValues.get(nextProperty)){
												valueArray[idx++] = nextObject;	
												min = Math.min(min, nextObject);
											}
											sb.append(min).append(",");
										}											
										else{
											for(Integer nextObject : poValues.get(nextProperty)){
												sb.append(nextObject).append(",");
											}
										}
									    
									}
									else{																																		
										sb.append("null").append(",");
									}
									
																			   
								}						
								sb.deleteCharAt(sb.length()-1);
								sb.append("\n");
							    if (rowBatch++ % batchSize == 0)
							    {
							      reader = new PushbackReader( new StringReader(""), sb.length() );
							      reader.unread( sb.toString().toCharArray() );
							      cpManager.copyIn("COPY cs_" + nextPathIndex + " FROM STDIN WITH CSV NULL AS 'null'", reader );
							      sb.delete(0,sb.length());
							      if (rowBatch++ % 1000000 == 0)
							    	  System.out.println("Next checkpoint: " + rowBatch);
							    }
								poValues = new HashMap<Integer, HashSet<Integer>>();						
								sb.append(prevSubject).append(',');
							}
							prevSubject = tripleNext[0];
								
						}
						
						for(int nextProperty : propsList){
							if(poValues.containsKey(nextProperty)){
								if(poValues.get(nextProperty).size() > 1){										
									valueArray = new int[poValues.get(nextProperty).size()];
									idx = 0;
									min = Integer.MAX_VALUE;
									for(Integer nextObject : poValues.get(nextProperty)){
										valueArray[idx++] = nextObject;	
										min = Math.min(min, nextObject);
									}
									sb.append(min).append(",");
								}		
								else
									for(Integer nextObject : poValues.get(nextProperty)){
										sb.append(nextObject).append(",");
									}
							    
							}
							else{																																		
								sb.append("null").append(",");
							}
							
																	   
						}	
						//last line
						sb.deleteCharAt(sb.length()-1);
						sb.append("\n");
					    
					    reader = new PushbackReader( new StringReader(""), sb.length() );
					    reader.unread( sb.toString().toCharArray() );
					    cpManager.copyIn("COPY cs_" + nextPathIndex + " FROM STDIN WITH CSV NULL AS 'null'", reader );
					    sb.delete(0,sb.length());
					    						
						it.remove();
						//System.out.println("Removed CS from cs Map.");
						
					}
					catch (Exception e){
						e.printStackTrace();
					}
										
					try{				
				        Statement stmt2 = conn.createStatement();
						stmt2.executeUpdate(cs_properties_query);
						stmt2.close();
				        
					} catch (Exception e){
						e.printStackTrace();
					}

			         
					
				}
				System.out.println(pathPairs.size());
				StringBuilder ecsQuery = new StringBuilder();
				ecsQuery.append("CREATE TABLE IF NOT EXISTS ecs_schema (id INT, css INT, cso INT, css_properties int[], cso_properties int[]); ");
				ecsQuery.append("INSERT INTO ecs_schema (id, css, cso, css_properties, cso_properties) VALUES ");
				idx = 0;
				for(String csPair : pathPairs){
					String[] split = csPair.split("_");
					ecsQuery.append(" ("+(idx++)+", "+ split[0] + ", " + split[1] + ", "
							+ "ARRAY"+Arrays.toString(csProps.get(Integer.parseInt(split[0])))+", "
							+ "ARRAY"+Arrays.toString(csProps.get(Integer.parseInt(split[1]))) +") ");
					if(idx < pathPairs.size())
						ecsQuery.append(", ");
					else
						ecsQuery.append("; ");
					
					Set<Integer> props = pathPairProperties.get(csPair) ;
					for(Integer nextProp : props){
						
						String index = " CREATE INDEX IF NOT EXISTS cs_"+split[0]+"_p"+nextProp+" ON cs_"+split[0]+" (p_"+nextProp+") " ;
						
						
						//System.out.println(index);
						try{				
							//c.setAutoCommit(false);
							stmt = conn.createStatement();
					        stmt.executeUpdate(index);
					        stmt.close();	       
					        
						} catch (Exception e){
							e.printStackTrace();
						}
					}
					String index = " CREATE INDEX IF NOT EXISTS cs_"+split[1]+"_s ON cs_"+split[1]+" (s) " ;
					try{				
						//c.setAutoCommit(false);
						stmt = conn.createStatement();
				        stmt.executeUpdate(index);
				        stmt.close();	       
				        
					} catch (Exception e){
						e.printStackTrace();
					}
					
					//idx++;
				}
				//System.out.println(ecsQuery);
				try{				
					//c.setAutoCommit(false);
					stmt = conn.createStatement();
					System.out.println(ecsQuery.toString());
			        stmt.executeUpdate(ecsQuery.toString());
			        stmt.close();	       
			        
				} catch (Exception e){
					e.printStackTrace();
				}
				
				String propertiesSetQuery = "CREATE TABLE IF NOT EXISTS propertiesSet (id INT, uri TEXT) ; "
						+ "INSERT INTO propertiesSet (id, uri) VALUES ";
				int propCount = 0;
				for(int nextProp : revPropertiesSet.keySet()){
					propertiesSetQuery += "(" + nextProp + ", '" + revPropertiesSet.get(nextProp) + "') ";
					if(propCount < revPropertiesSet.size()-1)
						propertiesSetQuery += ", ";
					else
						propertiesSetQuery += "; ";
					propCount++;
				}
				//System.out.println(propertiesSetQuery);
				try{				
					//c.setAutoCommit(false);
					stmt = conn.createStatement();
			        stmt.executeUpdate(propertiesSetQuery.toString());
			        stmt.close();	       
			        
				} catch (Exception e){
					e.printStackTrace();
				}
				//List<List<Integer>> sortECSList = new List<Integer>(cs);
				
				
				
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Ending time: " + new Date().toString());
				
				
		/*Connection c = null;
		
		  
		System.out.println("Merging...");
		System.out.println("Starting time: " + new Date().toString());
			
		CSMerger merger = new CSMerger();
		merger.mergeExistingRelationalDB("non_lubm", "merge_test", "195.251.63.129");
		System.out.println("Ending time: " + new Date().toString());
		
		if(true) return;*/


	}
	
	public static Map<NewCS, Set<NewCS>> getImmediateAncestors(Map<NewCS, Set<NewCS>> ancestors){
		Map<NewCS, Set<NewCS>> immediateAncestors = new HashMap<NewCS, Set<NewCS>>();
		for(NewCS parent : ancestors.keySet()) {
			
			Set<NewCS> children = ancestors.get(parent) ;
			Set<NewCS> toRemove = new HashSet<NewCS>();
			for(NewCS nextChild : children) {
									
				for(NewCS potentialParent : children) {
					
					if(nextChild.equals(potentialParent)) continue;
					if(nextChild.getAsList().containsAll(potentialParent.getAsList())){
						//toBreak = true;
						//break ;
						toRemove.add(nextChild);
						break ;
					}
				}					
			}
			children.removeAll(toRemove) ;
			immediateAncestors.put(parent,children ) ;
			
			
		}
		
		return immediateAncestors;
	}
	
	private static Map<List<NewCS>, Integer> updateCardinality(List<NewCS> next,
			Map<NewCS, Integer> csSizes, Map<List<NewCS>, Integer> pathCosts) {
		int newCardinality = 0;
		
		for(NewCS innerCS : next){
			
			newCardinality += csSizes.get(innerCS);
			
		}
		pathCosts.put(next, newCardinality) ;
		
		return pathCosts;
		
	}
	
	public static Set<List<NewCS>> findPaths(Set<NewCS> denseCSs, 
			Map<List<NewCS>, Integer> pathCosts, 
			Map<NewCS, Integer> csSizes, 
			Map<NewCS, Set<NewCS>> reverseImmediateAncestors, 
			boolean denseCheck,
			boolean withSiblings){
		Stack<List<NewCS>> stack ;
		List<NewCS> path ;
		List<NewCS> cur ;
		NewCS curCS ;
		int cardinality ;
		List<NewCS> newCur ;
		Set<List<NewCS>> foundPaths = new HashSet<List<NewCS>>();
		
		for(NewCS nextDenseCS :  denseCSs){ 
			
			stack = new Stack<List<NewCS>>();
						
			path = new ArrayList<NewCS>();
			path.add(nextDenseCS);
			stack.push(path);		
			Set<NewCS> visited = new HashSet<NewCS>();
			
			while(!stack.empty()){
				
				cur = stack.pop();
				curCS = cur.get(cur.size()-1);
				
				if(withSiblings && visited.contains(curCS)) continue;
				//if no parents, is root, add path
				if(reverseImmediateAncestors.get(curCS).isEmpty()){
					//no parents and no dense node reached.
					//has it become dense?
					cardinality = 0;
					for(NewCS node : cur){
						cardinality += csSizes.get(node);
						//added.add(node);
					}
					foundPaths.add(cur);
					pathCosts.put(cur, cardinality);
					
					continue;
				}
				
				if(!reverseImmediateAncestors.get(curCS).isEmpty()){
											
					for(NewCS parent : reverseImmediateAncestors.get(curCS)){							
						if(denseCheck && denseCSs.contains(parent)) {
							
							//it already contains a dense node so just add it.
							foundPaths.add(cur);
							
							cardinality = 0;
							for(NewCS node : cur){
								cardinality += csSizes.get(node);
							}
							pathCosts.put(cur, cardinality);

							continue;
						}
						newCur = new ArrayList<NewCS>(cur);
						newCur.add(parent);	
						if(withSiblings)
							visited.add(parent);
						stack.push(newCur);
					}
				}
				
			}
			
		}
		
		return foundPaths;
		
	}
	
	static private double jaccardSimilarity(List<Integer> a1, List<Integer> b1) {
	  
		Set<Integer> a = new HashSet<Integer>(a1);
		Set<Integer> b = new HashSet<Integer>(b1);
	    final int sa = a.size();
	    final int sb = b.size();
	    a.retainAll(b);
	    final int intersection = a.size();
	    return 1d / (sa + sb - intersection) * intersection;
	}
}
