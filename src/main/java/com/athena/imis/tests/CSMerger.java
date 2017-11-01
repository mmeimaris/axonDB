package com.athena.imis.tests;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.athena.imis.models.NewCS;

public class CSMerger {
	
	Connection c ;
	
	public void mergeExistingRelationalDB(String existingDB, String newDB, String host){				
				
		try {
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+host+":5432/"+existingDB, "mmeimaris",	        		          
	        				 "dirtymarios");	            
	         System.out.println("Opened database successfully");
	         	      
			Statement st = c.createStatement();
			Map<NewCS, Set<NewCS>> ancestors = new HashMap<NewCS, Set<NewCS>>();
			
			String csQuery = " SELECT id, properties FROM cs_schema ;";
			ResultSet rsCS = st.executeQuery(csQuery);
			
			Map<NewCS, Integer> csMap = new HashMap<NewCS, Integer>();
			
			while(rsCS.next()){
				Array parentArray = rsCS.getArray(2);				
				NewCS parentCS = new NewCS((Integer[])parentArray.getArray()) ;				
				csMap.put(parentCS, rsCS.getInt(1)) ;
				ancestors.put(parentCS, new HashSet<NewCS>()) ;				
			}
			rsCS.close();
			st.close();
			
			
			Map<NewCS, Integer> csSizes = new HashMap<NewCS, Integer>();
			for(NewCS nextCS : csMap.keySet()){
				Integer csId = csMap.get(nextCS);
				String csSizeQuery = " SELECT reltuples::bigint AS estimate FROM pg_class where relname='cs_"+csId+"';";
				st = c.createStatement();
				ResultSet rsCSsizes = st.executeQuery(csSizeQuery);
												
				while(rsCSsizes.next()){
										
					csSizes.put(nextCS, rsCSsizes.getInt(1));
				}
				rsCSsizes.close();
				st.close();
			}
			
			
			st = c.createStatement();
			String ancestryQuery = " SELECT parent.properties, child.properties FROM cs_schema as parent JOIN cs_schema as child ON "
					+ "parent.properties <@ child.properties WHERE parent.id != child.id ;";
			ResultSet rsAncestry = st.executeQuery(ancestryQuery);
			
			
			
			while(rsAncestry.next()){
				Array parentArray = rsAncestry.getArray(1);
				Array childArray = rsAncestry.getArray(2);
				//Integer[] asArray = (Integer[])parentArray.getArray();
				NewCS parentCS = new NewCS((Integer[])parentArray.getArray()) ;
				NewCS childCS = new NewCS((Integer[])childArray.getArray()) ;
				Set<NewCS> children = ancestors.getOrDefault(parentCS, new HashSet<NewCS>());
				children.add(childCS);
				ancestors.put(parentCS, children);
				//System.out.println(parentCS.toString() + " -> " + childCS.toString());
			}
			rsAncestry.close();
			st.close();
			
			
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
			for(NewCS parent : immediateAncestors.keySet()){
				System.out.println(parent.toString()) ;
				for(NewCS child : immediateAncestors.get(parent))
					System.out.println("\t" +child.toString()) ;
			}
			
			int total = 0;
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
				if(csSizes.get(nextCS) >= total/csMap.size()/2){
					denseCS++;
					denseCSs.add(nextCS);
					totalDenseRows += csSizes.get(nextCS);
				}
				
			}
			System.out.println("Total CSs: " + csMap.size());
			System.out.println("Dense CSs: " + denseCS);
			for(NewCS nextDense : denseCSs){
				System.out.println("\t"+nextDense.toString());
			}
			System.out.println("Dense CS Coverage: " + totalDenseRows);						
			
			System.out.println("\n\n\n\n");
			
			Set<List<NewCS>> foundPaths = new HashSet<List<NewCS>>();
			
			HashMap<List<NewCS>, Integer> pathCosts = new HashMap<List<NewCS>, Integer>();
			
			for(NewCS nextDenseCS :  csMap.keySet()){ //denseCSs
				
				Stack<List<NewCS>> stack = new Stack<List<NewCS>>();
				
				//Set<NewCS> visited = new HashSet<NewCS>();
				List<NewCS> path = new ArrayList<NewCS>();
				path.add(nextDenseCS);
				stack.push(path);
				
				List<NewCS> cur ;
				
				while(!stack.empty()){
					
					cur = stack.pop();
					NewCS curCS = cur.get(cur.size()-1);

					if(denseCSs.contains(curCS)){
						foundPaths.add(cur);
						
						int cardinality = 0;
						for(NewCS node : cur){
							cardinality += csSizes.get(node);
						}
						
						pathCosts.put(cur, cardinality);
						System.out.println((double)csSizes.get(cur.get(0))/cardinality+", " + cardinality+" " + cur.toString());
						continue;
					}
					if(reverseImmediateAncestors.get(curCS).isEmpty()){
						//no parents and no dense node reached.
						//has it become dense?
						int cardinality = 0;
						for(NewCS node : cur){
							cardinality += csSizes.get(node);
						}
						if(cardinality >= total/csMap.size()/2){
							foundPaths.add(cur);
							pathCosts.put(cur, cardinality);
						}
						else continue;
					}
					
					if(!reverseImmediateAncestors.get(curCS).isEmpty()){
												
						for(NewCS parent : reverseImmediateAncestors.get(curCS)){							
							
							List<NewCS> newCur = new ArrayList<NewCS>(cur);
							newCur.add(parent);							
							stack.push(newCur);
						}
					}
					
				}
				
			}
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
			for(List<NewCS> nextPath : foundPaths){
				System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
			}
			//System.out.println("\n\n\n\n");
			
			List<List<NewCS>> orderedPaths = new ArrayList<List<NewCS>>(foundPaths);			
			Collections.sort(orderedPaths, new Comparator<List<NewCS>>() {

		        public int compare(List<NewCS> o1, List<NewCS> o2) {
		            if (pathCosts.get(o1) > pathCosts.get(o2)) return -11;
		            else if (pathCosts.get(o1) < pathCosts.get(o2)) return 1;
		            else return 0;
		            		
		        }
		    });
			
			for(List<NewCS> nextPath : orderedPaths){
				System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
			}
			List<List<NewCS>> finalList = new ArrayList<List<NewCS>>();

			int totalIterations = 0;					
			
			for(int i = 0; i < orderedPaths.size(); i++){
				
				List<NewCS> cur = orderedPaths.get(i);
												
				for(int k = i+1; k < orderedPaths.size(); k++){
					
					List<NewCS> next = orderedPaths.get(k);
					
					next.removeAll(cur);
					updateCardinality(next, csSizes, pathCosts);	
					
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
			System.out.println("\n\n\n\n\n");
			for(List<NewCS> nextPath : finalList){
				System.out.println(pathCosts.get(nextPath)+": " + nextPath.toString()) ;
			}
			System.out.println(finalUnique.size());
			System.out.println(finalList.size() + ": " + finalList.toString());
			
			int notCovered = 0;
			for(NewCS nextCS : csMap.keySet()){
				if(!finalUnique.contains(nextCS)){
					System.out.println("Not contained: ("+csSizes.get(nextCS)+")" + nextCS.toString()) ;
					notCovered += csSizes.get(nextCS);
				}
			}
			System.out.println("Not covered : " + notCovered); 
			
			int totalCovered = 0;
			
			for(List<NewCS> nextPath : finalList){
				
				System.out.println("next path: " + nextPath );
				totalCovered += pathCosts.get(nextPath);
				
			}
			System.out.println(totalCovered) ;
			c.close();
			
			c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+host+":5432/"+newDB, "mmeimaris",	        		          
	        				 "dirtymarios");          
	         System.out.println("Opened database successfully");
	         
	         
	         
			c.close();
		}catch (Exception e){
				e.printStackTrace();
				return ;
				
			}
		}
		
		private HashMap<List<NewCS>, Integer> updateCardinality(List<NewCS> next,
				Map<NewCS, Integer> csSizes, HashMap<List<NewCS>, Integer> pathCosts) {
			int newCardinality = 0;
			
			for(NewCS innerCS : next){
				
				newCardinality += csSizes.get(innerCS);
				
			}
			pathCosts.put(next, newCardinality) ;
			
			return pathCosts;
			
		}
		

}
