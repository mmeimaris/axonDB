package com.athena.imis.tests;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class RandomTesting {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int size = 1000000, chunk = 1000;
		long[] data = new long[size];
		for(int i = 0; i < size ; i ++){
			data[size-i-1] = (long) i;
		}
		long min1 = Long.MAX_VALUE, min2 = Long.MAX_VALUE, min3 = Long.MAX_VALUE;
		System.out.println("Test data created. Size: " + data.length);
		
		DB db = DBMaker.fileDB(new File("c:/temp/random.db"))
				//.transactionDisable()
 				.fileChannelEnable() 			
 				.fileMmapEnable() 				
 				//.asyncWriteFlushDelay(5000)
 				//.cacheSize(32768*16)
 				.closeOnJvmShutdown() 				
 				.make();
		
		Map<Integer, Long> map = db.treeMap("map")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG) 				
 				//.valuesOutsideNodesEnable()
 				.createOrOpen();
		
		long start = System.nanoTime();
		
		for(int i = 0; i < size; i ++){
			map.put(i, data[i]);
		}
		
		long end = System.nanoTime();
		
		System.out.println("DB Map populated in: " + (end-start));
		long dum = 0;
		
		for(int j = 0; j < 10; j++){
			int count = 0;
			start = System.nanoTime();
			
			for(int i = 0; i < size; i ++){
				dum = map.get(i)+1;
				count++;
			}
			
			end = System.nanoTime();
			min1 = Math.min(min1, end-start);
			//System.out.println("All items (" + count + ") retrieved in: " + (end-start));
		}
		
		Map<Integer, long[]> map2 = db.treeMap("map2")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY) 				
 				.createOrOpen();
		
		start = System.nanoTime();
		
		for(int i = 0; i < size/chunk; i ++){
			long[] sub = new long[chunk];
			for(int k = 0; k < chunk; k++){
				sub[k] = data[i*chunk+k];
			}
			map2.put(i, sub);
		}
		
		end = System.nanoTime();
		
		System.out.println("DB Map2 populated in: " + (end-start));
		for(int j = 0; j < 10; j++){
			int count = 0;
			start = System.nanoTime();
			
			for(int i = 0; i < size/chunk; i ++){
				long[] d = map2.get(i);
				for(int l = 0; l < d.length; l++){
					dum = d[l] + 1;
					count++;
				}
				
			}
			
			end = System.nanoTime();
			min2 = Math.min(min2, end-start);
			//System.out.println("All items (" + count + ") retrieved in: " + (end-start));
		}
		
		Set<Long> map3 = db.treeSet("map3")
 				.serializer(Serializer.LONG) 				 			
 				.createOrOpen();
		
		start = System.nanoTime();
		
		for(int i = 0; i < size; i ++){
			
			map3.add(data[i]);
		}
		
		end = System.nanoTime();
		
		System.out.println("DB Map3 populated in: " + (end-start));
		for(int j = 0; j < 10; j++){
			int count = 0;
			start = System.nanoTime();
			
			for(Long i : map3){
				dum = i+1;
				count++;
			}
			
			end = System.nanoTime();
			
			//System.out.println("All items (" + count + ") retrieved in: " + (end-start));
			min3 = Math.min(min3, end-start);
		}
		System.out.println("min1: " + min1);
		System.out.println("min2: " + min2);
		System.out.println("min3: " + min3);
		db.close();
	}

}
