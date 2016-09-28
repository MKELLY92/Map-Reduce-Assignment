package ThreadingImpl;

//Matthew Kelly 12393001
//Ronan Carr 12363236

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapReduce {

	public static void main(String[] args) {
		final Map<String, String> input = new HashMap<String, String>();
		
		long inputStartTime = System.currentTimeMillis();
		for (String s : args) {
			try {
				String currText = new String(Files.readAllBytes(Paths.get(s)));
				String[] sArr = s.split("/");
				input.put(sArr[sArr.length-1], currText);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		long inputFinishTime = System.currentTimeMillis();

		long inputTimeTaken = inputFinishTime - inputStartTime;
		System.out.println("TIME TAKEN TO INPUT TEXT FILES : " + inputTimeTaken + "\n");

		// APPROACH #3: Distributed MapReduce
		{
			final long overallStartTime = System.currentTimeMillis();
			final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			// MAP:
			
			final long mapStartTime = System.currentTimeMillis();
			final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {

				@Override
				public synchronized void mapDone(String file, List<MappedItem> results) {
					mappedItems.addAll(results);
				}
			};

			List<Thread> mapCluster = new ArrayList<Thread>(input.size());

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			int mappingRunnables = 0;
			while (inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				final String file = entry.getKey();
				final String contents = entry.getValue();
				mappingRunnables++;
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						map(file, contents, mapCallback);
					}
				});
				mapCluster.add(t);
				t.start();
			}
			System.out.println("No. of mapping runnable tasks: "+mappingRunnables);
			System.out.println("No. of mapping threads: "+mapCluster.size());
			// wait for mapping phase to be over:
			for (Thread t : mapCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			final long mapFinishTime = System.currentTimeMillis();
			final long mapTimeTaken = mapFinishTime - mapStartTime;
			System.out.println("TIME TAKEN FOR MAPPING "+mapTimeTaken+"\n");
			
			// GROUP: 

			//takes the map of all items that have been returned from the mapping phase which will be single 
			//occurrences of each word.
			//group the files in which each word occurs into the same string.
			//with the key of the map being the word and the value for the key being the list of files 
			//in which the word occurs, there will be multiple occurrences of filenames if the word appears more than once 
			//in the file.
			Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			while (mappedIter.hasNext()) {
				MappedItem item = mappedIter.next();
				String word = item.getWord();
				String file = item.getFile();
				List<String> list = groupedItems.get(word);
				if (list == null) {
					list = new LinkedList<String>();
					groupedItems.put(word, list);
				}
				list.add(file);
			}

			// REDUCE:
			final long reduceStartTime = System.currentTimeMillis();
			final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {

				//will put the results from the reduce call into the output map.
				@Override
				public synchronized void reduceDone(String k, Map<String, Integer> v) {
					output.put(k, v);
				}
			};

			List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			int reduceRunnables = 0;
			while (groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				final String word = entry.getKey();
				final List<String> list = entry.getValue();
				reduceRunnables++;
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						reduce(word, list, reduceCallback);
					}
				});
				reduceCluster.add(t);
				t.start();
			}
			System.out.println("No. of reduce runnable tasks: "+ reduceRunnables);
			System.out.println("No. of reduce threads: "+ reduceCluster.size());
			// wait for reducing phase to be over:
			for (Thread t : reduceCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			final long reduceFinishTime = System.currentTimeMillis();
			final long reduceTimeTaken = reduceFinishTime - reduceStartTime;
			System.out.println("TIME TAKEN FOR EEDUCING"+reduceTimeTaken+"\n");
			final long overallFinishTime = System.currentTimeMillis();
			final long overallTimeTaken = overallFinishTime - overallStartTime;
			System.out.println("Total Execution Time: " + overallTimeTaken);
			
			try {
				PrintWriter w = new PrintWriter("/MapReduceAssignment/src/Output/ThreadingImplOut.txt", "UTF-8");
				w.write("Total Mapping Time: "+mapTimeTaken+"\nTotal Reducing Time: "+reduceTimeTaken+"\nTotal Execution Time: "+overallTimeTaken+"\nOutput:"+output);
				w.close();
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			
		}

	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	
	//take in the filename and the text of the file.
	//split into words using trim
	//create an empty hashmap.
	//will map every word even in the file, even if the word is repeated.
	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for (String word : words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K, V> results);
	}

	
	//takes in a word, and all the individual occurrences of the word in ALL files, can 
	//be repeated files in list for multiple occurrences of the word in the file. 
	//Then creates a new HashMap and goes through the list of occurrences and
	//reduces the multiple file names and puts in value to represent multiple occurrences.
	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	private static class MappedItem {

		private final String word;
		private final String file;

		public MappedItem(String word, String file) {
			this.word = word;
			this.file = file;
		}

		public String getWord() {
			return word;
		}

		public String getFile() {
			return file;
		}

		@Override
		public String toString() {
			return "[\"" + word + "\",\"" + file + "\"]";
		}
	}
}
