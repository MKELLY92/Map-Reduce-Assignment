package ThreadSafe;

//Matthew Kelly 12393001
//Ronan Carr 

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MapReduce {

	public static void main(String[] args) {

		// INPUT:
		
		final Map<String, String> input = new HashMap<String, String>();
		int poolSize = Integer.parseInt(args[0]);
		System.out.println("THREAD POOL SIZE : " + poolSize);

		long inputStartTime = System.currentTimeMillis();
		for (String s : Arrays.copyOfRange(args, 1, args.length)) {
			try {
				String currText = new String(Files.readAllBytes(Paths.get(s)));
				String[] sArr = s.split("/");
				input.put(sArr[sArr.length-1], currText);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		long inputFinishTime = System.currentTimeMillis();

		long timeTaken = inputFinishTime - inputStartTime;
		System.out.println("TIME TAKEN TO INPUT TEXT FILES : " + timeTaken + "\n");

		// Thread Safe Distributed Map Reduce
		{
			long overallStartTime = System.currentTimeMillis();
			final ConcurrentHashMap<String, Map<String, Integer>> output = new ConcurrentHashMap<String, Map<String, Integer>>();

			// MAP:
			long mapStartTime = System.currentTimeMillis();
			final CopyOnWriteArrayList<MappedItem> mappedItems = new CopyOnWriteArrayList<MappedItem>();

			ExecutorService executor = Executors.newFixedThreadPool(poolSize);
			int mappingRunnables = 0;

			for (Map.Entry<String, String> e : input.entrySet()) {

				final String key = e.getKey();
				final String val = e.getValue();
				mappingRunnables++;
				executor.submit(new Runnable() {
					@Override
					public void run() {
						map(key, val, mappedItems);
					}
				});
			}
			System.out.println("No. of mapping runnable tasks: " + mappingRunnables);
			// wait for all tasks to finish then stop service
			executor.shutdown();
			try {
				executor.awaitTermination(1, TimeUnit.DAYS);
			} catch (InterruptedException e1) {
			}
			long mapEndTime = System.currentTimeMillis();
			long mapTimeTaken = mapEndTime - mapStartTime;
			System.out.println("Time Taken To Map : " + mapTimeTaken + "\n");

			// GROUP:
			Map<String, ArrayList<String>> groupedItems = new HashMap<String, ArrayList<String>>();

			for (MappedItem mI : mappedItems) {
				String file = mI.getFile();
				String word = mI.getWord();

				ArrayList<String> list = groupedItems.get(word);
				if (list == null) {
					list = new ArrayList<String>();
					groupedItems.put(word, list);
				}
				list.add(file);

			}

			// REDUCE:

			long reduceStartTime = System.currentTimeMillis();
			ExecutorService reduceExec = Executors.newFixedThreadPool(poolSize);
			int redTasks = 0;

			for (Map.Entry<String, ArrayList<String>> e : groupedItems.entrySet()) {
				final String key = e.getKey();
				final ArrayList<String> val = e.getValue();
				redTasks++;

				reduceExec.submit(new Runnable() {

					@Override
					public void run() {
						reduce(key, val, output);
					}
				});
			}

			System.out.println("# OF REDUCE TASKS : " + redTasks);
			reduceExec.shutdown();

			// wait for reducing phase to be over:
			try {
				reduceExec.awaitTermination(1, TimeUnit.DAYS);
			} catch (InterruptedException e1) {
			}

			long reduceEndTime = System.currentTimeMillis();
			long reduceTimeTaken = reduceEndTime - reduceStartTime;
			System.out.println("TIME TAKEN FOR REDUCING: " + reduceTimeTaken + "\n");

			long overallEndTime = System.currentTimeMillis();
			long overallTimeTaken = overallEndTime - overallStartTime;
			System.out.println("OVERALL TIME TAKEN: " + overallTimeTaken);

			try {
				PrintWriter w = new PrintWriter("/MapReduceAssignment/src/Output/ThreadSafeOut.txt", "UTF-8");
				w.write("TIME TAKEN FOR MAPPING : " + mapTimeTaken + "\n TIME TAKEN FOR REDUCING : " + reduceTimeTaken
						+ "\n OVERALL TIME TAKEN : " + overallTimeTaken + "\n Output:" + output);
				w.close();
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		}
	}

	public static void map(String file, String contents, CopyOnWriteArrayList<MappedItem> mappedItems) {
		String[] words = contents.trim().split("\\s+");
		for (String word : words) {
			mappedItems.add(new MappedItem(word, file));
		}
	}

	public static void reduce(String word, List<String> list, ConcurrentHashMap<String, Map<String, Integer>> output) {
		ConcurrentHashMap<String, Integer> reducedList = new ConcurrentHashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		output.put(word, reducedList);
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
			return "{\"" + word + "\",\"" + file + "\"}\n";
		}
	}
}
