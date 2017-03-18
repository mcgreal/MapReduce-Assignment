package assign2;

import java.util.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MapReduce {

    public static void main(String[] args) throws IOException {
        //Array of filenames to be tested
        long startTime = System.nanoTime();
        try{
            int threadPoolSize = Integer.parseInt(args[0]);
            mapReduce(threadPoolSize);
        } catch (ArrayIndexOutOfBoundsException e){
            System.err.println("ERROR: No thread pool size argument provided");
            e.printStackTrace();
        }
    }

    private static void mapReduce(int threadPoolSize) throws IOException {
        long startTime = System.nanoTime();
        String[] files = {"bakerStreet.txt", "fish.txt", "singles.txt"};

        Map<String, String> input = addFilesContentToInputHashmap(files);

        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

            for (Map.Entry<String, String> entry : input.entrySet()) {
                final String file = entry.getKey();
                final String contents = entry.getValue();

                threadPool.execute(() -> map(file, contents, mapCallback));
            }

            threadPool.shutdown();
            try{
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!threadPool.isTerminated()) {}

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<>();

            for (MappedItem item : mappedItems) {
                String entry = item.getFirstLetter();
                String file = item.getFile();
                List<String> list = groupedItems.get(entry);
                if (list == null) {
                    list = new LinkedList<>();
                    groupedItems.put(entry, list);
                }
                list.add(file);
            }

            threadPool = Executors.newFixedThreadPool(threadPoolSize);

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                threadPool.execute(() -> reduce(word, list, reduceCallback));
            }

            threadPool.shutdown();
            // Add timeont of 1 minute to thread pool
            try{
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!threadPool.isTerminated()) {}long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000;
            System.out.println(output);
            System.out.println("Total Duration: " + duration + "us");
        }
    }

    private static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        char firstLetter;
        String entry;
        List<MappedItem> results = new ArrayList<>(words.length);
        for (String word : words) {
            firstLetter = word.charAt(0);
            entry = Character.toString(firstLetter);
            results.add(new MappedItem(entry, file));
        }
        callback.mapDone(file, results);
    }

    private static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }

        callback.reduceDone(word, reducedList);
    }

    private static Map<String, String> addFilesContentToInputHashmap(String[] files) throws IOException{
        Map<String, String> input = new HashMap<>();
        for (String file : files) {
            //BufferedReader with FileReader created to read the contents of the given file
            BufferedReader in = new BufferedReader(new FileReader(file));
            String line;
            String[] lineSplit;
            String fileContent = "";
            //The file is read line-by-line
            while ((line = in.readLine()) != null) {
                //The lines are split on spaces and the words are stored in an array called lineSplit
                lineSplit = line.split("\\s+");
                //For each element of lineSplit all non-letters are replaced with blank space
                //All the letters are converted to upper case so that, for example, capital A and small a are not treated as separate letters
                for (int j = 0; j < lineSplit.length; j++) {
                    lineSplit[j] = lineSplit[j].toUpperCase().replaceAll("[^a-zA-Z]", "");
                    //Each element of the lineSplit array are added to one string called fileContent
                    //The elements are separated by a space
                    fileContent += lineSplit[j] + " ";
                }
            }
            //The file name and the file's content are then added to the HashMap input
            input.put(file, fileContent);
        }
        return input;
    }
}