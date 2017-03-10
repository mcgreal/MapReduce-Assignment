package assign2;

import java.util.*;
import java.io.*;

public class MapReduce {
        
    public static void main(String[] args) throws IOException {
                    
        Map<String, String> input = new HashMap<String, String>();
        
        //Array of filenames to be tested
        String[] files = {"bakerStreet.txt", "fish.txt", "singles.txt"};
        
        //String[] files = {"test.txt"};
        
        //Each file is read in as part of a for loop
        for(int i=0; i<files.length; i++){
        	//BufferedReader with FileReader created to read the contents of the given file
        	BufferedReader in = new BufferedReader(new FileReader(files[i]));
            String line = "";
            String[] lineSplit = null;
            String fileContent="";
            //The file is read line-by-line 
            while ((line = in.readLine()) != null) {
            	//The lines are split on spaces and the words are stored in an array called lineSplit
            	lineSplit = line.split("\\s+");
            	//For each element of lineSplit all non-letters are replaced with blank space
            	//All the letters are converted to upper case so that, for example, capital A and small a are not treated as separate letters
            	for(int j=0; j<lineSplit.length; j++){
                    lineSplit[j]=lineSplit[j].toUpperCase().replaceAll("[^a-zA-Z]", "");
                    //Each element of the lineSplit array are added to one string called fileContent
                    //The elements are separated by a space
            		fileContent+=lineSplit[j]+" "; 
            	}
            } 
            //The file name and the file's content are then added to the HashMap input
            input.put(files[i].toString(), fileContent);
        }
            // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                    
                    // MAP:
                    
                    final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                    
                    final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                            @Override
                            public synchronized void mapDone(String file, List<MappedItem> results) {
                            	mappedItems.addAll(results);
                            }
                    };
                    
                    List<Thread> mapCluster = new ArrayList<Thread>(input.size());
                    
                    Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                    while(inputIter.hasNext()) {
                            Map.Entry<String, String> entry = inputIter.next();
                            final String file = entry.getKey();
                            final String contents = entry.getValue();
                            
                            Thread t = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                            map(file, contents, mapCallback);
                                    }
                            });
                            mapCluster.add(t);
                            t.start();
                    }
                    
                    // wait for mapping phase to be over:
                    for(Thread t : mapCluster) {
                            try {
                                    t.join();
                            } catch(InterruptedException e) {
                                    throw new RuntimeException(e);
                            }
                    }
                    
                    // GROUP:
                    
                    Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                    
                    Iterator<MappedItem> mappedIter = mappedItems.iterator();
                    while(mappedIter.hasNext()) {
                            MappedItem item = mappedIter.next();
                            String entry = item.getFirstLetter();
                            String file = item.getFile();
                            List<String> list = groupedItems.get(entry);
                            if (list == null) {
                                    list = new LinkedList<String>();
                                    groupedItems.put(entry, list);
                            }
                            list.add(file);
                    }
                    
                    // REDUCE:
                    
                    final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                            @Override
                            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                            	output.put(k, v);
                            }
                    };
                    
                    List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
                    
                    Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                    while(groupedIter.hasNext()) {
                            Map.Entry<String, List<String>> entry = groupedIter.next();
                            final String word = entry.getKey();
                            final List<String> list = entry.getValue();
                            
                            Thread t = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                            reduce(word, list, reduceCallback);
                                    }
                            });
                            reduceCluster.add(t);
                            t.start();
                    }
                    
                    // wait for reducing phase to be over:
                    for(Thread t : reduceCluster) {
                            try {
                                    t.join();
                            } catch(InterruptedException e) {
                                    throw new RuntimeException(e);
                            }
                    }
                    
                    System.out.println(output);
            }
    }
    
    public static interface MapCallback<E, V> {
            
            public void mapDone(E key, List<V> values);
    }
    
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
    		String[] words = contents.trim().split("\\s+");
            char firstLetter;
            String entry;
            List<MappedItem> results = new ArrayList<MappedItem>(words.length);
            for(String word: words) {
            		firstLetter=word.charAt(0);
            		entry=Character.toString(firstLetter);
            		results.add(new MappedItem(entry, file));
            }
            callback.mapDone(file, results);
    }
    
    public static interface ReduceCallback<E, K, V> {
            
            public void reduceDone(E e, Map<K,V> results);
    }
    
    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
            
            Map<String, Integer> reducedList = new HashMap<String, Integer>();
            for(String file: list) {
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
            
            private final String letter;
            private final String file;
            
            public MappedItem(String entry, String file) {
                    this.letter = entry;
                    this.file = file;
            }

            public String getFirstLetter() {
                    return letter;
            }

            public String getFile() {
                    return file;
            }
            
            @Override
            public String toString() {
                    return "[\"" + letter + "\",\"" + file + "\"]";
            }
        }
}