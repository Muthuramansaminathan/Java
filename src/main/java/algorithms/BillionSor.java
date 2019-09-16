import java.awt.image.MultiPixelPackedSampleModel;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;


public class BillionSort {

    public static final String INPUT_FILE_NAME = "billion_random_numbers.dat";
    private static long totalNumbers = 1000_000_000L;
    private static long chunkPercentage = 5;
    
    private static int maxThreads = 3 * Runtime.getRuntime().availableProcessors() + 1;
    //private static List<MappedByteBuffer>  buffersList = new ArrayList<>();
  
    public static void main(String[] args) throws Exception {

        System.out.println(new SimpleDateFormat("HH:mm:ss:SSS").format(new Date()));
        if (totalNumbers < 1 || totalNumbers > 1_000_000_000L) {
            return;
        }
        Random random = new Random();
        long totalMemory = Runtime.getRuntime().freeMemory();
        System.out.println("available mem in MB " + totalMemory/(1024*2014));
        long bytesPerChunk = totalMemory * chunkPercentage / (100);
        long entriesPerChunk = bytesPerChunk/ 8;
        bytesPerChunk = entriesPerChunk * 8;

        RandomAccessFile memoryMappedSortedFile = new RandomAccessFile(INPUT_FILE_NAME, "rw");
    
        List<MappedByteBuffer> fileChannelsArray = new ArrayList<>();
        long numberOfChunks = (totalNumbers < entriesPerChunk) ? 1: (totalNumbers / entriesPerChunk);

        long filePointerBeginningPosition = 0;
        int i = 0;
        // Create array of file chunks of the input file
        long bytesSoFar = 0;
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
        while (i < numberOfChunks - 1 )    {
            bytesSoFar += bytesPerChunk;
            //System.out.println(i + " newfile from " + filePointerBeginningPosition + " to " + bytesSoFar);
            MappedByteBuffer newFileChannel = memoryMappedSortedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, filePointerBeginningPosition, bytesPerChunk);
            filePointerBeginningPosition += bytesPerChunk;
            fileChannelsArray.add(i, newFileChannel);
            ++i;
        }

        //the last chunk may not have the same number of elements as the chunk size
        long entriesInLastChunk =  totalNumbers - ((numberOfChunks -1) * entriesPerChunk);
        MappedByteBuffer newFileChannel = memoryMappedSortedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, filePointerBeginningPosition, entriesInLastChunk * 8);
        //System.out.println("last file count : " + numberOfChunks + ", numbers in last file  " + entriesInLastChunk);
        fileChannelsArray.add(i, newFileChannel);
        // (Number CPUs + 1 )  * 2 threads to create and sort random numbers

        List<Callable<Long>> generators = new ArrayList<>();
        i = 0;
        long total = 0;
        while  (i < numberOfChunks) {
            total +=  i == numberOfChunks - 1 ? entriesInLastChunk : entriesPerChunk;
            generators.add(new WriterTask((i == numberOfChunks - 1 ? entriesInLastChunk : entriesPerChunk),
                    fileChannelsArray.get(i),
                    random, i));
            ++i;
        }
        System.out.println("creating " + total + " random numbers") ;
        List<Future<Long>> results = executor.invokeAll(generators);
        for(Future future : results) {
            future.get();
        }
        
        System.out.println(new SimpleDateFormat("HH:mm:ss:SSS").format(new Date()))  ;
        System.out.println("Merging files") ;

        // keep merging the chunks into the target file
        // keep track of the number of elements that are being merged to avoid overflow
        mergeAllFiles(fileChannelsArray, entriesPerChunk, entriesInLastChunk);
        System.out.println(new SimpleDateFormat("HH:mm:ss:SSS").format(new Date()));

       System.out.println(" Done");
    }


    /**
     * Merge multiple memory mapped files in a single go.
     * Send the last File's size separately.
     * fileChannelArray - list of memery mapped files to be merged in ascending order
     */
    private static void mergeAllFiles(List<MappedByteBuffer> fileChannelsArray, long commonFileSize, long sizeOfLastFile) {
        int noOfFilesToBeMerged = fileChannelsArray.size();
        long[] entriesInFile = new long[noOfFilesToBeMerged];

        Arrays.fill(entriesInFile, commonFileSize);
        entriesInFile[noOfFilesToBeMerged - 1] = sizeOfLastFile;

        long[] filePositions = new long[noOfFilesToBeMerged];
        Arrays.fill(filePositions, 0L);

        int mergedFiles = 0;
        int minValueFileIndex = 0;
        ArrayList<Integer> startIndexHelper = new ArrayList<Integer>();
        long[] currentValueFromFiles = new long[noOfFilesToBeMerged];
        boolean[] merged = new boolean[noOfFilesToBeMerged];

        for (int i = 0; i < noOfFilesToBeMerged -1 ; ++i) {
                filePositions[i] += 1;
                startIndexHelper.add(i);
                currentValueFromFiles[i] = fileChannelsArray.get(i).getLong();
        }
        long total = 0;
        while (mergedFiles < noOfFilesToBeMerged) {

            Long minValue = null;
            int startIndex ;
            for (startIndex = 0; startIndex < noOfFilesToBeMerged; ++startIndex)  {
                if (!merged[startIndex]) {
                    minValue = currentValueFromFiles[startIndex];
                    minValueFileIndex = startIndex;
                    break;
                }
            };

            long min = minValue;
            for (int i = startIndex + 1; i < noOfFilesToBeMerged; ++i ) {
               if (!merged[i] && min > currentValueFromFiles[i]) {
                   min = currentValueFromFiles[i];
                   minValueFileIndex = i;
               }
            }

            if (filePositions[minValueFileIndex] < entriesInFile[minValueFileIndex]) {
                filePositions[minValueFileIndex] += 1;
                currentValueFromFiles[minValueFileIndex] = fileChannelsArray.get(minValueFileIndex).getLong();
            } else {
                // content of this file is completely merged. Ignore this file from futher merging.
                //System.out.println(" current count :" + filePositions[minValueFileIndex]     + " total count in file :" + entriesInFile[minValueFileIndex]);
                merged[minValueFileIndex] = true;
                ++mergedFiles;
            }

            ++total;
        }
        System.out.println(" sorted " + total + " entries");//use this to reconcile the count of records
    }

}

// create a set of random numbers and sort them using priority queue
// number of elements to be lesser to avoid crash
class WriterTask implements Callable<Long>    {
    long maxEntries;
    MappedByteBuffer out;
    Random random;
    long index;
    long range = 10000L;
    public WriterTask (long count, MappedByteBuffer fileChannel, Random rand, long ind) {
        maxEntries = count;
        out = fileChannel;
        random = rand;
        index = ind;
    }

    @Override
    public Long call() {
        long count  = 0;
        long max = maxEntries;
        PriorityQueue<Long> queue = new PriorityQueue<>();
        long val;
       // System.out.println( " total size :" + max + " file : " + index) ;
        while (count < max) {
            val = (long)(random.nextDouble()*range);
            queue.add(val);
            //queue.add(random.nextLong());
            ++count;
        }
        int i = 0;
        while(!queue.isEmpty())  {
            val = queue.remove();
            try {
                out.putLong(val);
            }
            catch (Exception e) {
                System.out.println(" current index : " + i + " total size :" + max + " file : " + index) ;
                throw e;
            }
            ++i;
        }
        out.flip();
        out.position(0);

        System.out.println(" chunk " + index + " created with " + count + " entries");
        return count;
    }

}
