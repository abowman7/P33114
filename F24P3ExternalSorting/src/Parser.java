import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.EOFException;
import java.nio.*;
import java.nio.channels.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
/**
 * File to hold the input buffer and output buffer
 *
 * @author Rami Benhamida and Adam Bowman
 * @version Fall 2024
 */
public class Parser {
    /**
     * the number of records in one block
     */
    public final static int BLOCK_SIZE = 8192;
    /**
     * Size of two bytes: 16
     */
    public final static int TWO_BYTES = 16;
    /**
     * Lowest double value, used to compare last item to when no item
     */
    public final static double LOWEST = Double.MIN_VALUE;
    /**
     * half the size of a block - size of the Heap
     */
    public final static int HALF_SIZE = 4096;
    /**
     * Max number of runs you can merge at once
     */
    public final static int MAX_MERGE = 8;
    /**
     * Input and output buffers for reading / writing
     */
    private ByteBuffer inputBuffer = ByteBuffer.allocate(BLOCK_SIZE); // Creates a buffer with capacity of 8192 bytes.
    private ByteBuffer outputBuffer = ByteBuffer.allocate(BLOCK_SIZE);
    /**
     * heap used for removing min
     */
    private MinHeap<Record> heap = new MinHeap<Record>(
        new Record[HALF_SIZE], 0, HALF_SIZE);
    private int numHidden;
    private int ndx;
    private int currRun;
    private int itr;
    private String filename;
    private List<Long> runs;    //holds runs for next merge
    private List<Long> runLengths; //holds lengths calculated with each run
    private List<Long> mergeRun; // holds runs for current merge
    /**
     * Constructor
     * @param fn    File name
     */
    Parser(String fn) {
        filename = fn;
        numHidden = 0;
        ndx = 0;
        currRun = 0;
        itr = 0;
        runs = new List<Long>();
        runLengths = new List<Long>();
        runs.append((long) 0);
        mergeRun = new List<Long>();
    }
    
    /**
     * function to print the first record of each block of the sorted file
     */
    public void printInput() {
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            long fileLength = file.length();
            long numBlocks = fileLength / BLOCK_SIZE;
            int counter = 1; // counter for how many records on a line
            // iterate over each block
            for (int block = 0; block < numBlocks; block++) {
                long position = block * BLOCK_SIZE;
                file.seek(position);
                // read the id (8 bytes) and key (8 bytes)
                long id = file.readLong();
                double key = file.readDouble();
                // print the key and id
                System.out.print(id + " " + key);
                // check if new line or a space is needed
                if (counter % 5 == 0) {
                 // make new line after 5 records
                    System.out.println();
                    // reset counter for new line
                    counter = 1;
                }
                else {
                    System.out.print(" "); // for space between records
                    counter++;
                }
            }
        }
        catch (IOException e) { }
   
       
    }
    

    /**
     * used to run 8 blocks and under
     */
    public void eightBlocker() {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_MERGE * BLOCK_SIZE);
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            // read long and double pairs until reaching the end of the file
            while (file.getFilePointer() < file.length()) {
                //System.out.println(file.getFilePointer());
                long id = file.readLong();
                double key = file.readDouble();
                Record newInsert = new Record(id, key);
                heap.insert(newInsert);
            }
            //System.out.println("file len" + file.length());

        }
        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
        File newFile = new File(filename);
        newFile.delete();
        try (RandomAccessFile outputFile = new RandomAccessFile(
            filename, "rw")) {
            for (int i = 0; i < HALF_SIZE; i++) {

                heap.buildHeap();

                Record temp = heap.removeMin();

                // Write the record's long and double to the buffer

                buffer.putLong(temp.getID());

                buffer.putDouble(temp.getKey());

                // If buffer is full (65,536 bytes), write to output file and
                // clear buffer
                if (buffer.position() >= 8 * BLOCK_SIZE) {
                    buffer.flip(); // Prepare buffer for writing

                    outputFile.write(buffer.array(), 0, buffer.limit()); 
                    buffer.clear(); // Clear buffer for next chunk
                }
            }
        }
        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());

        }

    }


    /**
     * return runs list
     * @return  list of runs for current run through
     */
    public List<Long> getRuns() {
        return runs;
    }
    /**
     * return runlengths list
     * @return  list of run lengths for current run through
     */
    public List<Long> getRunLengths() {
        return runLengths;
    }
    /**
     * return mergeRuns list
     * @return  list of runs for previous run through
     */
    public List<Long> getMergeRun() {
        return mergeRun;
    }
    
    /**
     * fills empty input buffer until full or at end of file
     * @param f
     *  file used to fill buffer
     */
    public void fillInputBuf(RandomAccessFile f) {
        try {   //try/catch Exception
            inputBuffer.limit(BLOCK_SIZE); // reset limit
            //System.out.println("fillInputBuf");
            
            if (f.getFilePointer() == f.length()) {
                return;
            }
            inputBuffer.position(0);    //set input buffer pos to start
            while (inputBuffer.hasRemaining() &&    //check if at end or
                f.getFilePointer() != f.length()) { //nothing left in file
                //System.out.println(f.getFilePointer() + "   " + f.length());
                inputBuffer.putLong(f.readLong());  //read long
                inputBuffer.putDouble(f.readDouble()); //read double
            } //set limit to position as not to read previous data from buffer
            if (f.getFilePointer() >= f.length()) {
                inputBuffer.limit(inputBuffer.position());
            }
            inputBuffer.position(0);
            //System.out.println(Arrays.toString(inputBuffer.array()));
        }
        catch (IOException e) {
        }
    }
    
    /**
     * fills heap initially, will have to be called 8 times with empty 
     * fill Input Buffer upon start of program (assuming 8+ blocks)
     */
    public void fillEmptyHeap() {
        inputBuffer.position(0); //reset buffer position
        //while input buffer position isn't at end
        while (inputBuffer.hasRemaining()) {
            //put nect long and double binary into record
            Record rec = new Record(inputBuffer.getLong(), 
                inputBuffer.getDouble());
            //insert record into heap
            heap.insert(rec);
        }
    }
    
    /**
     * write from outputBuffer to runfile then clear outputbuffer
     */
    public void writeOutput() {
        try (RandomAccessFile raf = new RandomAccessFile("runfile.bin", "rw")) {
            raf.seek(raf.length()); //go to proper file location to append
            outputBuffer.limit(outputBuffer.position()); //no write extra data
            raf.write(outputBuffer.array(), 0, outputBuffer.limit()); //write
            outputBuffer.clear();   //clear buffer after written
            outputBuffer.limit(BLOCK_SIZE); //reset limit
            outputBuffer.position(0);   //reset position
        } //initialize RandomAccessFile and try/catch IOException
        catch (IOException e) { 
            System.out.println("Write Output Error.");
        }
    }
    
    /**
     * Function for replacing heap with a new Record from inputBuffer
     */
    public void replace() {
       // inputBuffer.flip();  // prepare buffer for reading
       
        // check if there are at least 16 bytes remaining 
        if (inputBuffer.remaining() >= 16) { 
            long id = inputBuffer.getLong();     // read next long (ID)
            double key = inputBuffer.getDouble(); // read next double (Key)
            //System.out.println(id +" " + key);
            // insert the record into the heap
            Record record = new Record(id, key);
            heap.modify(0, record);
        }
    }

    
    /**
     * Function for removing min from the heap
     * @param lastRemoved   Last record recorded to outputBuffer
     */
    public Record takeMinimum(Record lastRemoved) {
        Record newestMin = heap.showMin();
        // if nextMin < lastMin
        if (newestMin.compareTo(lastRemoved) < 0) {
            // hide it, increment numHidden, replace it,
            // and recursively call takeMin until a valid min is found/
            heap.removeMin();
            numHidden++;
            if (heap.heapSize() == 0) {
                try (RandomAccessFile rf = 
                    new RandomAccessFile("runfile.bin", "rw")) {
                    //System.out.println(lastRemoved.getKey());
                    handleHiddens(rf);
                    return null;
                }
                catch(IOException e) { 
                    System.out.println("File Doesn't Exist");
                }
                
            }
            return takeMinimum(lastRemoved);
        }
        // else, send in the next item from the input buffer
        replace();
        // return the newest min greater than the last removed
        return newestMin;
    }

    
    /**
     * pushes next record's ID and key to outputBuffer
     * writes to runfile if outputBuffer is full
     * @param newMin    next record to be output
     * @return newMin most recently output record
     */
    public Record heapOut(Record newMin) {
        if (newMin == null) {   //check if null
            return new Record((long) 0, LOWEST);
        }
        outputBuffer.putLong(newMin.getID());
        outputBuffer.putDouble(newMin.getKey());
        if (!outputBuffer.hasRemaining()) {
            writeOutput();
        }
        return newMin;
    }
    
    /**
     * top level function that puts everything together
     * completes one full run of file
     */
    public List<Long> mainFunction() {
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            //fill up heap initially with 8 blocks of data.
            for (int i = 0; i < MAX_MERGE; i++) {
                fillInputBuf(raf);
                fillEmptyHeap();
            }
            fillInputBuf(raf);
            //reset inputBuffer position
            inputBuffer.position(0);
            //sort file and record run start and end positions
            sortWithHeap(raf);
            //print output buffer
            writeOutput();
            
            // send rest of heap to output
            heapFinal();
            calculateRunLengths(raf, runs);
            copyRuns(); //copy runs to mergeRuns and reinitialize runs
        }
        catch (IOException e) { 
            System.out.println("No file found");
        }
        return runs;
    }
    
    /**
     * runs sorting from 
     * @param f randomAccessFile to read inputs from
     */
    public void sortWithHeap(RandomAccessFile f) {
        //lowest value for double so no matter what first input doesn't hide
        Record lastRemoved = new Record((long) 0, LOWEST); 
        //exit conditions for loop - heapSize 0
        while (inputBuffer.hasRemaining() && heap.heapSize() != 0) {
            //set last removed and run takeMinimum then heapOut: in->heap->out
            lastRemoved = heapOut(takeMinimum(lastRemoved));
            if (!inputBuffer.hasRemaining()) {
                fillInputBuf(f);  //refill inputBuffer until file is empty
            }
            if (heap.heapSize() == 0) { //handle when heap is 'empty'
                try (RandomAccessFile raf = new 
                    RandomAccessFile("runfile.bin", "rw")) {
                    handleHiddens(raf);   // only hidden data in heap
                }
                catch (IOException e) { }
                lastRemoved = new Record((long) 0, LOWEST); //reset lowest val
            }
        }
    }
    
    /**
     * handle when heap size == 0 because it is taken up by hiddens
     * or when heap size == 0 because at end of file
     * @param f RandomAccessFile 
     */
    public void handleHiddens(RandomAccessFile f) {
        try {   //try/catch IOException
            if (numHidden > 0 && heap.heapSize() == 0) {
                // add run start position and length to list
                writeOutput();
                runs.append(f.length());
                // set size to number of hidden values(will be 8 Blocks most
                // times)
                heap.setHeapSize(numHidden); // set heap size to
                numHidden = 0; // reset numHidden
                heap.buildHeap(); // rebuild heap
            }
        }
        catch (IOException e) {
            System.out.println("File not found - Hidden");
        }
    }
    
    /**
     * nothing left in input buffer
     */
    public void heapFinal() {
      //hidden data case at end
        try (RandomAccessFile f = new RandomAccessFile("runfile.bin", "rw")) {
            if (numHidden > 0) {
                heap.setHeapSize(heap.heapSize() + numHidden);
                heap.buildHeap();
                numHidden = 0;
            }
            if (f.length() != 0) {
                runs.append(f.length());
            }
        }
        catch (IOException e) { 
            System.out.println("Couldn't Access File - heap final.");
        }
        //remove from heap until empty
        while (heap.heapSize() != 0) {
            //get/remove min record and put into outputBuffer
            Record min = heap.removeMin();
            //System.out.println(min.getID());
            outputBuffer.putLong(min.getID());
            outputBuffer.putDouble(min.getKey());
            //whenever outputBuffer fills up, write to output file
            if (!outputBuffer.hasRemaining()) {
                writeOutput();
            }
        }   //write the leftovers
        if (outputBuffer.position() != 0) {
            writeOutput();
        }
        //write final outputBuffer to output file
        //writeOutput();
        //Externalsort.swapFileNames(filename, "runfile.bin");
    }
    //Divide functions between MultiWay merge and pre Multiway merge functions
    //========================================================================
    /**
     * fills empty input buffer until full or at end of file
     * @param f
     *  file used to fill buffer
     * @param rindex
     *  index of run
     */
    public void fillInputBufWRun(RandomAccessFile f, int rindex, int iter) {
        try {   //try/catch Exception
            inputBuffer.limit(BLOCK_SIZE); // reset limit
            //System.out.println("Input Fill: " + mergeRun.getValue(rindex) + " " + rindex + " " + iter);
            if (mergeRun.getValue(rindex) == -1) {
                return;
            }
            //go to position for run in file
            f.seek(mergeRun.getValue(rindex) + iter * BLOCK_SIZE);
            inputBuffer.position(0);    //set input buffer pos to start
            //while buffer not full, file not at end, and run within bounds
            while (inputBuffer.hasRemaining() &&    //check if at end or
                f.getFilePointer() != f.length() && f.getFilePointer() < 
                (mergeRun.getValue(rindex) + runLengths.getValue(rindex))) 
            {
                long l = f.readLong();
                double d = f.readDouble();
                inputBuffer.putLong(l);  //read long
                inputBuffer.putDouble(d); //read double
            } //set limit to position as not to read previous data from buffer
            if (f.getFilePointer() == f.length() || f.getFilePointer() >= 
                (mergeRun.getValue(rindex) + runLengths.getValue(rindex))) {
                inputBuffer.limit(inputBuffer.position());
                if (f.getFilePointer() >= 
                    (mergeRun.getValue(rindex) + runLengths.getValue(rindex))) {
                    mergeRun.setIndex(rindex, (long) -1);
                }
            }
            inputBuffer.position(0);
        }
        catch (IOException e) { 
            System.out.println("file not found - fillInputBuffWRun");
        }
    }
    
    /**
     * calculates run lengths
     *  @param f
     *  @param listOfRuns
     * 
     */
    public List<Long> calculateRunLengths(RandomAccessFile f, 
        List<Long> listOfRuns) {
        runLengths = new List<Long>(listOfRuns.length());
        try {
            long fileLength = f.length();
           
            // calculate distance between each starting point of runs
            for(int i = 0; i < listOfRuns.length()-1; i++) {
                //get length of run
                long length = listOfRuns.getValue(i+1) - 
                    listOfRuns.getValue(i);
                runLengths.append(length);
            }
           
            // final case is the distance between the start of the
            // last run and the file length
            long finalRunLength = fileLength - 
                listOfRuns.getValue(listOfRuns.length() - 1);
            runLengths.append(finalRunLength);
        }
        catch (IOException e) { 
            System.out.println("Error - Calc Run Lengths");
        }
        return runLengths;
    }
    
    /**
     * Multiway merge
     * merges all runs in sections of 8 or less
     */
    public void multiwayMerge() {
        long endOfMerge = calcEndPoint();
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            //fill input buffer and heap initially
            ndx = 0;    //index of buffer to be read
            itr = 0;
            currRun = 0;
            for (int i = 0; i < MAX_MERGE; i++) {
                calcVals();
                fillInputBufWRun(raf, ndx, itr);
                fillEmptyHeap();
            }
            calcVals();
            //fill input buffer one last time
            fillInputBufWRun(raf, ndx, itr);
            inputBuffer.position(0);
            //sort with run
            sortWithRun(raf, endOfMerge);
            //nothing left in input buffer
            heapFinal();
            //remove runs already merged
            for (int i = 0; i < mergeRun.length() && 
                i < MAX_MERGE; i++) {
                mergeRun.moveToStart();
                runLengths.moveToStart();
                mergeRun.remove();
                runLengths.remove();
            }
            //calc runlenghts
            calculateRunLengths(raf, runs);
        }
        catch (IOException e) { 
            System.out.println("fail - multiwayMerge");
        }
    }
    
    /**
     * Calculate values for next input buffer fill
     * @param i what number of input buffer fill we're on
     * @param ndx   index of start of run
     * @param itr   iteration/block offset
     */
    public void calcVals() {
        int smaller;
        if (mergeRun.length() < MAX_MERGE) {
            smaller = mergeRun.length();
        }
        else {
            smaller = MAX_MERGE;
        }
        ndx = currRun % smaller; // get index
        itr = currRun / smaller; // get interation
        currRun++;
    }
    
    /**
     * copy run List to mergeRun then reset run
     */
    public void copyRuns() { 
        mergeRun = new List<Long>(runs);
        runs.clear();
        runs.append((long) 0);  //add 0
    }
    
    /**
     * Calculate where the merge ends in the output file
     * @return long location of merge end
     */
    public long calcEndPoint() {
        long mergeSize = 0; //initial 0
        long endPoint = (long) 0;   //holds location of end of merge
        for (int i = 0; i < runLengths.length() && i < MAX_MERGE; i++) {
            mergeSize += runLengths.getValue(i);    //add the size of each run
        }   //get current length of runFile
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            long startSize = raf.length();  //get starting size of file
            endPoint = mergeSize + startSize;
        }
        catch (IOException e) { 
            System.out.println("Error - calcEndPoint");
        }
        return endPoint;
    }
    
    /**
     * runs sorting from 
     * @param f randomAccessFile to read inputs from
     */
    public void sortWithRun(RandomAccessFile f, long last) {
        int smaller;    //calculate if whats left of runs is smaller or 8 is
        if (mergeRun.length() < MAX_MERGE) {
            smaller = mergeRun.length();    // <8 runs left
        }
        else {
            smaller = MAX_MERGE;    //>=8 left
        }
        try {
            // lowest value for double so no matter what first input doesn't
            // hide
            Record lastRemoved = new Record((long)0, LOWEST);

            //System.out.println(inputBuffer.position());

            //System.out.println(heap.heapSize());

            // exit conditions for loop - heapSize 0
            while (inputBuffer.hasRemaining() && heap.heapSize() != 0 && 
                f.getFilePointer() < last) {
                // set last removed and run takeMinimum then heapOut:
                // in->heap->out
                lastRemoved = heapOut(takeMinimum(lastRemoved));
                if (!inputBuffer.hasRemaining()) { //try to refill input buffer
                    for (int j = 0; j < smaller && //itr through runs in merge
                        !inputBuffer.hasRemaining(); j++) {
                        calcVals();
                        fillInputBufWRun(f, ndx, itr); // refill inputBuffer 
                    }                            // until run is empty
                }
                if (heap.heapSize() == 0) { // handle when heap is 'empty'
                    try (RandomAccessFile raf = new RandomAccessFile(
                        "runfile.bin", "rw")) {
                        handleHiddens(raf); // only hidden data in heap
                    }
                    catch (IOException e) {
                    }
                    lastRemoved = new Record((long)0, LOWEST); // reset lowest
                }
            }
        }
        catch (IOException e) {
            System.out.println("Error - sort with run");
        }
    }
    
}
