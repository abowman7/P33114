/**
 * {Project Description Here}
 */

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * The class containing the main method.
 *
 * @author {Rami Benhamida}
 * @version {1.0}
 */

// On my honor:
//
// - I have not used source code obtained from another student,
// or any other unauthorized source, either modified or
// unmodified.
//
// - All source code and documentation used in my program is
// either my original work, or was derived by me from the
// source code published in the textbook for this course.
//
// - I have not discussed coding details about this project with
// anyone other than my partner (in the case of a joint
// submission), instructor, ACM/UPE tutors or the TAs assigned
// to this course. I understand that I may discuss the concepts
// of this program with other students, and that another student
// may help me debug my program so long as neither of us writes
// anything during the discussion or modifies any computer file
// during the discussion. I have violated neither the spirit nor
// letter of this restriction.

public class Externalsort {
    
    private final static int EIGHT_BLOCKS = 65536;
    /**
     * @param args
     *     Command line parameters
     */
    public static void main(String[] args) {
        String inputFile = args[0];
        String runfile = "runfile.bin";
        Parser reader = new Parser(inputFile);
        try (RandomAccessFile f = new RandomAccessFile(inputFile, "r")) {
            if (f.length() <= EIGHT_BLOCKS) {
                reader.eightBlocker();
            }
            else {
                reader.mainFunction();
                swapFileNames(inputFile, runfile);
                // System.out.println(reader.getMergeRun().length());
                while (reader.getMergeRun().length() > 1) {
                    reader.multiwayMerge();
                    if (reader.getMergeRun().length() == 0 && reader.getRuns()
                        .length() > 1) {
                        reader.copyRuns();
                        swapFileNames(inputFile, runfile);
                    }
                }
                swapFileNames(inputFile, runfile); 
                
            }
            
        }
        catch (IOException e) {
            System.out.println("Input File not found");
        }
        reader.printInput();
        
    }
    
    /**
     * Rename fileB to fileA's name, delete file A
     * @param fileA Original File name
     * @param fileB runfile.bin
     */
    public static void swapFileNames(String fileA, String fileB) {
        File file1 = new File(fileA);
        File file2 = new File(fileB);
        File tempFile = new File(fileA + ".tmp");
        // Step 1: rename fileA to a temporary file
        if (!file1.renameTo(tempFile)) {
            return;
        }
        // Step 2: rename fileB to fileA
        if (!file2.renameTo(file1)) {
            // attempt to roll back the change
            tempFile.renameTo(file1);
            return;
        }
        // Step 3: rename the temporary file to fileB
        if (!tempFile.renameTo(file2)) {
            // attempt to roll back the change
            file1.renameTo(file2);
        }
        else {
            File file3 = new File(fileB);
            file3.delete();
        }
    }
    /*public static void swapFileNames2(String fileA, String fileB) {
        File file1 = new File(fileA);
        File file2 = new File(fileB);
        File tempFile = new File(fileA + ".tmp");
        // rename fileA to a temporary file
        if (!file1.renameTo(tempFile)) {
            return;
        }
        // rename fileB to fileA
        if (!file2.renameTo(file1)) {
            // attempt to roll back the change if theres error
            tempFile.renameTo(file1);
            return;
        }
        // rename the temporary file to fileB
        if (!tempFile.renameTo(file2)) {
            // rollback fail case
            file1.renameTo(file2);
        }
        else {
            //System.out.println("File names swapped successfully.");
        }
    }*/

}
