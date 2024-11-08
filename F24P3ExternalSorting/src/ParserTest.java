import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import student.TestCase;

/**
 * @author {Your Name Here}
 * @version {Put Something Here}
 */
public class ParserTest extends TestCase {
    /**
     * Sets up the tests that follow. In general, used for initialization
     */
    public void setUp() {
        // Nothing here
    }


    /**
     * Read contents of a file into a string
     * 
     * @param path
     *            File name
     * @return the string
     * @throws IOException
     */
    static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }
    
    /**
     * placeholder
     * 
     */
    public void testThis() {
        int i = 1;
        assertTrue(1 == i);

    }

    /**
     * Example 2: This method runs on a command sample IO file
     * You will write similar test cases
     * using different text files
     * 
     * @throws IOException
     */
    
    public void testPostedSyntaxSample() throws IOException {
        // Setting up all the parameters
        String[] args = new String[1];
        args[0] = "F24P3ExternalSorting/lib/sampleInput16.bin";

        // Invoke main method of our Graph Project
        Externalsort.main(args);

        // Actual output from your System console
        String actualOutput = systemOut().getHistory();
        
        
    }
}