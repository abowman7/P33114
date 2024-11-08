import java.util.NoSuchElementException;
/**
 * Array based List Class used in heap
 * 
 * Adapted from OpenDSA
 *
 * @author <Rami Benhamida, Adam Bowman>
 * @version <1.0>
 * 
 * @param <E> type of List
 */
class List<E> {
    private E[] listArray; // Array holding list elements
    private static final int DEFAULT_SIZE = 64; // Default size
    private int maxSize; // Maximum size of list
    private int listSize; // Current # of list items
    private int curr; // Position of current element

    // Constructors
    /**
     * Constructor
     * @param size  max size of array
     */
    @SuppressWarnings("unchecked") // Generic array allocation
    List(int size) {
        maxSize = size;
        listSize = 0;
        curr = 0;
        listArray = (E[])new Object[size]; // Create listArray
    }
    
    /**
     * Copy Constructor
     * @param list  list to be copied
     */
    @SuppressWarnings("unchecked") // Generic array allocation
    List(List<E> list) {
        maxSize = list.maxSize;
        listSize = list.listSize;
        curr = 0;
        listArray = (E[])new Object[list.maxSize]; // Create listArray
        for (int i = 0; i < list.listSize; i++) {
            listArray[i] = list.listArray[i];
        }
    }


    /**
     * constructor
     * Default
     */
    List() {
        this(DEFAULT_SIZE); // Just call the other constructor
    }

    /**
     * clear the list
     */
    public void clear() { // Reinitialize the list
        listSize = 0;
        curr = 0; // Simply reinitialize values
    }


    /**
     * inserts value at current location
     * @param it    val to be inserted
     * @return  t/f if inserted
     */
    public boolean insert(E it) {
        if (listSize >= maxSize) {
            return false;
        }
        for (int i = listSize; i > curr; i--) { // Shift elements up
            listArray[i] = listArray[i - 1]; // to make room
        }
        listArray[curr] = it;
        listSize++; // Increment list size
        return true;
    }


    /**
     * append value to end of list
     * @param it    value to be appended
     * @return  t/f or not
     */
    public boolean append(E it) {
        if (listSize >= maxSize) {
            return false;
        }
        listArray[listSize++] = it;
        return true;
    }


    /**
     * removes current index
     * @return value removed
     * @throws NoSuchElementException   out of range
     */
    public E remove() throws NoSuchElementException {
        if ((curr < 0) || (curr >= listSize)) { // No current element
            throw new NoSuchElementException("remove() in AList has current of "
                + curr + " and size of " + listSize
                + " that is not a a valid element");
        }
        E it = listArray[curr]; // Copy the element
        for (int i = curr; i < listSize - 1; i++) { // Shift them down
            listArray[i] = listArray[i + 1];
        }
        listSize--; // Decrement size
        return it;
    }

    /**
     * moves to first index
     */
    public void moveToStart() { // Set to front
        curr = 0;
    }

    /**
     * moves to end
     */
    public void moveToEnd() { // Set at end
        curr = listSize;
    }

    /**
     * goes to previous index
     */
    public void prev() { // Move left
        if (curr != 0) {
            curr--;
        }
    }

    /**
     * goes to next index
     */
    public void next() { // Move right
        if (curr < listSize) {
            curr++;
        }
    }

    /**
     * get length of list
     * @return listSize
     */
    public int length() { // Return list size
        return listSize;
    }

    /**
     * index of current position
     * @return  int
     */
    public int currPos() { // Return current position
        return curr;
    }


    /**
     * moveToPos
     * @param pos   position to move to
     * @return  true if position is valid
     */
    public boolean moveToPos(int pos) {
        if ((pos < 0) || (pos > listSize)) {
            return false;
        }
        curr = pos;
        return true;
    }


    /**
     * isAtEnd
     * @return true if at end
     */
    public boolean isAtEnd() {
        return curr == listSize;
    }


    /**
     * Value at current index
     * @return E value at current index
     * @throws NoSuchElementException
     */
    public E getValue() throws NoSuchElementException {
        if ((curr < 0) || (curr >= listSize)) { // No current element
            throw new NoSuchElementException(
                "getvalue() in AList has current of " + curr + " and size of "
                    + listSize + " that is not a a valid element");
        }
        return listArray[curr];
    }
    
    /**
     * getValue at index
     * @param ndx index of value
     * @return  E value at index
     */
    public E getValue(int ndx) {
        return listArray[ndx];
    }
    
    /**
     * set index in list
     * @param ndx   index
     * @param val   value
     */
    public void setIndex(int ndx, E val) {
        listArray[ndx] = val;
    }

    
    /**
     * isEmpty
     * @return true if empty
     */
    public boolean isEmpty() {
        return listSize == 0;
    }
    
}
