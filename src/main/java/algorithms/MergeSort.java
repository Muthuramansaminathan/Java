import java.io.*;

class MergeSort {
    public static void mergesort(int[] array, int[] temp, int left, int right) {
        if ( left >= right) {
            return ;
        }
        int middle = ( left + right ) / 2;
        mergesort(array, temp, left, middle);
         for (int a1 : array) {
		    System.out.print(a1 + " ");
		}
		System.out.println(" ");
        mergesort(array, temp, middle + 1 , right);
         for (int a1 : array) {
		    System.out.print(a1 + " ");
		}
		System.out.println(" ");
        mergeHalves (array, temp, left, right);
         for (int a1 : array) {
		    System.out.print(a1 + " ");
		}
		System.out.println(" ");
       
    }
    
    public static void mergeHalves(int[] array, int[]  temp, int leftstart, int rightend) {
        int left = leftstart;
        int middle = (leftstart + rightend) / 2;
        int leftend = middle;
        int rightstart = middle + 1;
        //int rightend = right;
        int index = leftstart;
        int size = rightend - leftstart + 1;
        int right = rightstart;
        
        while(left <= leftend && right <= rightend) {
            if (array[left] < array [right]) {
                temp[index] = array[left];
                ++left;
            } else {
                temp[index] = array[right];
                ++right;
                
            }
            ++index;
        }
        
        System.arraycopy(array, left, temp, index, leftend - left + 1);
        System.arraycopy(array, right, temp, index, rightend - right + 1);
        System.arraycopy(temp, leftstart, array, leftstart, size);
        
        
    }
    
	public static void main (String[] args) {
	    int array[] = {23, 2,34, 32, 45,1, 5, 5};
	    
	    mergesort(array, new int[array.length], 0, array.length -1);
		
		for (int a : array) {
		    System.out.print(a + " ");
		}
	}
}
