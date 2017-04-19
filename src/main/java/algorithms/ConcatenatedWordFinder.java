package algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConcatenatedWordFinder {
	public static class Node {//assumes only lowercase alphabets will be in the dictionary
		
		public static int SET_SIZE = 26;
		Node[] childNodes = new Node[SET_SIZE];
		int size = 0;
		boolean endOfWord; 
		
		public void add(String s) {
			add(s, 0);
		}
		
		public void add(String s, int index) {
			++size;
			if (index == s.length() -1) {
				endOfWord = true;
				System.out.println("set end of word for " + s);
			}
			if (index == s.length()) {					
				
				return;//word added
			}
			int indexInChildNodes = s.charAt(index) - 'a';
			Node childNode = childNodes[indexInChildNodes];//move current node to child node
			if (childNode == null) {
				childNode = new Node();
				childNodes[indexInChildNodes] = childNode;
			}
			childNode.add(s, index + 1);//continue adding the letters from this current node
		}
		
		public int getLinkedWordCount(String s, int index) {
			if (index == s.length()) {
				return size;
			}
			int indexInChildNodes = s.charAt(index) - 'a';
			Node childNode = childNodes[indexInChildNodes];//move current node to child node
			if (childNode == null) {
				return 0;
			}
			return childNode.getLinkedWordCount(s, index + 1);
		}
		

		public int containedWordsCount(String s, int index, int wordCount) {
			if (index == s.length()) {				
				return wordCount;
			}
			if (endOfWord) {
				++wordCount;
			}
			int indexInChildNodes = s.charAt(index) - 'a';
			Node childNode = childNodes[indexInChildNodes];//move current node to child node
			if (childNode == null) {
				return 0;
			}
			
			return childNode.containedWordsCount(s, index + 1, wordCount);
		}	
		
	}//Node
	
	public static void main(String args[]) {
		//Node root = new Node();
		//String[] sampleSet = {"cat", "mat", "rat", "matcat", "dog", "ratdog", "cats"};
		//add(root, sampleSet);
		//System.out.println("added words");
		//getLinkedWordCount(root, sampleSet);
		//printConcatenatedWords(root, sampleSet);		
		
		List<String> dictionary = Arrays.asList("cat", "mat", "rat", "matcat", "dog", "ratdog", "cats");
		printConcatenatedWordsUsingList(dictionary);
		
	}

	private static void getLinkedWordCount(Node root, String[] words) {
		for (String s : words) {
			int wordCount = root.getLinkedWordCount(s, 0);
			
			System.out.println("word : " + s + " linked Words = " + wordCount);			
		}
	}

	private static void printConcatenatedWords(Node root, String[] words) {
		for (String s : words) {
			int wordCount = root.containedWordsCount(s, 0, 0);
			System.out.println("Word count = " + wordCount);
			if (wordCount > 1) {
				System.out.println(s + " ");				
			}
		}
	}

	private static void add(Node root, String[] sampleSet) {
		for( String s : sampleSet) {
			root.add(s);
		}		
	}
	
	private static void printConcatenatedWordsUsingList(List<String> dictionary ) {
		for (String s : dictionary) {
			int count = findIfDictContains(s, dictionary);
			if (count > 1) {
				System.out.println(s);
			}
			
		}
	}
	
	private static int findIfDictContains(String s, List<String> dictionary) {
		int count = 0;
		
		for (int i = 1; i <= s.length(); ++i ) {
			if (dictionary.contains(s.substring(0, i))) {
				if (i == s.length()) {
					return count + 1;
				}
				count += findIfDictContains(s.substring(i, s.length()), dictionary);
			}
		}
		
		return count;
	}
	
	
}
