package com.vision.wb;

import java.util.Arrays;
import java.util.Scanner;

public class MaxNumber {
	public static void main(String[] args) {
		System.out.println("Enter a string:");
		Scanner sc= new Scanner(System.in);
		String s=sc.next();
		System.out.println(s);
		s=s.replaceAll("[a-z]", "");
		String[] s1=s.split("");
		Arrays.sort(s1);
		System.out.println("Maximum number is:"+s1[s1.length-1]);	
	}
}
